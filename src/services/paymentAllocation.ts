/**
 * Payment allocation logic: allocates actual payments across contractual payment items
 * in order: Indexation → Interest → Principal.
 */

import { mondayQuery } from './mondayApi';
import { logger } from '../logger';
import {
  ACTUAL_PAYMENTS,
  CONTRACTUAL_PAYMENTS,
} from '../config/config';

// ─── Types ───────────────────────────────────────────────────────────────────

export interface ActualPaymentItem {
  id: string;
  receiptAmount: number;
  receiptDate: string | null;
  linkedContractIds: number[];
}

export interface ContractualPaymentItem {
  id: string;
  name: string;
  paymentOrder: number; // 1, 2, 3, 4... from item name
  paymentDue: number;
  indexationPaymentDue: number;
  interestPaymentDue: number;
  principal: number;
  interest: number;
  indexation: number;
}

export interface RemainingBalances {
  principal: number;
  interest: number;
  indexation: number;
}

export interface AllocationResult {
  principalPaid: number;
  interestPaid: number;
  indexationPaid: number;
  remainingPrincipal: number;
  remainingInterest: number;
  remainingIndexation: number;
  amountUsed: number;
}

export interface SubitemPayload {
  name: string;
  columnValues: Record<string, unknown>;
}

// ─── Rounding helper ─────────────────────────────────────────────────────────

const ROUND = 2;

function round(value: number): number {
  return Math.round(value * 10 ** ROUND) / 10 ** ROUND;
}

/** Parse linked item IDs from board relation column value (handles various API formats) */
function parseBoardRelationIds(value: string | null | undefined): number[] {
  if (!value?.trim()) return [];
  try {
    const parsed = JSON.parse(value);
    const ids = parsed.linkedPulseIds ?? parsed.item_ids ?? parsed.linked_item_ids ?? [];
    return (Array.isArray(ids) ? ids : [])
      .map((x: { linkedPulseId?: number | string } | number | string) => {
        if (typeof x === 'number') return x;
        if (typeof x === 'string') return parseInt(x, 10);
        return typeof x.linkedPulseId === 'number' ? x.linkedPulseId : parseInt(String(x.linkedPulseId ?? ''), 10);
      })
      .filter((id): id is number => !isNaN(id));
  } catch {
    return [];
  }
}

// ─── Fetch actual payment item ──────────────────────────────────────────────

export async function fetchActualPaymentItem(
  itemId: string
): Promise<ActualPaymentItem | null> {
  logger.info('Fetching actual payment item', { itemId });

  const query = `
    query GetActualPayment($itemId: ID!) {
      items(ids: [$itemId]) {
        id
        column_values(ids: ["${ACTUAL_PAYMENTS.columns.receiptAmount}", "${ACTUAL_PAYMENTS.columns.receiptDate}", "${ACTUAL_PAYMENTS.columns.contracts}", "${ACTUAL_PAYMENTS.columns.contractId}"]) {
          id
          value
          type
          ... on BoardRelationValue {
            linked_item_ids
          }
        }
      }
    }
  `;

  type ColumnValue = { id: string; value?: string | null; type: string; linked_item_ids?: string[] };
  const data = await mondayQuery<{ items: Array<{ id: string; column_values: ColumnValue[] }> }>(query, { itemId: parseInt(itemId, 10) });

  const item = data.items?.[0];
  if (!item) {
    logger.warn('Actual payment item not found', { itemId });
    return null;
  }

  let receiptAmount = 0;
  let receiptDate: string | null = null;
  let linkedContractIds: number[] = [];
  let contractIdText: string | null = null;

  for (const cv of item.column_values) {
    if (cv.id === ACTUAL_PAYMENTS.columns.receiptAmount) {
      try {
        const parsed = JSON.parse(cv.value || '{}');
        receiptAmount = parseFloat(parsed.value ?? parsed) || 0;
      } catch {
        receiptAmount = parseFloat(cv.value ?? '') || 0;
      }
    } else if (cv.id === ACTUAL_PAYMENTS.columns.receiptDate) {
      try {
        const parsed = JSON.parse(cv.value || '{}');
        receiptDate = parsed.date ?? null;
      } catch {
        receiptDate = cv.value || null;
      }
    } else if (cv.id === ACTUAL_PAYMENTS.columns.contracts) {
      // API 2025-04+ returns value: null for board_relation; use linked_item_ids instead
      const ids = (cv as ColumnValue).linked_item_ids;
      if (ids?.length) {
        linkedContractIds = ids.map((id) => parseInt(id, 10)).filter((id) => !isNaN(id));
      } else {
        linkedContractIds = parseBoardRelationIds(cv.value ?? null);
      }
    } else if (cv.id === ACTUAL_PAYMENTS.columns.contractId) {
      try {
        const parsed = JSON.parse(cv.value || '{}');
        contractIdText = parsed.text ?? parsed.value ?? (typeof cv.value === 'string' ? cv.value : null);
      } catch {
        contractIdText = typeof cv.value === 'string' ? cv.value : null;
      }
      if (contractIdText && typeof contractIdText !== 'string') contractIdText = String(contractIdText);
    }
  }

  // Fallback: if board relation is empty but contractId text column has a value, use it
  if (linkedContractIds.length === 0 && contractIdText?.trim()) {
    const parsed = parseInt(contractIdText.trim(), 10);
    if (!isNaN(parsed)) {
      linkedContractIds = [parsed];
      logger.info('Using contractId text column as fallback', { contractIdText, linkedContractIds });
    }
  }

  if (!receiptAmount || receiptAmount <= 0) {
    logger.warn('Actual payment item has no valid receipt amount', { itemId, receiptAmount });
  }

  logger.info('Fetched actual payment', { itemId, receiptAmount, receiptDate, linkedContractIds });

  return {
    id: item.id,
    receiptAmount: round(receiptAmount),
    receiptDate,
    linkedContractIds,
  };
}

// ─── Extract contract ID ────────────────────────────────────────────────────

export function extractContractId(actualPayment: ActualPaymentItem): number | null {
  const id = actualPayment.linkedContractIds?.[0] ?? null;
  if (!id) {
    logger.warn('No linked contract on actual payment item', { itemId: actualPayment.id });
  }
  return id;
}

// ─── Find matching contractual payment items ─────────────────────────────────
// Note: Board relation columns are not supported in items_page_by_column_values,
// so we fetch items and filter by contract link in code.

export async function findMatchingContractualItems(
  contractId: number
): Promise<ContractualPaymentItem[]> {
  logger.info('Finding contractual payment items for contract', { contractId });

  type ContractualColumnValue = { id: string; value?: string | null; linked_item_ids?: string[] };
  const allItems: Array<{ id: string; name: string; column_values: ContractualColumnValue[] }> = [];
  let cursor: string | null = null;

  do {
    const query: string = cursor
      ? `
        query GetContractualItemsNext($cursor: String!) {
          next_items_page(cursor: $cursor, limit: 500) {
            cursor
            items {
                id
                name
                column_values(ids: ["${CONTRACTUAL_PAYMENTS.items.contractLink}", "${CONTRACTUAL_PAYMENTS.items.paymentDue}", "${CONTRACTUAL_PAYMENTS.items.indexationPaymentDue}", "${CONTRACTUAL_PAYMENTS.items.interestPaymentDue}"]) {
                id
                value
                ... on BoardRelationValue {
                  linked_item_ids
                }
              }
            }
          }
        }
      `
      : `
        query GetContractualItems($boardId: ID!) {
          boards(ids: [$boardId]) {
            items_page(limit: 500) {
              cursor
            items {
                id
                name
                column_values(ids: ["${CONTRACTUAL_PAYMENTS.items.contractLink}", "${CONTRACTUAL_PAYMENTS.items.paymentDue}", "${CONTRACTUAL_PAYMENTS.items.indexationPaymentDue}", "${CONTRACTUAL_PAYMENTS.items.interestPaymentDue}"]) {
                id
                value
                ... on BoardRelationValue {
                  linked_item_ids
                }
              }
            }
            }
          }
        }
      `;

    type PageResult = { cursor: string | null; items: typeof allItems };
    let page: PageResult | undefined;
    if (cursor) {
      const data: { next_items_page: PageResult } = await mondayQuery(query, { cursor });
      page = data.next_items_page;
    } else {
      const data: { boards: Array<{ items_page: PageResult }> } = await mondayQuery(query, { boardId: CONTRACTUAL_PAYMENTS.boardId });
      page = data.boards?.[0]?.items_page;
    }
    cursor = page?.cursor ?? null;
    allItems.push(...(page?.items ?? []));
  } while (cursor);

  const items = allItems.filter((item) => {
    const cv = item.column_values.find((c) => c.id === CONTRACTUAL_PAYMENTS.items.contractLink);
    if (!cv) return false;
    // API 2025-04+ returns value: null for board_relation; use linked_item_ids instead
    const ids = cv.linked_item_ids;
    if (ids?.length) {
      const linked = ids.map((id) => parseInt(id, 10)).filter((id) => !isNaN(id));
      return linked.includes(contractId);
    }
    if (!cv.value) return false;
    try {
      const parsed = JSON.parse(cv.value);
      const parsedIds = parsed.linkedPulseIds ?? parsed.item_ids ?? [];
      const linked = Array.isArray(parsedIds)
        ? parsedIds.map((x: { linkedPulseId?: number } | number) =>
            typeof x === 'number' ? x : x.linkedPulseId
          ).filter((id): id is number => typeof id === 'number')
        : [];
      return linked.includes(contractId);
    } catch {
      return false;
    }
  });

  if (items.length === 0) {
    logger.warn('No contractual payment items found for contract', { contractId });
    return [];
  }

  /** Parse payment order from item name: "1" -> 1, "2 5" -> 2, "3 1" -> 3, "4" -> 4 */
  function parsePaymentOrder(name: string): number {
    const match = name?.trim().match(/^(\d+)/);
    return match ? parseInt(match[1], 10) : 999;
  }

  const contractual: ContractualPaymentItem[] = items.map((item) => {
    let paymentDue = 0;
    let indexationPaymentDue = 0;
    let interestPaymentDue = 0;

    for (const cv of item.column_values) {
      try {
        const parsed = JSON.parse(cv.value || '{}');
        const val = parseFloat(parsed.value ?? parsed) || 0;
        if (cv.id === CONTRACTUAL_PAYMENTS.items.paymentDue) paymentDue = val;
        else if (cv.id === CONTRACTUAL_PAYMENTS.items.indexationPaymentDue) indexationPaymentDue = val;
        else if (cv.id === CONTRACTUAL_PAYMENTS.items.interestPaymentDue) interestPaymentDue = val;
      } catch {
        const val = parseFloat(cv.value ?? '') || 0;
        if (cv.id === CONTRACTUAL_PAYMENTS.items.paymentDue) paymentDue = val;
        else if (cv.id === CONTRACTUAL_PAYMENTS.items.indexationPaymentDue) indexationPaymentDue = val;
        else if (cv.id === CONTRACTUAL_PAYMENTS.items.interestPaymentDue) interestPaymentDue = val;
      }
    }

    const principal = round(Math.max(0, paymentDue - indexationPaymentDue - interestPaymentDue));
    const interest = round(interestPaymentDue);
    const indexation = round(indexationPaymentDue);
    const paymentOrder = parsePaymentOrder(item.name ?? '');

    return {
      id: item.id,
      name: item.name ?? '',
      paymentOrder,
      paymentDue,
      indexationPaymentDue,
      interestPaymentDue,
      principal,
      interest,
      indexation,
    };
  });

  // Sort by payment order (1, 2, 3, 4...) so we fill the first payment before moving to the next
  contractual.sort((a, b) => a.paymentOrder - b.paymentOrder);

  logger.info('Found contractual items (by payment order)', {
    contractId,
    count: contractual.length,
    order: contractual.map((c) => `${c.paymentOrder}:${c.name}`),
  });

  return contractual;
}

// ─── Get current remaining balances for a contractual item ───────────────────
// Uses latest subitem (by date) as source of truth. If no subitems, uses parent's original values.

export async function getRemainingBalances(
  parentItemId: string,
  parentOriginal: { principal: number; interest: number; indexation: number }
): Promise<RemainingBalances> {
  logger.info('Getting remaining balances for contractual item', { parentItemId });

  const query = `
    query GetSubitems($parentId: ID!) {
      items(ids: [$parentId]) {
        subitems {
          id
          column_values(ids: ["${CONTRACTUAL_PAYMENTS.subitems.remainingPrincipal}", "${CONTRACTUAL_PAYMENTS.subitems.remainingInterest}", "${CONTRACTUAL_PAYMENTS.subitems.remainingIndexLinkage}"]) {
            id
            value
          }
          created_at
        }
      }
    }
  `;

  const data = await mondayQuery<{
    items: Array<{
      subitems: Array<{
        id: string;
        column_values: Array<{ id: string; value: string }>;
        created_at: string;
      }>;
    }>;
  }>(query, { parentId: parentItemId });

  const subitems = data.items?.[0]?.subitems ?? [];
  if (subitems.length === 0) {
    logger.info('No subitems yet, using parent original values as remaining', { parentItemId });
    return { ...parentOriginal };
  }

  // Use latest subitem (by created_at) as source of truth
  const latest = subitems.sort(
    (a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime()
  )[0];

  let remainingPrincipal = 0;
  let remainingInterest = 0;
  let remainingIndexation = 0;

  for (const cv of latest.column_values) {
    try {
      const parsed = JSON.parse(cv.value || '{}');
      const val = round(parseFloat(parsed.value ?? parsed) || 0);
      if (cv.id === CONTRACTUAL_PAYMENTS.subitems.remainingPrincipal) remainingPrincipal = val;
      else if (cv.id === CONTRACTUAL_PAYMENTS.subitems.remainingInterest) remainingInterest = val;
      else if (cv.id === CONTRACTUAL_PAYMENTS.subitems.remainingIndexLinkage) remainingIndexation = val;
    } catch {
      const val = round(parseFloat(cv.value) || 0);
      if (cv.id === CONTRACTUAL_PAYMENTS.subitems.remainingPrincipal) remainingPrincipal = val;
      else if (cv.id === CONTRACTUAL_PAYMENTS.subitems.remainingInterest) remainingInterest = val;
      else if (cv.id === CONTRACTUAL_PAYMENTS.subitems.remainingIndexLinkage) remainingIndexation = val;
    }
  }

  logger.info('Remaining balances from latest subitem', { parentItemId, remainingPrincipal, remainingInterest, remainingIndexation });

  return { principal: remainingPrincipal, interest: remainingInterest, indexation: remainingIndexation };
}

// ─── Allocate payment amount by priority (indexation → interest → principal) ─

export function allocatePayment(
  amount: number,
  initialBalances: RemainingBalances
): AllocationResult {
  let remaining = round(amount);
  const { principal, interest, indexation } = initialBalances;

  let indexationPaid = round(Math.min(remaining, indexation));
  remaining = round(remaining - indexationPaid);

  let interestPaid = round(Math.min(remaining, interest));
  remaining = round(remaining - interestPaid);

  let principalPaid = round(Math.min(remaining, principal));
  remaining = round(remaining - principalPaid);

  const amountUsed = round(indexationPaid + interestPaid + principalPaid);

  return {
    principalPaid,
    interestPaid,
    indexationPaid,
    remainingPrincipal: round(principal - principalPaid),
    remainingInterest: round(interest - interestPaid),
    remainingIndexation: round(indexation - indexationPaid),
    amountUsed,
  };
}

// ─── Create subitem payload ──────────────────────────────────────────────────

export function createSubitemPayload(
  paymentDate: string,
  allocation: AllocationResult,
  actualAmountAllocated: number
): SubitemPayload {
  const sub = CONTRACTUAL_PAYMENTS.subitems;
  // Monday API expects numeric columns as plain strings: "column_id": "123"
  // Name is set via item_name; only set numeric columns in column_values
  const columnValues: Record<string, string> = {
    [sub.actualReceipt]: String(actualAmountAllocated),
    [sub.interest]: String(allocation.interestPaid),
    [sub.indexLinkage]: String(allocation.indexationPaid),
    [sub.remainingPrincipal]: String(allocation.remainingPrincipal),
    [sub.remainingInterest]: String(allocation.remainingInterest),
    [sub.remainingIndexLinkage]: String(allocation.remainingIndexation),
  };
  return {
    name: paymentDate,
    columnValues,
  };
}

// ─── Create subitem via API ──────────────────────────────────────────────────

export async function createSubitem(
  parentItemId: string,
  payload: SubitemPayload
): Promise<string> {
  logger.info('Creating subitem under parent', { parentItemId, name: payload.name });

  const columnValuesJson = JSON.stringify(payload.columnValues);

  const mutation = `
    mutation CreateSubitem($parentId: ID!, $itemName: String!, $columnValues: JSON!) {
      create_subitem(
        parent_item_id: $parentId,
        item_name: $itemName,
        column_values: $columnValues
      ) {
        id
      }
    }
  `;

  const data = await mondayQuery<{ create_subitem: { id: string } }>(mutation, {
    parentId: parseInt(parentItemId, 10),
    itemName: payload.name,
    columnValues: columnValuesJson,
  });

  const id = data.create_subitem?.id;
  if (!id) {
    throw new Error('Failed to create subitem: no ID returned');
  }

  logger.info('Subitem created', { parentItemId, subitemId: id });
  return id;
}

// ─── Main orchestration: apply payment from webhook ──────────────────────────

export interface ApplyPaymentInput {
  actualPaymentItemId: string;
}

export interface ApplyPaymentResult {
  success: boolean;
  subitemsCreated: number;
  error?: string;
}

export async function applyPayment(input: ApplyPaymentInput): Promise<ApplyPaymentResult> {
  const { actualPaymentItemId } = input;
  logger.info('Applying payment', { actualPaymentItemId });

  const actualPayment = await fetchActualPaymentItem(actualPaymentItemId);
  if (!actualPayment) {
    return { success: false, subitemsCreated: 0, error: 'Actual payment item not found' };
  }

  if (!actualPayment.receiptAmount || actualPayment.receiptAmount <= 0) {
    return { success: false, subitemsCreated: 0, error: 'Invalid or missing receipt amount' };
  }

  const contractId = extractContractId(actualPayment);
  if (contractId === null) {
    return { success: false, subitemsCreated: 0, error: 'No linked contract on actual payment item' };
  }

  const contractualItems = await findMatchingContractualItems(contractId);
  if (contractualItems.length === 0) {
    return { success: false, subitemsCreated: 0, error: 'No matching contractual payment items found' };
  }

  const paymentDate =
    actualPayment.receiptDate ??
    new Date().toISOString().slice(0, 10);

  let remainingToAllocate = round(actualPayment.receiptAmount);
  let subitemsCreated = 0;

  for (const item of contractualItems) {
    if (remainingToAllocate <= 0) break;

    const parentOriginal = {
      principal: item.principal,
      interest: item.interest,
      indexation: item.indexation,
    };

    const balances = await getRemainingBalances(item.id, parentOriginal);
    const totalRemaining = round(balances.principal + balances.interest + balances.indexation);

    if (totalRemaining <= 0) {
      logger.info('Contractual item fully paid, skipping', { itemId: item.id });
      continue;
    }

    const allocation = allocatePayment(remainingToAllocate, balances);
    if (allocation.amountUsed <= 0) break;

    const payload = createSubitemPayload(
      paymentDate,
      allocation,
      allocation.amountUsed
    );

    await createSubitem(item.id, payload);
    subitemsCreated++;
    remainingToAllocate = round(remainingToAllocate - allocation.amountUsed);

    logger.info('Allocated to contractual item', {
      itemId: item.id,
      amountUsed: allocation.amountUsed,
      remainingToAllocate,
    });
  }

  if (remainingToAllocate > 0) {
    logger.warn('Payment amount exceeded all contractual items', {
      actualPaymentItemId,
      unallocated: remainingToAllocate,
    });
  }

  logger.info('Payment application complete', { actualPaymentItemId, subitemsCreated });
  return { success: true, subitemsCreated };
}
