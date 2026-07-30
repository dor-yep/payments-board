/**
 * Builds payment-statement data for PDF generation.
 * Reuses existing allocation/indexation logic from paymentAllocation.ts.
 */

import { mondayQuery } from './mondayApi';
import { logger } from '../logger';
import {
  APARTMENTS_BOARD,
  CONTRACTUAL_PAYMENTS,
  PDF_CONTRACTS_BOARD,
  PDF_CONTRACTUAL_PAYMENTS,
} from '../config/config';
import {
  ContractDetails,
  ContractualPaymentItem,
  PaymentCategory,
  computeBalancesBeforePayment,
  fetchContractDetails,
  fetchIndexForPaymentDate,
  findMatchingContractualItems,
  getPreviousSubitemBalances,
  vatGrossMultiplier,
} from './paymentAllocation';

const ROUND = 2;

function round(value: number): number {
  return Math.round(value * 10 ** ROUND) / 10 ** ROUND;
}

function parseNumeric(value: string | null | undefined): number | null {
  if (value == null || value.trim() === '') return null;
  try {
    const parsed = JSON.parse(value);
    if (typeof parsed === 'number') return Number.isFinite(parsed) ? parsed : null;
    if (typeof parsed === 'string') return parseMoneyString(parsed);
    if (parsed && typeof parsed === 'object' && parsed.value != null) {
      return parseMoneyString(String(parsed.value));
    }
    return null;
  } catch {
    return parseMoneyString(value);
  }
}

/** Parse a display/money string like "₪2,550,000" or "2550000". */
function parseMoneyString(raw: string | null | undefined): number | null {
  if (raw == null) return null;
  const trimmed = String(raw).trim();
  if (!trimmed) return null;
  let s = trimmed.replace(/[₪\s]/g, '');
  // US-style thousands: 2,550,000 or 2,550,000.50
  if (/^-?\d{1,3}(,\d{3})+(\.\d+)?$/.test(s)) {
    s = s.replace(/,/g, '');
  } else if (/^-?\d{1,3}(\.\d{3})+(,\d+)?$/.test(s)) {
    // EU-style thousands: 2.550.000 or 2.550.000,50
    s = s.replace(/\./g, '').replace(',', '.');
  } else {
    s = s.replace(/,/g, '');
  }
  const num = Number(s);
  return Number.isFinite(num) ? num : null;
}

function parseDate(value: string | null | undefined): string | null {
  if (value == null || value.trim() === '') return null;
  try {
    const parsed = JSON.parse(value);
    return parsed.date ? String(parsed.date).slice(0, 10) : null;
  } catch {
    return value.slice(0, 10);
  }
}

function parseText(value: string | null | undefined, text?: string | null): string | null {
  if (text?.trim()) return text.trim();
  if (value == null || value.trim() === '') return null;
  try {
    const parsed = JSON.parse(value);
    const t = parsed.text ?? parsed.value ?? parsed.display_value;
    return t != null ? String(t).trim() : null;
  } catch {
    return value.trim() || null;
  }
}

function parseBoardRelationIds(
  value: string | null | undefined,
  linkedItemIds?: string[] | null
): number[] {
  if (linkedItemIds?.length) {
    return linkedItemIds.map((id) => parseInt(id, 10)).filter((id) => !isNaN(id));
  }
  if (!value?.trim()) return [];
  try {
    const parsed = JSON.parse(value);
    const ids = parsed.linkedPulseIds ?? parsed.item_ids ?? parsed.linked_item_ids ?? [];
    return (Array.isArray(ids) ? ids : [])
      .map((x: { linkedPulseId?: number | string } | number | string) => {
        if (typeof x === 'number') return x;
        if (typeof x === 'string') return parseInt(x, 10);
        return typeof x.linkedPulseId === 'number'
          ? x.linkedPulseId
          : parseInt(String(x.linkedPulseId ?? ''), 10);
      })
      .filter((id): id is number => !isNaN(id));
  } catch {
    return [];
  }
}

/**
 * Prefer display_value for Monday lookup/mirror columns (the cell total you see in the UI).
 * Never trust raw `value` JSON or a comma-joined list of linked line items — stripping
 * commas from that list used to produce garbage like ₪3,400,500,000+.
 */
function parseLookupNumeric(
  value: string | null | undefined,
  text?: string | null,
  displayValue?: string | null
): number | null {
  const fromDisplay = parseMoneyString(displayValue ?? null);
  if (fromDisplay != null) return fromDisplay;

  // `text` is OK only when it is a single money amount (not "a, b, c" linked rows)
  if (text?.trim() && !looksLikeMultipleAmounts(text)) {
    const fromText = parseMoneyString(text);
    if (fromText != null) return fromText;
  }

  // Raw value only if it clearly encodes one number
  return parseNumeric(value);
}

function looksLikeMultipleAmounts(raw: string): boolean {
  // More than one currency symbol, or several thousand-grouped numbers separated by commas
  if ((raw.match(/₪/g) ?? []).length > 1) return true;
  const chunks = raw.split(/,\s+/).map((s) => s.trim()).filter(Boolean);
  if (chunks.length <= 1) return false;
  const numericChunks = chunks.filter((c) => /[\d]/.test(c));
  return numericChunks.length > 1;
}

type ColumnValueWithMirror = {
  id: string;
  value?: string | null;
  text?: string | null;
  display_value?: string | null;
  date?: string | null;
  linked_item_ids?: string[];
};

function parseLookupDate(cv: ColumnValueWithMirror): string | null {
  if (cv.date) return String(cv.date).slice(0, 10);

  const fromValue = parseDate(cv.value);
  if (fromValue) return fromValue;

  const candidates = [cv.text, cv.display_value].filter(Boolean) as string[];
  for (const raw of candidates) {
    const trimmed = raw.trim();
    if (!trimmed) continue;

    const isoMatch = trimmed.match(/^(\d{4})-(\d{2})-(\d{2})/);
    if (isoMatch) return `${isoMatch[1]}-${isoMatch[2]}-${isoMatch[3]}`;

    const heMatch = trimmed.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
    if (heMatch) {
      const [, dd, mm, yyyy] = heMatch;
      return `${yyyy}-${mm.padStart(2, '0')}-${dd.padStart(2, '0')}`;
    }

    const parsed = new Date(trimmed);
    if (!isNaN(parsed.getTime())) return parsed.toISOString().slice(0, 10);

    return trimmed;
  }

  return null;
}

function sortKeyDueDate(iso: string | null): number {
  if (!iso?.trim()) return Number.POSITIVE_INFINITY;
  const t = new Date(iso).getTime();
  return Number.isNaN(t) ? Number.POSITIVE_INFINITY : t;
}

async function findAllContractualPaymentsForStatement(
  contractId: number
): Promise<ContractualPaymentItem[]> {
  const [apartmentItems, registrationItems] = await Promise.all([
    findMatchingContractualItems(contractId, 'דירה'),
    findMatchingContractualItems(contractId, 'רישום זכויות'),
  ]);

  return [...apartmentItems, ...registrationItems].sort((a, b) => {
    const da = sortKeyDueDate(a.contractualDueDate);
    const db = sortKeyDueDate(b.contractualDueDate);
    if (da !== db) return da - db;
    return a.paymentOrder - b.paymentOrder;
  });
}

function parseStatusLabel(cv: { label?: string | null; value?: string | null }): string | null {
  if (cv.label?.trim()) return cv.label.trim();
  try {
    const parsed = JSON.parse(cv.value || '{}');
    return (parsed.label ?? parsed.text ?? '').toString().trim() || null;
  } catch {
    return null;
  }
}

function parseSubitemNameToIsoDate(name: string | null): string | null {
  if (!name?.trim()) return null;
  const d = new Date(name);
  if (isNaN(d.getTime())) return null;
  return d.toISOString().slice(0, 10);
}

export type PaymentVisualStatus = 'paid' | 'due' | 'future';

export interface PaymentReceiptRow {
  receiptDate: string | null;
  receiptAmount: number | null;
  principalPaid: number | null;
  indexationPaid: number | null;
  interestPaid: number | null;
  /** Index info for this sub-row (splits when multiple receipts). */
  indexMonth?: string | null;
  indexValue?: number | null;
  indexChangePercent?: number | null;
  indexationAmount?: number | null;
  /** Leftover balance sub-row (not an actual Monday receipt). */
  isRemainder?: boolean;
  /** Per-sub-row color; paid receipts stay green, יתרה can be red. */
  visualStatus?: PaymentVisualStatus;
}

export interface PaymentStatementRow {
  contractualDueDate: string | null;
  milestoneDescription: string;
  principalIncludingVat: number | null;
  receipts: PaymentReceiptRow[];
  currentBalance: number | null;
  statusLabel: string | null;
  visualStatus: PaymentVisualStatus;
  statusBadge: string;
  paymentCategory: PaymentCategory;
}

export interface PaymentStatementHeader {
  contractName: string;
  contractNumber: string | null;
  project: string | null;
  signingDate: string | null;
  apartmentDescription: string | null;
  buildingName: string | null;
  totalContractAmount: number | null;
  baseIndex: number | null;
  baseIndexPeriod: string | null;
  generationDate: string;
}

export interface PaymentStatementSummary {
  totalContractAmount: number | null;
  totalPrincipalPaid: number | null;
  totalIndexationPaid: number | null;
  currentRemainingBalance: number | null;
}

export interface PaymentStatementData {
  header: PaymentStatementHeader;
  summary: PaymentStatementSummary;
  rows: PaymentStatementRow[];
}

export interface CreatePaymentStatementResult {
  success: boolean;
  data?: PaymentStatementData;
  filename?: string;
  error?: string;
}

interface ContractHeaderRaw {
  id: string;
  name: string;
  contractNumber: string | null;
  project: string | null;
  signingDate: string | null;
  apartmentIds: number[];
  totalContractAmount: number | null;
}

interface ContractualPaymentExtras {
  principalIncludingVat: number | null;
  paymentStatusLabel: string | null;
}

interface SubitemRow {
  name: string;
  receiptAmount: number | null;
  principalPaid: number | null;
  indexationPaid: number | null;
  interestPaid: number | null;
  currentIndexValue: number | null;
  indexChangePercent: number | null;
  vatPercent: number | null;
  paymentCategory: string | null;
  createdAt: string;
}

async function fetchContractHeader(contractId: number): Promise<ContractHeaderRaw | null> {
  const cols = PDF_CONTRACTS_BOARD.columns;
  const query = `
    query GetContractHeader($itemId: ID!) {
      items(ids: [$itemId]) {
        id
        name
        column_values(ids: ["${cols.project}", "${cols.signingDate}", "${cols.apartmentRelation}", "${cols.totalContractAmount}", "${cols.contractNumber}"]) {
          id
          value
          text
          ... on BoardRelationValue {
            linked_item_ids
          }
          ... on MirrorValue {
            display_value
          }
          ... on LookupValue {
            display_value
          }
          ... on DateValue {
            date
          }
        }
      }
    }
  `;

  type ColumnValue = ColumnValueWithMirror;

  const data = await mondayQuery<{
    items: Array<{ id: string; name: string; column_values: ColumnValue[] }>;
  }>(query, { itemId: contractId });

  const item = data.items?.[0];
  if (!item) return null;

  let project: string | null = null;
  let signingDate: string | null = null;
  let apartmentIds: number[] = [];
  let totalContractAmount: number | null = null;
  let contractNumber: string | null = null;

  for (const cv of item.column_values) {
    if (cv.id === cols.project) {
      project = parseText(cv.value, cv.text);
    } else if (cv.id === cols.signingDate) {
      signingDate = parseLookupDate(cv);
    } else if (cv.id === cols.apartmentRelation) {
      apartmentIds = parseBoardRelationIds(cv.value, cv.linked_item_ids);
    } else if (cv.id === cols.contractNumber) {
      contractNumber = parseText(cv.value, cv.text);
    } else if (cv.id === cols.totalContractAmount) {
      totalContractAmount = parseLookupNumeric(cv.value, cv.text, cv.display_value);
    }
  }

  return {
    id: item.id,
    name: item.name ?? '',
    contractNumber,
    project,
    signingDate,
    apartmentIds,
    totalContractAmount,
  };
}

async function fetchApartmentDetails(
  apartmentId: number
): Promise<{
  apartmentName: string | null;
  buildingName: string | null;
  originalApartmentPrice: number | null;
}> {
  const cols = APARTMENTS_BOARD.columns;
  const query = `
    query GetApartment($itemId: ID!) {
      items(ids: [$itemId]) {
        id
        name
        column_values(ids: ["${cols.buildingRelation}", "${cols.originalApartmentPrice}"]) {
          id
          value
          text
          ... on BoardRelationValue {
            linked_item_ids
          }
          ... on MirrorValue {
            display_value
          }
          ... on LookupValue {
            display_value
          }
        }
      }
    }
  `;

  type ColumnValue = ColumnValueWithMirror;

  const data = await mondayQuery<{
    items: Array<{ id: string; name: string; column_values: ColumnValue[] }>;
  }>(query, { itemId: apartmentId });

  const item = data.items?.[0];
  if (!item) return { apartmentName: null, buildingName: null, originalApartmentPrice: null };

  const buildingCv = item.column_values.find((c) => c.id === cols.buildingRelation);
  const priceCv = item.column_values.find((c) => c.id === cols.originalApartmentPrice);
  const buildingIds = buildingCv
    ? parseBoardRelationIds(buildingCv.value, buildingCv.linked_item_ids)
    : [];

  let buildingName: string | null = buildingCv?.text?.trim() || buildingCv?.display_value?.trim() || null;
  const originalApartmentPrice = priceCv
    ? parseLookupNumeric(priceCv.value, priceCv.text, priceCv.display_value)
    : null;

  logger.info('Fetched original apartment price', {
    apartmentId,
    originalApartmentPrice,
    priceText: priceCv?.text ?? null,
    priceDisplay: priceCv?.display_value ?? null,
  });

  if (buildingIds.length > 0) {
    const buildingQuery = `
      query GetBuilding($itemId: ID!) {
        items(ids: [$itemId]) { id name }
      }
    `;
    const buildingData = await mondayQuery<{ items: Array<{ name: string }> }>(buildingQuery, {
      itemId: buildingIds[0],
    });
    buildingName = buildingData.items?.[0]?.name ?? buildingName;
  }

  return { apartmentName: item.name ?? null, buildingName, originalApartmentPrice };
}

async function fetchContractualPaymentExtras(
  itemId: string
): Promise<ContractualPaymentExtras> {
  const query = `
    query GetContractualExtras($itemId: ID!) {
      items(ids: [$itemId]) {
        column_values(ids: ["${PDF_CONTRACTUAL_PAYMENTS.principalIncludingVat}", "${CONTRACTUAL_PAYMENTS.items.paymentStatus}"]) {
          id
          value
          ... on StatusValue {
            label
          }
        }
      }
    }
  `;

  const data = await mondayQuery<{
    items: Array<{ column_values: Array<{ id: string; value?: string; label?: string }> }>;
  }>(query, { itemId: parseInt(itemId, 10) });

  const cvs = data.items?.[0]?.column_values ?? [];
  let principalIncludingVat: number | null = null;
  let paymentStatusLabel: string | null = null;

  for (const cv of cvs) {
    if (cv.id === PDF_CONTRACTUAL_PAYMENTS.principalIncludingVat) {
      principalIncludingVat = parseNumeric(cv.value ?? null);
    } else if (cv.id === CONTRACTUAL_PAYMENTS.items.paymentStatus) {
      paymentStatusLabel = parseStatusLabel(cv);
    }
  }

  return { principalIncludingVat, paymentStatusLabel };
}

async function fetchPaymentSubitems(
  parentItemId: string,
  paymentCategory: PaymentCategory
): Promise<SubitemRow[]> {
  const sub = CONTRACTUAL_PAYMENTS.subitems;
  const query = `
    query GetPaymentSubitems($parentId: ID!) {
      items(ids: [$parentId]) {
        subitems {
          name
          created_at
          column_values(ids: ["${sub.splitPaymentAfterVat}", "${sub.principalPayment}", "${sub.indexLinkage}", "${sub.interest}", "${sub.currentIndexValue}", "${sub.indexChangePercent}", "${sub.vatPercent}", "${sub.paymentCategory}"]) {
            id
            value
            ... on StatusValue {
              label
            }
          }
        }
      }
    }
  `;

  const data = await mondayQuery<{
    items: Array<{
      subitems: Array<{
        name: string;
        created_at: string;
        column_values: Array<{ id: string; value?: string; label?: string }>;
      }>;
    }>;
  }>(query, { parentId: parentItemId });

  const raw = data.items?.[0]?.subitems ?? [];

  return raw
    .map((s) => {
      let receiptAmount: number | null = null;
      let principalPaid: number | null = null;
      let indexationPaid: number | null = null;
      let interestPaid: number | null = null;
      let currentIndexValue: number | null = null;
      let indexChangePercent: number | null = null;
      let vatPercent: number | null = null;
      let paymentCategory: string | null = null;

      for (const cv of s.column_values) {
        if (cv.id === sub.splitPaymentAfterVat) receiptAmount = parseNumeric(cv.value ?? null);
        else if (cv.id === sub.principalPayment) principalPaid = parseNumeric(cv.value ?? null);
        else if (cv.id === sub.indexLinkage) indexationPaid = parseNumeric(cv.value ?? null);
        else if (cv.id === sub.interest) interestPaid = parseNumeric(cv.value ?? null);
        else if (cv.id === sub.currentIndexValue) currentIndexValue = parseNumeric(cv.value ?? null);
        else if (cv.id === sub.indexChangePercent) indexChangePercent = parseNumeric(cv.value ?? null);
        else if (cv.id === sub.vatPercent) vatPercent = parseNumeric(cv.value ?? null);
        else if (cv.id === sub.paymentCategory) paymentCategory = parseStatusLabel(cv);
      }

      return {
        name: s.name,
        receiptAmount,
        principalPaid,
        indexationPaid,
        interestPaid,
        currentIndexValue,
        indexChangePercent,
        vatPercent,
        paymentCategory,
        createdAt: s.created_at,
      };
    })
    .filter((s) => {
      if (s.paymentCategory == null) return paymentCategory === 'דירה';
      return s.paymentCategory === paymentCategory;
    })
    .sort((a, b) => new Date(a.createdAt).getTime() - new Date(b.createdAt).getTime());
}

function formatIndexPeriodHebrew(period: string | null): string | null {
  if (!period?.trim()) return null;
  const match = period.trim().match(/^(\d{1,2})-(\d{4})$/);
  if (!match) return period;
  const months = [
    'ינואר', 'פברואר', 'מרץ', 'אפריל', 'מאי', 'יוני',
    'יולי', 'אוגוסט', 'ספטמבר', 'אוקטובר', 'נובמבר', 'דצמבר',
  ];
  const monthIndex = parseInt(match[1], 10) - 1;
  if (monthIndex < 0 || monthIndex > 11) return period;
  return `${months[monthIndex]} ${match[2]}`;
}

function statusBadgeFor(
  visualStatus: PaymentVisualStatus,
  paymentCategory: PaymentCategory
): string {
  if (paymentCategory === 'רישום זכויות') {
    if (visualStatus === 'paid') return 'רישום זכויות · שולם';
    return 'רישום זכויות';
  }
  if (visualStatus === 'paid') return 'שולם';
  if (visualStatus === 'due') return 'פתוח';
  return 'עתידי';
}

function emptyReceipts(index?: {
  indexMonth?: string | null;
  indexValue?: number | null;
  indexChangePercent?: number | null;
  indexationAmount?: number | null;
  visualStatus?: PaymentVisualStatus;
}): PaymentReceiptRow[] {
  return [{
    receiptDate: null,
    receiptAmount: null,
    principalPaid: null,
    indexationPaid: null,
    interestPaid: null,
    indexMonth: index?.indexMonth ?? null,
    indexValue: index?.indexValue ?? null,
    indexChangePercent: index?.indexChangePercent ?? null,
    indexationAmount: index?.indexationAmount ?? null,
    visualStatus: index?.visualStatus,
  }];
}

/**
 * VAT rate for PDF display (all money columns are shown after VAT).
 * Prefer the rate stored on payment subitems; if the milestone has no receipts yet,
 * derive it from סכום מקורי כולל מע״מ ÷ קרן לפני מע״מ.
 */
function resolveDisplayVatRate(options: {
  fromSubitems: number | null | undefined;
  principalPreVat: number | null | undefined;
  principalInclVat: number | null | undefined;
  fallbackVat?: number | null;
}): number {
  const fromSub = options.fromSubitems;
  if (fromSub != null && Number.isFinite(fromSub) && fromSub !== 0) {
    return fromSub;
  }

  const pre = options.principalPreVat;
  const incl = options.principalInclVat;
  if (
    pre != null &&
    incl != null &&
    Number.isFinite(pre) &&
    Number.isFinite(incl) &&
    Math.abs(pre) > 1e-9
  ) {
    const ratio = incl / pre;
    // e.g. 1,880,000 / 1,593,220 ≈ 1.18 → return 0.18 (fraction; vatGrossMultiplier accepts it)
    if (Number.isFinite(ratio) && ratio > 1.001) {
      return ratio - 1;
    }
  }

  if (options.fallbackVat != null && Number.isFinite(options.fallbackVat) && options.fallbackVat !== 0) {
    return options.fallbackVat;
  }

  return 0;
}

/**
 * Indexation on remaining principal from the last receipt's index through today's index.
 * Presentation only — does not change allocation rules when applying payments.
 */
function indexationFromLastPaymentToToday(
  remainingPrincipal: number,
  lastPaymentIndex: number | null,
  todayIndex: number,
  indexLinked: boolean
): { indexation: number; indexChangePercent: number } {
  if (!indexLinked || remainingPrincipal <= 0 || !lastPaymentIndex || lastPaymentIndex <= 0 || todayIndex <= 0) {
    return { indexation: 0, indexChangePercent: 0 };
  }
  const ratio = todayIndex / lastPaymentIndex;
  const indexation = round(remainingPrincipal * (ratio - 1));
  const indexChangePercent = round((ratio - 1) * 100);
  return {
    indexation: Math.max(indexation, 0),
    indexChangePercent,
  };
}

function sanitizeFilename(name: string): string {
  const cleaned = name
    .replace(/[^\w\u0590-\u05FF\s-]/g, '')
    .trim()
    .replace(/\s+/g, '_');
  return cleaned || 'payment-statement';
}

export async function buildPaymentStatement(
  contractItemId: string
): Promise<CreatePaymentStatementResult> {
  const contractId = parseInt(contractItemId, 10);
  if (isNaN(contractId)) {
    return { success: false, error: 'Invalid contract item ID' };
  }

  logger.info('Building payment statement', { contractItemId });

  const [contractHeader, contractDetails, contractualItems] = await Promise.all([
    fetchContractHeader(contractId),
    fetchContractDetails(contractId),
    findAllContractualPaymentsForStatement(contractId),
  ]);

  logger.info('Payment statement data fetched', {
    contractItemId,
    contractualPaymentCount: contractualItems.length,
  });

  if (!contractHeader) {
    return { success: false, error: 'Contract not found' };
  }

  if (contractHeader.apartmentIds.length === 0) {
    return { success: false, error: 'Contract has no linked apartment' };
  }

  if (contractualItems.length === 0) {
    return { success: false, error: 'No contractual payments found for this contract' };
  }

  const apartmentDetails = await fetchApartmentDetails(contractHeader.apartmentIds[0]);
  if (!apartmentDetails.apartmentName) {
    return { success: false, error: 'Linked apartment not found' };
  }

  const today = new Date().toISOString().slice(0, 10);
  const indexToday = await fetchIndexForPaymentDate(today);

  const rows: PaymentStatementRow[] = [];
  let totalPrincipalPaid = 0;
  let totalIndexationPaid = 0;
  let currentRemainingBalance = 0;
  let nextPaymentAssigned = false;
  /** VAT learned from paid milestones — used when an unpaid line has no subitem VAT yet. */
  let contractVatFallback: number | null = null;

  for (const item of contractualItems) {
    const isRegistration = item.paymentCategory === 'רישום זכויות';
    const [extras, subitems] = await Promise.all([
      fetchContractualPaymentExtras(item.id),
      fetchPaymentSubitems(item.id, item.paymentCategory),
    ]);

    const vatFromSubitems =
      subitems.length > 0 ? (subitems[subitems.length - 1].vatPercent ?? null) : null;
    const vatPercent = resolveDisplayVatRate({
      fromSubitems: vatFromSubitems,
      principalPreVat: item.principal,
      principalInclVat: extras.principalIncludingVat,
      fallbackVat: contractVatFallback,
    });
    const vatMult = vatGrossMultiplier(vatPercent);
    if (vatPercent !== 0) {
      contractVatFallback = vatPercent;
    }

    const receipts: PaymentReceiptRow[] = await Promise.all(
      subitems.map(async (s) => {
        const lineVat = s.vatPercent ?? vatPercent;
        const lineVatMult = vatGrossMultiplier(lineVat);
        const indexationPaidGross =
          s.indexationPaid != null ? round(s.indexationPaid * lineVatMult) : null;
        const receiptIso = parseSubitemNameToIsoDate(s.name);
        let indexMonth: string | null = null;
        if (!isRegistration && receiptIso) {
          const idx = await fetchIndexForPaymentDate(receiptIso);
          indexMonth = formatIndexPeriodHebrew(idx?.period ?? null);
        }
        return {
          receiptDate: receiptIso ?? s.name,
          receiptAmount: s.receiptAmount,
          principalPaid:
            s.principalPaid != null ? round(s.principalPaid * lineVatMult) : null,
          indexationPaid: indexationPaidGross,
          interestPaid:
            s.interestPaid != null ? round(s.interestPaid * lineVatMult) : null,
          indexMonth: isRegistration ? null : indexMonth,
          indexValue: isRegistration ? null : s.currentIndexValue,
          indexChangePercent: isRegistration ? null : s.indexChangePercent,
          indexationAmount: isRegistration ? null : indexationPaidGross,
          visualStatus: 'paid' as const,
        };
      })
    );

    for (const s of subitems) {
      const lineVat = s.vatPercent ?? vatPercent;
      const lineVatMult = vatGrossMultiplier(lineVat);
      totalPrincipalPaid += round((s.principalPaid ?? 0) * lineVatMult);
      totalIndexationPaid += round((s.indexationPaid ?? 0) * lineVatMult);
    }

    const previous = await getPreviousSubitemBalances(
      item.id,
      item.principal,
      item.paymentCategory
    );
    // Keep the signed remaining principal — negative = open credit / discount
    const remainingPrincipalRaw = previous.remainingPrincipal;
    const remainingPrincipalPositive = Math.max(remainingPrincipalRaw, 0);
    const isCredit = remainingPrincipalRaw < 0 || item.principal < 0;
    const paymentBase = remainingPrincipalPositive;

    const { balances, remaining } = await computeBalancesBeforePayment(
      item.id,
      item,
      today,
      today,
      paymentBase,
      contractDetails as ContractDetails | null,
      indexToday?.value ?? 100,
      indexToday?.period ?? ''
    );

    // Do not clamp to ≥0 — open credits (negative principal) must still appear
    const balancePreVat = round(
      remaining.principal + remaining.interest + remaining.indexation
    );
    const isFullyPaid =
      !isCredit &&
      (extras.paymentStatusLabel === 'הושלם' ||
        balancePreVat === 0);
    const hasReceipts = subitems.length > 0;

    if (isCredit) {
      logger.info('Payment statement credit row detected', {
        itemId: item.id,
        name: item.name,
        paymentCategory: item.paymentCategory,
        status: extras.paymentStatusLabel,
        principal: item.principal,
        remainingPrincipal: remainingPrincipalRaw,
        balancePreVat,
      });
    }

    const latest = hasReceipts ? subitems[subitems.length - 1] : null;

    // Fully paid: one contractual row with green receipt sub-rows
    if (isFullyPaid) {
      rows.push({
        contractualDueDate: item.contractualDueDate,
        milestoneDescription: item.name,
        principalIncludingVat: extras.principalIncludingVat,
        receipts,
        currentBalance: 0,
        statusLabel: extras.paymentStatusLabel,
        visualStatus: 'paid',
        statusBadge: statusBadgeFor('paid', item.paymentCategory),
        paymentCategory: item.paymentCategory,
      });
      continue;
    }

    // ── Unpaid remainder (same contractual row; as a red יתרה sub-row when partial) ──
    // Credits and registration lines never take the red "next apartment payment" slot
    const isNextPayment =
      !nextPaymentAssigned && !isCredit && !isRegistration;
    let visualStatus: PaymentVisualStatus;
    let indexMonth: string | null = null;
    let indexValue: number | null = null;
    let indexChangePercent: number | null = null;
    let indexationAmount: number | null = null;
    let interestInclVat: number | null = null;
    let currentBalance: number | null = null;
    // Preserve sign so negative credits display as negative amounts
    const residualPrincipalInclVat = round(remainingPrincipalRaw * vatMult);

    if (isNextPayment) {
      visualStatus = 'due';
      nextPaymentAssigned = true;

      const lastPaymentIndex =
        latest?.currentIndexValue && latest.currentIndexValue > 0
          ? latest.currentIndexValue
          : balances.indexationBaseIndex || null;
      const todayIndex = indexToday?.value ?? balances.currentIndexValue;
      const accrued = indexationFromLastPaymentToToday(
        remainingPrincipalPositive,
        lastPaymentIndex,
        todayIndex,
        item.indexLinkedStatus !== 'X'
      );
      const interestPreVat = Math.max(remaining.interest, 0);
      const indexationPreVat = accrued.indexation;
      interestInclVat = round(interestPreVat * vatMult);
      currentBalance = round(
        (remainingPrincipalPositive + interestPreVat + indexationPreVat) * vatMult
      );
      indexMonth = formatIndexPeriodHebrew(indexToday?.period ?? null);
      indexValue = todayIndex || null;
      indexChangePercent = accrued.indexChangePercent;
      indexationAmount = round(indexationPreVat * vatMult);
    } else if (isRegistration || isCredit) {
      // Open registration / credit: show signed principal only (no indexation)
      visualStatus = 'future';
      currentBalance = residualPrincipalInclVat;
      indexMonth = null;
      indexValue = null;
      indexChangePercent = null;
      indexationAmount = null;
      interestInclVat = null;
    } else {
      visualStatus = 'future';
      currentBalance = residualPrincipalInclVat;
      indexMonth = null;
      indexValue = null;
      indexChangePercent = null;
      indexationAmount = null;
      interestInclVat = null;
    }

    currentRemainingBalance += currentBalance ?? 0;

    // When there are already actual receipts, append leftover as a sub-row (like another receipt)
    // instead of duplicating the contractual payment as a second parent row.
    if (hasReceipts) {
      receipts.push({
        receiptDate: null,
        receiptAmount: currentBalance,
        principalPaid: residualPrincipalInclVat,
        indexationPaid: indexationAmount,
        interestPaid: interestInclVat,
        indexMonth: isRegistration ? null : indexMonth,
        indexValue: isRegistration ? null : indexValue,
        indexChangePercent: isRegistration ? null : indexChangePercent,
        indexationAmount: isRegistration ? null : indexationAmount,
        isRemainder: true,
        visualStatus,
      });
    }

    rows.push({
      contractualDueDate: item.contractualDueDate,
      milestoneDescription: item.name,
      // Always the full contractual principal — leftover lives in the יתרה sub-row
      principalIncludingVat:
        extras.principalIncludingVat ?? residualPrincipalInclVat,
      receipts: hasReceipts
        ? receipts
        : emptyReceipts({
            indexMonth: isRegistration ? null : indexMonth,
            indexValue: isRegistration ? null : indexValue,
            indexChangePercent: isRegistration ? null : indexChangePercent,
            indexationAmount: isRegistration ? null : indexationAmount,
            visualStatus,
          }),
      currentBalance,
      statusLabel: extras.paymentStatusLabel,
      visualStatus,
      statusBadge: statusBadgeFor(visualStatus, item.paymentCategory),
      paymentCategory: item.paymentCategory,
    });
  }

  const apartmentDescription = [
    contractHeader.project,
    apartmentDetails.apartmentName,
    apartmentDetails.buildingName ? `בניין ${apartmentDetails.buildingName}` : null,
  ]
    .filter(Boolean)
    .join(', ');

  const resolvedApartmentPrice =
    apartmentDetails.originalApartmentPrice ?? contractHeader.totalContractAmount;

  logger.info('Resolved מחיר דירה מקורי for PDF', {
    contractItemId,
    fromApartmentLookup: apartmentDetails.originalApartmentPrice,
    fromContractFallback: contractHeader.totalContractAmount,
    resolved: resolvedApartmentPrice,
  });

  const data: PaymentStatementData = {
    header: {
      contractName: contractHeader.name,
      contractNumber: contractHeader.contractNumber,
      project: contractHeader.project,
      signingDate: contractHeader.signingDate,
      apartmentDescription: apartmentDescription || apartmentDetails.apartmentName,
      buildingName: apartmentDetails.buildingName,
      totalContractAmount: resolvedApartmentPrice,
      baseIndex: contractDetails?.baseIndex ?? null,
      baseIndexPeriod: contractDetails?.baseIndexPeriod ?? null,
      generationDate: today,
    },
    summary: {
      totalContractAmount: resolvedApartmentPrice,
      totalPrincipalPaid: round(totalPrincipalPaid),
      totalIndexationPaid: round(totalIndexationPaid),
      currentRemainingBalance: round(currentRemainingBalance),
    },
    rows,
  };

  return {
    success: true,
    data,
    filename: `${sanitizeFilename(contractHeader.name)}-payment-statement.pdf`,
  };
}

export async function createPaymentStatementPdf(
  contractItemId: string
): Promise<{ success: boolean; buffer?: Buffer; filename?: string; error?: string }> {
  const result = await buildPaymentStatement(contractItemId);
  if (!result.success || !result.data) {
    return { success: false, error: result.error ?? 'Failed to build payment statement' };
  }

  try {
    const { renderPaymentStatementPdf } = await import('./pdfStatementRenderer.js');
    const buffer = await renderPaymentStatementPdf(result.data);
    return { success: true, buffer, filename: result.filename };
  } catch (err) {
    logger.warn('PDF rendering failed', { contractItemId, err });
    return {
      success: false,
      error: err instanceof Error ? err.message : 'PDF rendering failed',
    };
  }
}
