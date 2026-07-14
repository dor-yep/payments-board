import { Router, Request, Response } from 'express';
import { mondayWebhookAuth } from '../middleware/mondayAuth';
import { logger } from '../logger';
import { applyPayment } from '../services/paymentAllocation';
import {
  applySupplierPayment,
  calculateSupplierPayment,
} from '../services/supplierPaymentAllocation';
import { ACTUAL_PAYMENTS, PDF_CONTRACTS_BOARD } from '../config/config';
import { createPaymentStatementPdf } from '../services/paymentStatement';
import {
  finalizeContractPdfOnBoard,
  markContractPdfFailed,
} from '../services/contractPdfBoard';
import { fetchCbsIndex, getLatestConstructionIndex, CBS_INDEX_CODES } from '../services/cbsApi';
import { createIndexItem, fillConstructionIndex } from '../services/indexBoard';
import { INDEX_BOARD } from '../config/config';

const router = Router();

/** In-memory idempotency: skip duplicate webhooks within 5 minutes */
const processedWebhooks = new Map<string, number>();
/** Prevents concurrent PDF generation for the same contract; regenerating after completion is allowed */
const pdfGenerationInFlight = new Set<string>();
const IDEMPOTENCY_TTL_MS = 5 * 60 * 1000;

function getIdempotencyKey(event: { pulseId?: number; itemId?: number; triggerTime?: string; boardId?: number }): string {
  const id = event.pulseId ?? event.itemId ?? 'unknown';
  const time = event.triggerTime ?? '';
  const board = event.boardId ?? '';
  return `${board}:${id}:${time}`;
}

function isDuplicateWebhook(event: Record<string, unknown>): boolean {
  const key = getIdempotencyKey(event as Parameters<typeof getIdempotencyKey>[0]);
  const now = Date.now();
  if (processedWebhooks.has(key)) {
    return true;
  }
  processedWebhooks.set(key, now);
  for (const [k, t] of processedWebhooks) {
    if (now - t > IDEMPOTENCY_TTL_MS) processedWebhooks.delete(k);
  }
  return false;
}

function getActualPaymentItemId(event: Record<string, unknown>): string | null {
  const id = event.pulseId ?? event.itemId;
  if (id != null) return String(id);
  return null;
}

function extractItemIdFromFields(fields: Record<string, unknown>): string | null {
  const raw = fields.itemId ?? fields.pulseId ?? fields.item;
  if (raw == null) return null;
  if (typeof raw === 'number' || typeof raw === 'string') return String(raw);
  if (typeof raw === 'object') {
    const obj = raw as Record<string, unknown>;
    const id = obj.id ?? obj.itemId ?? obj.pulseId;
    if (id != null) return String(id);
  }
  return null;
}

interface CreatePdfRequestContext {
  itemId: string;
  boardId: string | null;
  source: 'webhook' | 'action';
}

function parseCreatePdfRequest(body: Record<string, unknown>): CreatePdfRequestContext | null {
  if (body.event && typeof body.event === 'object') {
    const event = body.event as Record<string, unknown>;
    const itemId = getActualPaymentItemId(event);
    if (!itemId) return null;

    const boardIdRaw = event.boardId ?? event.board_id;
    return {
      itemId,
      boardId: boardIdRaw != null ? String(boardIdRaw) : null,
      source: 'webhook',
    };
  }

  if (body.payload && typeof body.payload === 'object') {
    const payload = body.payload as Record<string, unknown>;
    const fields = (payload.inputFields ?? payload.inboundFieldValues) as
      | Record<string, unknown>
      | undefined;
    if (!fields) return null;

    const itemId = extractItemIdFromFields(fields);
    if (!itemId) return null;

    const boardIdRaw = fields.boardId ?? fields.board_id;

    return {
      itemId,
      boardId: boardIdRaw != null ? String(boardIdRaw) : null,
      source: 'action',
    };
  }

  return null;
}

router.post('/apply-payment', mondayWebhookAuth, applyPaymentHandler);
router.post('/apply-payment/', mondayWebhookAuth, applyPaymentHandler);
router.post('/apply-supplier-payment', mondayWebhookAuth, applySupplierPaymentHandler);
router.post('/apply-supplier-payment/', mondayWebhookAuth, applySupplierPaymentHandler);
router.post('/calculate-supplier-payment', mondayWebhookAuth, calculateSupplierPaymentHandler);
router.post('/calculate-supplier-payment/', mondayWebhookAuth, calculateSupplierPaymentHandler);
router.post('/createPDF', mondayWebhookAuth, createPdfHandler);
router.post('/createPDF/', mondayWebhookAuth, createPdfHandler);
async function applyPaymentHandler(req: Request, res: Response) {
  const body = req.body;

  if (body.challenge) {
    logger.info('Webhook challenge received');
    return res.status(200).json({ challenge: body.challenge });
  }

  const event = body.event;
  if (!event) {
    logger.warn('Missing event in webhook payload');
    return res.status(400).send({ error: 'Missing event payload' });
  }

  const boardId = event.boardId ?? event.board_id;
  if (boardId != null && String(boardId) !== ACTUAL_PAYMENTS.boardId) {
    logger.info('Webhook from non-Actual-Payments board, ignoring', { boardId });
    return res.status(200).json({ received: true, skipped: 'wrong_board' });
  }

  if (isDuplicateWebhook(event)) {
    logger.info('Duplicate webhook, skipping', { event });
    return res.status(200).json({ received: true, skipped: 'duplicate' });
  }

  const itemId = getActualPaymentItemId(event);
  if (!itemId) {
    logger.warn('Webhook event has no pulseId/itemId');
    return res.status(400).send({ error: 'Missing item ID in webhook' });
  }

  logger.info('Applying payment for actual payment item', { itemId });

  try {
    const result = await applyPayment({ actualPaymentItemId: itemId });
    if (result.success) {
      return res.status(200).json({
        received: true,
        subitemsCreated: result.subitemsCreated,
      });
    }
    logger.warn('Payment application failed', { itemId, error: result.error });
    return res.status(400).json({ error: result.error });
  } catch (err) {
    logger.warn('Payment application error', { itemId, err });
    return res.status(500).json({
      error: err instanceof Error ? err.message : 'Internal server error',
    });
  }
}

async function createPdfHandler(req: Request, res: Response) {
  const body = req.body as Record<string, unknown>;

  logger.info('createPDF request received', {
    hasChallenge: typeof body.challenge === 'string',
    hasEvent: !!body.event,
    hasPayload: !!body.payload,
  });

  if (typeof body.challenge === 'string') {
    logger.info('createPDF webhook challenge received');
    return res.status(200).json({ challenge: body.challenge });
  }

  const request = parseCreatePdfRequest(body);
  if (!request) {
    logger.warn('Unrecognized createPDF payload', {
      keys: Object.keys(body),
      payloadKeys:
        body.payload && typeof body.payload === 'object'
          ? Object.keys(body.payload as Record<string, unknown>)
          : [],
    });
    return res.status(400).json({ error: 'Missing contract item ID in request payload' });
  }

  const { itemId, boardId, source } = request;

  if (
    source === 'webhook' &&
    boardId != null &&
    boardId !== PDF_CONTRACTS_BOARD.boardId
  ) {
    logger.info('createPDF webhook from non-Contracts board, ignoring', { boardId });
    return res.status(200).json({ received: true, skipped: 'wrong_board' });
  }

  if (pdfGenerationInFlight.has(itemId)) {
    logger.info('createPDF already in progress, skipping', { itemId, source });
    return res.status(200).json({ received: true, skipped: 'in_progress' });
  }

  pdfGenerationInFlight.add(itemId);
  logger.info('Generating payment statement PDF', { contractItemId: itemId, source, boardId });

  try {
    logger.info('createPDF: fetching contract data', { contractItemId: itemId });
    const result = await createPaymentStatementPdf(itemId);
    if (!result.success || !result.buffer) {
      logger.warn('PDF generation failed', { itemId, error: result.error });
      await markContractPdfFailed(itemId);
      return res.status(400).json({ error: result.error ?? 'PDF generation failed' });
    }

    const filename = result.filename ?? 'payment-statement.pdf';
    logger.info('createPDF: uploading to Monday', { contractItemId: itemId, filename });

    try {
      await finalizeContractPdfOnBoard(itemId, result.buffer, filename);
    } catch (uploadErr) {
      logger.warn('PDF upload to Monday failed', { itemId, uploadErr });
      await markContractPdfFailed(itemId);
      return res.status(500).json({
        error:
          uploadErr instanceof Error
            ? uploadErr.message
            : 'PDF created but failed to upload to Monday',
      });
    }

    logger.info('createPDF: completed successfully', { contractItemId: itemId, filename });
    return res.status(200).json({ success: true, filename });
  } catch (err) {
    logger.warn('createPDF error', { itemId, err });
    await markContractPdfFailed(itemId);
    return res.status(500).json({
      error: err instanceof Error ? err.message : 'Internal server error',
    });
  } finally {
    pdfGenerationInFlight.delete(itemId);
  }
}

async function applySupplierPaymentHandler(req: Request, res: Response) {
  const body = req.body;

  if (body.challenge) {
    logger.info('Supplier webhook challenge received');
    return res.status(200).json({ challenge: body.challenge });
  }

  const event = body.event;
  if (!event) {
    logger.warn('Missing event in supplier webhook payload');
    return res.status(400).send({ error: 'Missing event payload' });
  }

  const boardId = event.boardId ?? event.board_id;
  if (boardId != null && String(boardId) !== '5092501259') {
    logger.info('Webhook from non-Supplier-Payments board, ignoring', { boardId });
    return res.status(200).json({ received: true, skipped: 'wrong_board' });
  }

  if (isDuplicateWebhook(event)) {
    logger.info('Duplicate supplier webhook, skipping', { event });
    return res.status(200).json({ received: true, skipped: 'duplicate' });
  }

  const itemId = getActualPaymentItemId(event);
  if (!itemId) {
    logger.warn('Supplier webhook event has no pulseId/itemId');
    return res.status(400).send({ error: 'Missing item ID in webhook' });
  }

  logger.info('Applying supplier payment for item', { itemId });

  try {
    const result = await applySupplierPayment({ supplierPaymentItemId: itemId });
    if (result.success) {
      return res.status(200).json({
        received: true,
        subitemId: result.subitemId,
        principalPayment: result.principalPayment,
        indexedPayment: result.indexedPayment,
      });
    }
    logger.warn('Supplier payment application failed', { itemId, error: result.error });
    return res.status(400).json({ error: result.error });
  } catch (err) {
    logger.warn('Supplier payment application error', { itemId, err });
    return res.status(500).json({
      error: err instanceof Error ? err.message : 'Internal server error',
    });
  }
}

async function calculateSupplierPaymentHandler(req: Request, res: Response) {
  const body = req.body;

  if (body.challenge) {
    logger.info('Supplier calculation webhook challenge received');
    return res.status(200).json({ challenge: body.challenge });
  }

  const event = body.event;
  if (!event) {
    logger.warn('Missing event in supplier calculation webhook payload');
    return res.status(400).send({ error: 'Missing event payload' });
  }

  const boardId = event.boardId ?? event.board_id;
  if (boardId != null && String(boardId) !== '5092501259') {
    logger.info('Webhook from non-Supplier-Payments board, ignoring', { boardId });
    return res.status(200).json({ received: true, skipped: 'wrong_board' });
  }

  if (isDuplicateWebhook(event)) {
    logger.info('Duplicate supplier calculation webhook, skipping', { event });
    return res.status(200).json({ received: true, skipped: 'duplicate' });
  }

  const itemId = getActualPaymentItemId(event);
  if (!itemId) {
    logger.warn('Supplier calculation webhook event has no pulseId/itemId');
    return res.status(400).send({ error: 'Missing item ID in webhook' });
  }

  logger.info('Calculating supplier payment for item', { itemId });

  try {
    const result = await calculateSupplierPayment({ supplierPaymentItemId: itemId });
    if (result.success) {
      return res.status(200).json({
        received: true,
        principalPayment: result.principalPayment,
        indexedPayment: result.indexedPayment,
        totalPayment: result.totalPayment,
      });
    }
    logger.warn('Supplier payment calculation failed', { itemId, error: result.error });
    return res.status(400).json({ error: result.error });
  } catch (err) {
    logger.warn('Supplier payment calculation error', { itemId, err });
    return res.status(500).json({
      error: err instanceof Error ? err.message : 'Internal server error',
    });
  }
}

router.post('/fill-construction-index', fillConstructionIndexHandler);
async function fillConstructionIndexHandler(req: Request, res: Response) {
  if (req.body?.challenge) {
    logger.info('fill-construction-index: webhook challenge received');
    return res.status(200).json({ challenge: req.body.challenge });
  }

  try {
    logger.info('fill-construction-index: starting');
    const result = await fillConstructionIndex();
    if (result.success) {
      return res.status(200).json({
        success: true,
        created: result.created,
        updated: result.updated,
      });
    }
    return res.status(502).json({
      success: false,
      error: result.error,
    });
  } catch (err) {
    logger.warn('fill-construction-index error', { err });
    return res.status(500).json({
      success: false,
      error: err instanceof Error ? err.message : 'Internal server error',
    });
  }
}

router.post('/get-index', getIndexHandler);
async function getIndexHandler(req: Request, res: Response) {
  if (req.body?.challenge) {
    logger.info('get-index: webhook challenge received');
    return res.status(200).json({ challenge: req.body.challenge });
  }

  const updateDate = new Date().toISOString().slice(0, 10);

  try {
    logger.info('get-index: fetching CBS indices');

    // CBS series 200010 (מדד מחירי תשומה בבנייה למגורים), 120010 (Consumer Price)
    const [constructionResult, consumerResult] = await Promise.all([
      getLatestConstructionIndex(),
      fetchCbsIndex(CBS_INDEX_CODES.CONSUMER_PRICE, 'Consumer Price Index'),
    ]);

    if (!constructionResult.success || !constructionResult.latest) {
      logger.warn('get-index: Construction Input fetch failed', constructionResult);
      return res.status(502).json({
        success: false,
        error: constructionResult.error ?? 'Failed to fetch Construction Input Price Index',
        details: { construction: constructionResult, consumer: consumerResult },
      });
    }

    if (!consumerResult.success || !consumerResult.latest) {
      logger.warn('get-index: Consumer Price fetch failed', consumerResult);
      return res.status(502).json({
        success: false,
        error: consumerResult.error ?? 'Failed to fetch Consumer Price Index',
        details: { construction: constructionResult, consumer: consumerResult },
      });
    }

    const [constructionItem, consumerItem] = await Promise.all([
      createIndexItem(
        INDEX_BOARD.groups.constructionInput,
        constructionResult.latest,
        updateDate
      ),
      createIndexItem(
        INDEX_BOARD.groups.consumerPrice,
        consumerResult.latest,
        updateDate
      ),
    ]);

    if (!constructionItem.success || !consumerItem.success) {
      const errors = [
        constructionItem.error && `Construction: ${constructionItem.error}`,
        consumerItem.error && `Consumer: ${consumerItem.error}`,
      ].filter(Boolean);
      return res.status(500).json({
        success: false,
        error: errors.join('; '),
        constructionItemId: constructionItem.itemId,
        consumerItemId: consumerItem.itemId,
      });
    }

    return res.status(200).json({
      success: true,
      constructionItemId: constructionItem.itemId,
      consumerItemId: consumerItem.itemId,
      constructionIndex: constructionResult.latest,
      consumerIndex: consumerResult.latest,
      updateDate,
    });
  } catch (err) {
    logger.warn('get-index error', { err });
    return res.status(500).json({
      success: false,
      error: err instanceof Error ? err.message : 'Internal server error',
    });
  }
}

export default router;
