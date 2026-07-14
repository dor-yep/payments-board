/**
 * Updates contract board columns after PDF generation:
 * status (סיים / נכשל), file column clear, and PDF upload.
 */

import { PDF_CONTRACTS_BOARD } from '../config/config';
import { logger } from '../logger';
import { mondayQuery } from './mondayApi';

export type PdfGenerationStatus = 'סיים' | 'נכשל';

const FILE_UPLOAD_URL = 'https://api.monday.com/v2/file';

export async function updatePdfGenerationStatus(
  contractItemId: string,
  status: PdfGenerationStatus
): Promise<void> {
  const cols = PDF_CONTRACTS_BOARD.columns;
  const mutation = `
    mutation UpdatePdfGenerationStatus($itemId: ID!, $boardId: ID!, $columnValues: JSON!) {
      change_multiple_column_values(
        item_id: $itemId,
        board_id: $boardId,
        column_values: $columnValues
      ) {
        id
      }
    }
  `;

  const columnValues = JSON.stringify({
    [cols.pdfGenerationStatus]: { label: status },
  });

  await mondayQuery(mutation, {
    itemId: parseInt(contractItemId, 10),
    boardId: parseInt(PDF_CONTRACTS_BOARD.boardId, 10),
    columnValues,
  });

  logger.info('Updated PDF generation status', { contractItemId, status });
}

export async function clearContractPdfFiles(contractItemId: string): Promise<void> {
  const cols = PDF_CONTRACTS_BOARD.columns;
  const mutation = `
    mutation ClearContractPdfFiles($itemId: ID!, $boardId: ID!, $columnValues: JSON!) {
      change_multiple_column_values(
        item_id: $itemId,
        board_id: $boardId,
        column_values: $columnValues
      ) {
        id
      }
    }
  `;

  const columnValues = JSON.stringify({
    [cols.pdfDocument]: { clear_all: true },
  });

  await mondayQuery(mutation, {
    itemId: parseInt(contractItemId, 10),
    boardId: parseInt(PDF_CONTRACTS_BOARD.boardId, 10),
    columnValues,
  });

  logger.info('Cleared contract PDF file column', { contractItemId });
}

export async function uploadContractPdfFile(
  contractItemId: string,
  pdfBuffer: Buffer,
  filename: string
): Promise<void> {
  const token = process.env.MONDAY_API_TOKEN;
  if (!token) {
    throw new Error('MONDAY_API_TOKEN is not set');
  }

  const columnId = PDF_CONTRACTS_BOARD.columns.pdfDocument;
  const query = `
    mutation AddContractPdf($file: File!, $itemId: ID!, $columnId: String!) {
      add_file_to_column(item_id: $itemId, column_id: $columnId, file: $file) {
        id
      }
    }
  `;

  const variables = {
    itemId: parseInt(contractItemId, 10),
    columnId,
  };

  const form = new FormData();
  form.append('query', query);
  form.append('variables', JSON.stringify(variables));
  form.append('map', JSON.stringify({ file: 'variables.file' }));
  form.append('file', new Blob([new Uint8Array(pdfBuffer)], { type: 'application/pdf' }), filename);

  const res = await fetch(FILE_UPLOAD_URL, {
    method: 'POST',
    headers: { Authorization: token },
    body: form,
  });

  const json = (await res.json()) as {
    data?: { add_file_to_column?: { id: string } };
    errors?: Array<{ message: string }>;
  };

  if (json.errors?.length) {
    const msg = json.errors.map((e) => e.message).join('; ');
    throw new Error(`Monday file upload error: ${msg}`);
  }

  if (!json.data?.add_file_to_column?.id) {
    throw new Error('Monday file upload returned no asset ID');
  }

  logger.info('Uploaded contract PDF to Monday', {
    contractItemId,
    filename,
    assetId: json.data.add_file_to_column.id,
  });
}

export async function finalizeContractPdfOnBoard(
  contractItemId: string,
  pdfBuffer: Buffer,
  filename: string
): Promise<void> {
  await clearContractPdfFiles(contractItemId);
  await uploadContractPdfFile(contractItemId, pdfBuffer, filename);
  await updatePdfGenerationStatus(contractItemId, 'סיים');
}

export async function markContractPdfFailed(contractItemId: string): Promise<void> {
  try {
    await updatePdfGenerationStatus(contractItemId, 'נכשל');
  } catch (err) {
    logger.warn('Failed to set PDF generation status to נכשל', { contractItemId, err });
  }
}
