import { mondayQuery } from './mondayApi';

export type VxStatus = 'V' | 'X';

type StatusColumnValue = {
  label?: string | null;
  text?: string | null;
  index?: number | null;
  value?: string | null;
};

function toNumericIndex(value: unknown): number | null {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value === 'string' && value.trim() !== '') {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  return null;
}

function statusIndicatesTrue(labelText: string): boolean {
  const normalized = labelText.trim().toUpperCase();
  if (!normalized) return false;
  return (
    normalized === 'V' ||
    normalized === 'TRUE' ||
    normalized === 'YES' ||
    normalized === 'כן' ||
    normalized.includes('✅') ||
    normalized.includes('✔') ||
    normalized.includes('☑')
  );
}

function statusIndicatesFalse(labelText: string): boolean {
  const normalized = labelText.trim().toUpperCase();
  if (!normalized) return false;
  return (
    normalized === 'X' ||
    normalized === 'FALSE' ||
    normalized === 'NO' ||
    normalized === 'לא' ||
    normalized.includes('❌') ||
    normalized === '✗' ||
    normalized === '✕'
  );
}

/** Resolve V/X from visible label text (not status index). */
export function parseVxFromLabel(label: string): VxStatus | null {
  const trimmed = label.trim();
  if (!trimmed) return null;
  if (statusIndicatesFalse(trimmed)) return 'X';
  if (statusIndicatesTrue(trimmed)) return 'V';
  const upper = trimmed.toUpperCase();
  if (upper === 'X') return 'X';
  if (upper === 'V') return 'V';
  return null;
}

const statusColumnLabelCache = new Map<string, Record<string, string>>();

async function fetchStatusColumnLabelByIndex(
  boardId: string,
  columnId: string,
  statusIndex: number
): Promise<string | null> {
  const cacheKey = `${boardId}:${columnId}`;
  let labels = statusColumnLabelCache.get(cacheKey);
  if (!labels) {
    const labelsQuery = `
      query StatusColumnLabels($boardId: ID!, $columnId: String!) {
        boards(ids: [$boardId]) {
          columns(ids: [$columnId]) {
            settings_str
          }
        }
      }
    `;
    const labelsData = await mondayQuery<{
      boards: Array<{ columns: Array<{ settings_str?: string | null }> }>;
    }>(labelsQuery, {
      boardId: parseInt(boardId, 10),
      columnId,
    });
    const settingsStr = labelsData.boards?.[0]?.columns?.[0]?.settings_str ?? null;
    labels = {};
    if (settingsStr) {
      try {
        const settings = JSON.parse(settingsStr) as { labels?: Record<string, string> };
        labels = settings.labels ?? {};
      } catch {
        labels = {};
      }
    }
    statusColumnLabelCache.set(cacheKey, labels);
  }
  const mapped = labels[String(statusIndex)] ?? null;
  return typeof mapped === 'string' ? mapped.trim() || null : null;
}

/**
 * Resolve contractual V/X status from label text (V, X, icons), not from status index id.
 * Index is only used to look up the configured label string in column settings.
 */
export async function resolveVxStatus(
  cv: StatusColumnValue | undefined,
  boardId: string,
  columnId: string,
  defaultWhenUnset: VxStatus = 'V'
): Promise<VxStatus> {
  if (!cv) return defaultWhenUnset;

  const fromGraphql = parseVxFromLabel((cv.label ?? cv.text ?? '').toString());
  if (fromGraphql) return fromGraphql;

  let statusIndex = toNumericIndex(cv.index);
  try {
    const parsed = JSON.parse(cv.value || '{}');
    const fromInner = parseVxFromLabel((parsed.label ?? parsed.text ?? '').toString());
    if (fromInner) return fromInner;

    statusIndex =
      statusIndex ??
      toNumericIndex(parsed.index) ??
      toNumericIndex(parsed?.additional_info?.index);
  } catch {
    // ignore
  }

  if (statusIndex !== null) {
    const mappedLabel = await fetchStatusColumnLabelByIndex(boardId, columnId, statusIndex);
    const fromMapped = parseVxFromLabel(mappedLabel ?? '');
    if (fromMapped) return fromMapped;
  }

  return defaultWhenUnset;
}
