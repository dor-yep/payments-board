import { mondayQuery } from './mondayApi';

export type VxStatus = 'V' | 'X';

type StatusColumnValue = {
  label?: string | null;
  text?: string | null;
  index?: number | null;
  value?: string | null;
};

type StatusColorInfo = {
  color?: string;
  border?: string;
  var_name?: string;
};

type StatusColumnSettings = {
  labels: Record<string, string>;
  labels_colors: Record<string, StatusColorInfo>;
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
    normalized.includes('☑') ||
    normalized.includes('✓')
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
    normalized.includes('✗') ||
    normalized.includes('✕') ||
    normalized.includes('✘')
  );
}

/** Resolve V/X from visible label text or icon characters (not status index). */
export function parseVxFromLabel(label: string): VxStatus | null {
  if (!label) return null;
  if (statusIndicatesFalse(label)) return 'X';
  if (statusIndicatesTrue(label)) return 'V';
  const upper = label.trim().toUpperCase();
  if (upper === 'X') return 'X';
  if (upper === 'V') return 'V';
  return null;
}

function inferVxFromColorVar(varName: string | undefined): VxStatus | null {
  const v = (varName ?? '').toLowerCase();
  if (!v) return null;
  if (v.includes('red') || v.includes('stuck') || v.includes('salmon')) return 'X';
  if (v.includes('green') || v.includes('done') || v.includes('grass')) return 'V';
  return null;
}

function inferVxFromStatusColor(colorHex: string | undefined): VxStatus | null {
  if (!colorHex?.startsWith('#') || colorHex.length < 7) return null;
  const r = parseInt(colorHex.slice(1, 3), 16);
  const g = parseInt(colorHex.slice(3, 5), 16);
  const b = parseInt(colorHex.slice(5, 7), 16);
  if ([r, g, b].some((n) => Number.isNaN(n))) return null;

  // Monday reds (e.g. stuck / X icon)
  if (r > 150 && r > g + 25 && r > b + 25) return 'X';
  // Monday greens (e.g. done / V icon)
  if (g > 90 && g > r + 15 && g >= b - 10) return 'V';
  return null;
}

const statusColumnSettingsCache = new Map<string, StatusColumnSettings>();

async function fetchStatusColumnSettings(
  boardId: string,
  columnId: string
): Promise<StatusColumnSettings> {
  const cacheKey = `${boardId}:${columnId}`;
  const cached = statusColumnSettingsCache.get(cacheKey);
  if (cached) return cached;

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

  const settings: StatusColumnSettings = { labels: {}, labels_colors: {} };
  const settingsStr = labelsData.boards?.[0]?.columns?.[0]?.settings_str ?? null;
  if (settingsStr) {
    try {
      const parsed = JSON.parse(settingsStr) as {
        labels?: Record<string, string>;
        labels_colors?: Record<string, StatusColorInfo>;
      };
      settings.labels = parsed.labels ?? {};
      settings.labels_colors = parsed.labels_colors ?? {};
    } catch {
      // keep empty settings
    }
  }

  statusColumnSettingsCache.set(cacheKey, settings);
  return settings;
}

async function resolveVxFromStatusIndex(
  boardId: string,
  columnId: string,
  statusIndex: number
): Promise<VxStatus | null> {
  const settings = await fetchStatusColumnSettings(boardId, columnId);
  const key = String(statusIndex);

  const fromLabel = parseVxFromLabel(settings.labels[key] ?? '');
  if (fromLabel) return fromLabel;

  const colorInfo = settings.labels_colors[key];
  const fromVar = inferVxFromColorVar(colorInfo?.var_name);
  if (fromVar) return fromVar;

  const fromHex = inferVxFromStatusColor(colorInfo?.color);
  if (fromHex) return fromHex;

  return null;
}

/**
 * Resolve V/X from label text, icons, or column color — never from index number alone.
 * Index is only used to look up the label/color configured for that option in column settings.
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
    const fromSettings = await resolveVxFromStatusIndex(boardId, columnId, statusIndex);
    if (fromSettings) return fromSettings;
  }

  return defaultWhenUnset;
}

/** For boolean flags (e.g. supplier indexation): V/check/green = true, X/cross/red = false. */
export async function resolveBooleanFromVxStatus(
  cv: StatusColumnValue | undefined,
  boardId: string,
  columnId: string,
  defaultWhenUnset = false
): Promise<boolean> {
  const vx = await resolveVxStatus(cv, boardId, columnId, defaultWhenUnset ? 'V' : 'X');
  return vx === 'V';
}
