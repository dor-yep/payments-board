/** Shared formatters and value normalization for PDF HTML rendering. */

const HEBREW_MONTHS = [
  'ינואר', 'פברואר', 'מרץ', 'אפריל', 'מאי', 'יוני',
  'יולי', 'אוגוסט', 'ספטמבר', 'אוקטובר', 'נובמבר', 'דצמבר',
];

export function escapeHtml(value: string): string {
  return value
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

export function normalizeDisplayText(value: unknown): string {
  if (value == null) return '--';
  if (typeof value === 'number') {
    return Number.isFinite(value) ? String(value) : '--';
  }
  if (typeof value === 'boolean') return value ? 'כן' : 'לא';
  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (!trimmed || trimmed === 'undefined' || trimmed === 'null' || trimmed === 'NaN') return '--';
    if (trimmed.startsWith('{') || trimmed.startsWith('[')) {
      try {
        const parsed = JSON.parse(trimmed) as Record<string, unknown>;
        const inner =
          parsed.text ??
          parsed.label ??
          parsed.name ??
          parsed.display_value ??
          parsed.value;
        if (inner != null && typeof inner !== 'object') {
          return normalizeDisplayText(inner);
        }
        return '--';
      } catch {
        return trimmed;
      }
    }
    return trimmed;
  }
  if (typeof value === 'object') {
    const obj = value as Record<string, unknown>;
    const inner = obj.text ?? obj.label ?? obj.name ?? obj.display_value ?? obj.value;
    if (inner != null) return normalizeDisplayText(inner);
    return '--';
  }
  return '--';
}

export function formatDateHe(dateStr: string | null | undefined): string {
  if (!dateStr?.trim()) return '--';
  const d = new Date(dateStr);
  if (isNaN(d.getTime())) {
    if (/^\d{2}\/\d{2}\/\d{4}$/.test(dateStr.trim())) return dateStr.trim();
    return '--';
  }
  const dd = String(d.getDate()).padStart(2, '0');
  const mm = String(d.getMonth() + 1).padStart(2, '0');
  const yyyy = d.getFullYear();
  return `${dd}/${mm}/${yyyy}`;
}

export function formatCurrency(amount: number | null | undefined): string {
  if (amount == null || !Number.isFinite(amount)) return '--';
  const negative = amount < 0;
  const formatted = Math.abs(amount).toLocaleString('en-US', {
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  });
  return `${negative ? '-' : ''}₪${formatted}`;
}

export function formatPercent(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return '--';
  if (value < 0) return `-${Math.abs(value).toFixed(2)}%`;
  if (value > 0) return `+${value.toFixed(2)}%`;
  return `${value.toFixed(2)}%`;
}

export function formatNumber(value: number | null | undefined, decimals = 2): string {
  if (value == null || !Number.isFinite(value)) return '--';
  if (value < 0) return `-${Math.abs(value).toFixed(decimals)}`;
  return value.toFixed(decimals);
}

export function formatIndexPeriod(period: string | null | undefined): string {
  if (!period?.trim()) return '--';
  const match = period.trim().match(/^(\d{1,2})-(\d{4})$/);
  if (!match) return normalizeDisplayText(period);
  const monthIndex = parseInt(match[1], 10) - 1;
  if (monthIndex < 0 || monthIndex > 11) return period;
  return `${HEBREW_MONTHS[monthIndex]} ${match[2]}`;
}

/** Wrap numeric / currency / date values for correct LTR display inside RTL layout. */
export function ltr(value: string, className = 'num'): string {
  if (value === '--') return `<span class="${className}">--</span>`;
  return `<span class="${className}" dir="ltr">${escapeHtml(value)}</span>`;
}

/**
 * Renders a Hebrew date with the day/month on the first line and the full year
 * on a second line, so the year is never truncated in narrow table columns.
 */
export function ltrDateStacked(dateStr: string | null | undefined): string {
  const formatted = formatDateHe(dateStr);
  if (formatted === '--') return `<span class="num">--</span>`;
  const match = formatted.match(/^(\d{2}\/\d{2})\/(\d{4})$/);
  if (!match) return `<span class="num" dir="ltr">${escapeHtml(formatted)}</span>`;
  const [, dayMonth, year] = match;
  return `<span class="num date-stacked" dir="ltr"><span class="date-dm">${escapeHtml(dayMonth)}</span><span class="date-year">${escapeHtml(year)}</span></span>`;
}

export function text(value: unknown): string {
  return escapeHtml(normalizeDisplayText(value));
}
