/**
 * Builds RTL Hebrew HTML for the payment statement PDF.
 */

import type { PaymentStatementData, PaymentStatementRow, PaymentVisualStatus } from './paymentStatement.js';
import {
  escapeHtml,
  formatCurrency,
  formatDateHe,
  formatIndexPeriod,
  formatNumber,
  formatPercent,
  ltr,
  ltrDateStacked,
  normalizeDisplayText,
  text,
} from './pdfStatementFormatters.js';

export interface EmbeddedFonts {
  regularBase64: string;
  boldBase64: string;
}

function statusClass(status: PaymentVisualStatus, paymentCategory?: string): string {
  if (paymentCategory === 'רישום זכויות') return 'badge-registration';
  if (status === 'paid') return 'badge-paid';
  if (status === 'due') return 'badge-due';
  return 'badge-future';
}

function renderStatusBadges(row: PaymentStatementRow): string {
  if (row.paymentCategory === 'רישום זכויות' && row.visualStatus === 'paid') {
    return `
      <span class="badge badge-registration">${text('רישום זכויות')}</span>
      <span class="badge badge-paid">${text('שולם')}</span>
    `;
  }
  return `<span class="badge ${statusClass(row.visualStatus, row.paymentCategory)}">${text(row.statusBadge)}</span>`;
}

function amountClass(status: PaymentVisualStatus): string {
  if (status === 'paid') return 'amount-paid';
  if (status === 'due') return 'amount-due';
  return 'amount-default';
}

function receiptRows(row: PaymentStatementRow): PaymentStatementRow['receipts'] {
  const withData = row.receipts.filter(
    (r) =>
      r.isRemainder ||
      r.receiptDate != null ||
      r.receiptAmount != null ||
      r.principalPaid != null ||
      r.indexationPaid != null ||
      r.interestPaid != null
  );
  return withData.length > 0 ? withData : [row.receipts[0] ?? {
    receiptDate: null,
    receiptAmount: null,
    principalPaid: null,
    indexationPaid: null,
    interestPaid: null,
  }];
}

function receiptVisualStatus(
  receipt: PaymentStatementRow['receipts'][number],
  rowStatus: PaymentVisualStatus
): PaymentVisualStatus {
  if (receipt.visualStatus) return receipt.visualStatus;
  if (receipt.isRemainder) return rowStatus;
  const looksLikeActualReceipt =
    receipt.receiptDate != null ||
    receipt.receiptAmount != null ||
    receipt.principalPaid != null;
  return looksLikeActualReceipt ? 'paid' : rowStatus;
}

function renderIndexCells(receipt: PaymentStatementRow['receipts'][number]): string {
  // Index columns stay default color (never red), even on יתרה / due rows
  return `
    <td class="cell-index">${text(receipt.indexMonth ?? '--')}</td>
    <td class="cell-center cell-index">${ltr(formatNumber(receipt.indexValue ?? null))}</td>
    <td class="cell-center cell-index">${ltr(formatPercent(receipt.indexChangePercent ?? null))}</td>
    <td class="cell-num cell-index">${ltr(formatCurrency(receipt.indexationAmount ?? null))}</td>
  `;
}

function renderReceiptCells(
  receipt: PaymentStatementRow['receipts'][number],
  rowStatus: PaymentVisualStatus
): string {
  const status = receiptVisualStatus(receipt, rowStatus);
  const valueClass =
    status === 'paid'
      ? 'amount-paid'
      : status === 'due'
        ? 'amount-due'
        : 'amount-default';
  const dateCell = receipt.isRemainder
    ? `<td class="${valueClass} cell-remainder">${text('יתרה')}</td>`
    : `<td class="${valueClass}">${ltr(formatDateHe(receipt.receiptDate))}</td>`;
  return `
    ${renderIndexCells(receipt)}
    ${dateCell}
    <td class="${valueClass}">${ltr(formatCurrency(receipt.receiptAmount))}</td>
    <td class="${valueClass}">${ltr(formatCurrency(receipt.principalPaid))}</td>
    <td class="${valueClass}">${ltr(formatCurrency(receipt.indexationPaid))}</td>
    <td class="${valueClass}">${ltr(formatCurrency(receipt.interestPaid))}</td>
  `;
}

function renderPaymentRows(rows: PaymentStatementRow[]): string {
  return rows
    .map((row, groupIndex) => {
      const receipts = receiptRows(row);
      const rowSpan = receipts.length;
      const hasPaidReceipts = receipts.some((r) => !r.isRemainder);
      const alt = groupIndex % 2 === 1 ? 'row-alt' : '';
      // Pure unpaid next payment: paint non-identity data red.
      // Partial (paid receipts + יתרה): only the remainder receipt amounts are red.
      const fullRowDue = row.visualStatus === 'due' && !hasPaidReceipts;
      const due = fullRowDue ? 'row-due' : '';
      const principalClass = fullRowDue ? 'amount-due' : '';
      const mergedStart = `
        <td rowspan="${rowSpan}" class="cell-center cell-identity">${ltrDateStacked(row.contractualDueDate)}</td>
        <td rowspan="${rowSpan}" class="cell-wrap cell-identity">${text(row.milestoneDescription)}</td>
        <td rowspan="${rowSpan}" class="cell-num ${principalClass}">${ltr(formatCurrency(row.principalIncludingVat))}</td>
      `;
      const mergedEnd = `
        <td rowspan="${rowSpan}" class="cell-num ${amountClass(row.visualStatus)}">${ltr(formatCurrency(row.currentBalance))}</td>
        <td rowspan="${rowSpan}" class="cell-center cell-status">
          ${renderStatusBadges(row)}
        </td>
      `;

      const first = receipts[0];
      return `
        <tr class="${alt} ${due}">
          ${mergedStart}
          ${renderReceiptCells(first, row.visualStatus)}
          ${mergedEnd}
        </tr>
        ${receipts
          .slice(1)
          .map(
            (receipt) => `
        <tr class="${alt} ${due} ${receipt.isRemainder && row.visualStatus === 'due' ? 'row-remainder-due' : ''}">
          ${renderReceiptCells(receipt, row.visualStatus)}
        </tr>`
          )
          .join('')}
      `;
    })
    .join('');
}

function renderSummaryCards(data: PaymentStatementData): string {
  const cards = [
    {
      title: 'מחיר דירה מקורי בחוזה',
      value: formatCurrency(data.summary.totalContractAmount),
      className: 'card-value-dark',
    },
    {
      title: 'סך תקבולים (על חשבון קרן)',
      value: formatCurrency(data.summary.totalPrincipalPaid),
      className: 'card-value-green',
    },
    {
      title: 'סך הפרשי מדד שנפרעו/נצברו',
      value: formatCurrency(data.summary.totalIndexationPaid),
      className: 'card-value-dark',
    },
    {
      title: 'יתרה סופית לתשלום (כולל מדד נוכחי)',
      value: formatCurrency(data.summary.currentRemainingBalance),
      className: 'card-value-red',
    },
  ];

  return cards
    .map(
      (card) => `
    <div class="summary-card">
      <div class="summary-card-title">${escapeHtml(card.title)}</div>
      <div class="summary-card-value ${card.className}">${ltr(card.value, 'num card-num')}</div>
    </div>`
    )
    .join('');
}

export function buildPaymentStatementHtml(
  data: PaymentStatementData,
  fonts: EmbeddedFonts
): string {
  const baseIndexText =
    data.header.baseIndex != null
      ? `${formatNumber(data.header.baseIndex)} (${formatIndexPeriod(data.header.baseIndexPeriod)})`
      : '--';

  return `<!DOCTYPE html>
<html lang="he" dir="rtl">
<head>
  <meta charset="UTF-8" />
  <style>
    @font-face {
      font-family: 'Noto Sans Hebrew';
      font-style: normal;
      font-weight: 400;
      src: url(data:font/truetype;charset=utf-8;base64,${fonts.regularBase64}) format('truetype');
    }
    @font-face {
      font-family: 'Noto Sans Hebrew';
      font-style: normal;
      font-weight: 700;
      src: url(data:font/truetype;charset=utf-8;base64,${fonts.boldBase64}) format('truetype');
    }

    @page {
      size: A4 landscape;
      margin: 10mm 12mm 12mm 12mm;
    }

    *, *::before, *::after { box-sizing: border-box; }

    html, body {
      margin: 0;
      padding: 0;
      direction: rtl;
      font-family: 'Noto Sans Hebrew', sans-serif;
      font-size: 8.5pt;
      line-height: 1.35;
      color: #2d3748;
      background: #fff;
    }

    .page {
      width: 100%;
      max-width: 100%;
      overflow: hidden;
    }

    .header {
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      gap: 16px;
      margin-bottom: 10px;
    }

    .brand-title {
      font-size: 14pt;
      font-weight: 700;
      color: #1a365d;
      margin: 0 0 4px;
      text-align: left;
    }

    .brand-subtitle {
      font-size: 8pt;
      color: #718096;
      margin: 0;
      text-align: left;
    }

    .doc-title {
      font-size: 15pt;
      font-weight: 700;
      color: #1a365d;
      margin: 0 0 4px;
      text-align: right;
    }

    .doc-meta {
      font-size: 7.5pt;
      color: #718096;
      margin: 0;
      text-align: right;
    }

    .info-box {
      border: 1px solid #cbd5e0;
      border-radius: 6px;
      padding: 10px 12px;
      margin-bottom: 10px;
      display: grid;
      grid-template-columns: 1fr 1fr 1fr;
      gap: 8px 16px;
    }

    .info-item {
      font-size: 8pt;
      word-break: break-word;
    }

    .info-label {
      font-weight: 700;
      color: #2d3748;
    }

    .summary-cards {
      display: grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 8px;
      margin-bottom: 10px;
    }

    .summary-card {
      border: 1px solid #cbd5e0;
      border-radius: 6px;
      padding: 8px 6px;
      text-align: center;
      min-height: 52px;
    }

    .summary-card-title {
      font-size: 7pt;
      color: #718096;
      margin-bottom: 6px;
      line-height: 1.25;
    }

    .summary-card-value {
      font-size: 11pt;
      font-weight: 700;
      line-height: 1.2;
    }

    .card-value-dark { color: #1a202c; }
    .card-value-green { color: #276749; }
    .card-value-red { color: #c53030; }

    .table-wrap {
      width: 100%;
      overflow: hidden;
    }

    table {
      width: 100%;
      table-layout: fixed;
      border-collapse: collapse;
      font-size: 7pt;
    }

    thead { display: table-header-group; }

    .group-header th {
      background: #1a365d;
      color: #fff;
      font-weight: 700;
      font-size: 7pt;
      padding: 4px 3px;
      border: 1px solid #2c5282;
      text-align: center;
    }

    .col-header th {
      background: #1a365d;
      color: #fff;
      font-weight: 700;
      font-size: 6.5pt;
      padding: 5px 3px;
      border: 1px solid #2c5282;
      text-align: center;
      vertical-align: middle;
      word-break: keep-all;
      overflow-wrap: normal;
      line-height: 1.2;
    }

    tbody td {
      border: 1px solid #e2e8f0;
      padding: 4px 3px;
      vertical-align: middle;
      word-wrap: break-word;
      overflow-wrap: anywhere;
      line-height: 1.25;
    }

    tbody tr.row-alt td { background: #f7fafc; }

    tbody tr.row-due td:not(.cell-identity):not(.cell-index) {
      color: #c53030;
      font-weight: 700;
    }

    tbody tr.row-due td:not(.cell-identity):not(.cell-index) .num {
      color: #c53030;
      font-weight: 700;
    }

    .cell-center { text-align: center; }
    .cell-num { text-align: center; }
    .cell-wrap { text-align: right; }
    .cell-identity { color: #2d3748; font-weight: 400; }
    .cell-index { color: #2d3748; font-weight: 400; }

    .num, .card-num {
      unicode-bidi: isolate;
      direction: ltr;
      display: inline-block;
      max-width: 100%;
    }

    .date-stacked {
      line-height: 1.15;
      white-space: nowrap;
    }
    .date-stacked .date-dm,
    .date-stacked .date-year {
      display: block;
      text-align: center;
      white-space: nowrap;
    }

    .amount-paid { color: #276749; }
    .amount-due { color: #c53030; font-weight: 700; }
    .amount-default { color: #2d3748; }
    .cell-remainder { font-weight: 700; }

    .badge {
      display: inline-block;
      padding: 2px 6px;
      border-radius: 4px;
      font-size: 6.5pt;
      font-weight: 700;
      white-space: nowrap;
      margin: 1px;
    }

    .cell-status {
      line-height: 1.5;
    }

    .badge-paid { background: #c6f6d5; color: #276749; }
    .badge-due { background: #fed7d7; color: #c53030; }
    .badge-future { background: #e2e8f0; color: #1a202c; }
    .badge-registration { background: #e9d8fd; color: #553c9a; }

    .notes {
      border: 1px solid #cbd5e0;
      border-radius: 6px;
      padding: 10px 12px;
      margin-top: 12px;
      page-break-inside: avoid;
    }

    .notes-title {
      color: #c05621;
      font-weight: 700;
      font-size: 8pt;
      margin: 0 0 6px;
    }

    .notes ul {
      margin: 0;
      padding: 0 16px 0 0;
      color: #718096;
      font-size: 7pt;
    }

    .notes li { margin-bottom: 3px; }

    .page-footer {
      margin-top: 8px;
      font-size: 7pt;
      color: #718096;
      text-align: left;
    }
  </style>
</head>
<body>
  <div class="page">
    <header class="header">
      <div>
        <h2 class="doc-title">${text('מצב חשבון דייר מפורט ומדדים')}</h2>
        <p class="doc-meta">${text(`תאריך הפקה: ${formatDateHe(data.header.generationDate)} | בסיס הצמדה: מדד תשומות הבנייה`)}</p>
      </div>
      <div>
        <h1 class="brand-title">${text('שדות גיטלמן בע"מ')}</h1>
        <p class="brand-subtitle">${text('כרטיס פיננסי מורחב הכולל מעקב מדדים מפורט')}</p>
      </div>
    </header>

    <section class="info-box">
      <div class="info-item"><span class="info-label">שם הדייר/רוכש:</span> ${text(data.header.contractName)}</div>
      <div class="info-item"><span class="info-label">תאריך חתימת חוזה:</span> ${ltr(formatDateHe(data.header.signingDate))}</div>
      <div class="info-item"><span class="info-label">מספר חוזה:</span> ${text(data.header.contractNumber)}</div>
      <div class="info-item"><span class="info-label">פרויקט ונכס:</span> ${text(data.header.apartmentDescription)}</div>
      <div class="info-item"><span class="info-label">מדד בסיס בחוזה:</span> ${ltr(baseIndexText)}</div>
      <div class="info-item"><span class="info-label">הצמדה לחוק המכר:</span> ${text('על פי חוק (50% מרכיב הבנייה)')}</div>
    </section>

    <section class="summary-cards">
      ${renderSummaryCards(data)}
    </section>

    <div class="table-wrap">
      <table>
        <colgroup>
          <col style="width:5%" />
          <col style="width:11%" />
          <col style="width:8%" />
          <col style="width:7%" />
          <col style="width:5%" />
          <col style="width:5%" />
          <col style="width:7%" />
          <col style="width:7%" />
          <col style="width:7%" />
          <col style="width:7%" />
          <col style="width:6%" />
          <col style="width:6%" />
          <col style="width:8%" />
          <col style="width:9%" />
        </colgroup>
        <thead>
          <tr class="group-header">
            <th colspan="3">פרטי השלב והחוזה המקורי</th>
            <th colspan="4">נתוני מדד תשומות הבנייה</th>
            <th colspan="5">ביצוע ותקבולים בפועל</th>
            <th colspan="2">יתרה</th>
          </tr>
          <tr class="col-header">
            <th>תאריך חוזי</th>
            <th>תיאור אבן דרך</th>
            <th>סכום מקורי (קרן)</th>
            <th>חודש מדד</th>
            <th>ערך מדד</th>
            <th>אחוז שינוי %</th>
            <th>סכום הצמדה</th>
            <th>תאריך תשלום</th>
            <th>סכום תקבול</th>
            <th>קרן</th>
            <th>הצמדה</th>
            <th>ריבית</th>
            <th>יתרה לתשלום</th>
            <th>סטטוס</th>
          </tr>
        </thead>
        <tbody>
          ${renderPaymentRows(data.rows)}
        </tbody>
      </table>
    </div>

    <section class="notes">
      <h3 class="notes-title">${text('מתודולוגיית חישוב והצמדה למדד (חוק המכר החדש)')}</h3>
      <ul>
        <li>${text('כל הסכומים הכספיים בדוח כוללים מע״מ.')}</li>
        <li>${text('מדד הבסיס נקבע לפי המדד הידוע ביום חתימת החוזה.')}</li>
        <li>${text('הפרשי המדד מחושבים כהפרש בין המדד הידוע ביום התשלום לבין מדד הבסיס.')}</li>
        <li>${text('חישוב ההצמדה מבוצע על 50% ממרכיב הבנייה בהתאם לחוק המכר.')}</li>
        <li>${text('ריבית פיגורים מחושבת על בסיס יתרת הקרן, שיעור הריבית בחוזה וימי האיחור.')}</li>
      </ul>
    </section>

    <div class="page-footer">${text('עמוד 1')}</div>
  </div>
</body>
</html>`;
}

export { normalizeDisplayText };
