/**
 * Generate a test payment statement PDF with Hebrew mock data.
 * Run: npm run test:pdf
 */

import fs from 'fs';
import path from 'path';
import type { PaymentStatementData } from '../src/services/paymentStatement.js';
import { renderPaymentStatementPdf } from '../src/services/pdfStatementRenderer.js';

const mockData: PaymentStatementData = {
  header: {
    contractName: 'משה ואילנה לוי',
    contractNumber: 'CTR-2025-084',
    project: 'מגדלי סביון',
    signingDate: '2025-01-12',
    apartmentDescription: 'מגדלי סביון, דירה 24, קומה 6, בניין B',
    buildingName: 'B',
    totalContractAmount: 3200000,
    baseIndex: 124.5,
    baseIndexPeriod: '12-2024',
    generationDate: '2026-07-08',
  },
  summary: {
    totalContractAmount: 3200000,
    totalPrincipalPaid: 1450000,
    totalIndexationPaid: 24350,
    currentRemainingBalance: 1774350,
  },
  rows: [
    {
      contractualDueDate: '2025-01-12',
      milestoneDescription: 'חתימת חוזה',
      principalIncludingVat: 320000,
      receipts: [
        {
          receiptDate: '2025-01-15',
          receiptAmount: 320000,
          principalPaid: 320000,
          indexationPaid: 0,
          interestPaid: 0,
          indexMonth: 'ינואר 2025',
          indexValue: 124.5,
          indexChangePercent: 0,
          indexationAmount: 0,
          visualStatus: 'paid',
        },
      ],
      currentBalance: 0,
      statusLabel: 'הושלם',
      visualStatus: 'paid',
      statusBadge: 'שולם',
      paymentCategory: 'דירה',
    },
    {
      contractualDueDate: '2025-04-01',
      milestoneDescription: 'גמר יסודות',
      principalIncludingVat: 640000,
      receipts: [
        {
          receiptDate: '2025-04-10',
          receiptAmount: 400000,
          principalPaid: 398000,
          indexationPaid: 2000,
          interestPaid: 0,
          indexMonth: 'אפריל 2025',
          indexValue: 125.75,
          indexChangePercent: 1.0,
          indexationAmount: 2000,
          visualStatus: 'paid',
        },
        {
          receiptDate: '2025-05-20',
          receiptAmount: 245200,
          principalPaid: 242000,
          indexationPaid: 1200,
          interestPaid: 0,
          indexMonth: 'מאי 2025',
          indexValue: 126.1,
          indexChangePercent: 0.28,
          indexationAmount: 1200,
          visualStatus: 'paid',
        },
      ],
      currentBalance: 0,
      statusLabel: 'הושלם',
      visualStatus: 'paid',
      statusBadge: 'שולם',
      paymentCategory: 'דירה',
    },
    {
      contractualDueDate: '2025-06-01',
      milestoneDescription: 'גמר שלד',
      principalIncludingVat: 960000,
      receipts: [
        {
          receiptDate: '2025-06-09',
          receiptAmount: 600000,
          principalPaid: 592000,
          indexationPaid: 8000,
          interestPaid: 0,
          indexMonth: 'יוני 2025',
          indexValue: 126.0,
          indexChangePercent: 1.2,
          indexationAmount: 8000,
          visualStatus: 'paid',
        },
        {
          receiptDate: null,
          receiptAmount: 374440,
          principalPaid: 368000,
          indexationPaid: 6440,
          interestPaid: 0,
          indexMonth: 'אפריל 2026',
          indexValue: 128.2,
          indexChangePercent: 1.75,
          indexationAmount: 6440,
          isRemainder: true,
          visualStatus: 'due',
        },
      ],
      currentBalance: 374440,
      statusLabel: 'חלקי',
      visualStatus: 'due',
      statusBadge: 'פתוח',
      paymentCategory: 'דירה',
    },
    {
      contractualDueDate: '2025-03-01',
      milestoneDescription: 'רישום זכויות',
      principalIncludingVat: 15000,
      receipts: [
        {
          receiptDate: '2025-03-01',
          receiptAmount: 15000,
          principalPaid: 15000,
          indexationPaid: 0,
          interestPaid: 0,
          indexMonth: null,
          indexValue: null,
          indexChangePercent: null,
          indexationAmount: null,
          visualStatus: 'paid',
        },
      ],
      currentBalance: 0,
      statusLabel: 'הושלם',
      visualStatus: 'paid',
      statusBadge: 'רישום זכויות · שולם',
      paymentCategory: 'רישום זכויות',
    },
    {
      contractualDueDate: '2026-12-01',
      milestoneDescription: 'מסירת דירה',
      principalIncludingVat: 1280000,
      receipts: [
        {
          receiptDate: null,
          receiptAmount: null,
          principalPaid: null,
          indexationPaid: null,
          interestPaid: null,
          indexMonth: null,
          indexValue: null,
          indexChangePercent: null,
          indexationAmount: null,
          visualStatus: 'future',
        },
      ],
      currentBalance: 1280000,
      statusLabel: null,
      visualStatus: 'future',
      statusBadge: 'עתידי',
      paymentCategory: 'דירה',
    },
  ],
};

async function main() {
  const outPath = path.join(process.cwd(), 'test-payment-statement.pdf');
  console.log('Generating test PDF with Hebrew mock data...');
  const buffer = await renderPaymentStatementPdf(mockData);
  fs.writeFileSync(outPath, buffer);
  console.log(`Written: ${outPath} (${buffer.length} bytes)`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
