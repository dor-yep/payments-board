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
      milestoneNumber: 1,
      milestoneDescription: 'חתימת חוזה',
      principalIncludingVat: 320000,
      indexMonth: 'ינואר 2025',
      indexValue: 124.5,
      indexChangePercent: 0,
      indexationAmount: 0,
      receipts: [
        {
          receiptDate: '2025-01-15',
          receiptAmount: 320000,
          principalPaid: 320000,
          indexationPaid: 0,
          interestPaid: 0,
        },
      ],
      currentBalance: 0,
      statusLabel: 'הושלם',
      visualStatus: 'paid',
      statusBadge: 'שולם',
      paymentCategory: 'דירה',
    },
    {
      milestoneNumber: 2,
      milestoneDescription: 'גמר יסודות',
      principalIncludingVat: 640000,
      indexMonth: 'אפריל 2025',
      indexValue: 125.75,
      indexChangePercent: 1.0,
      indexationAmount: 3200,
      receipts: [
        {
          receiptDate: '2025-04-10',
          receiptAmount: 400000,
          principalPaid: 398000,
          indexationPaid: 2000,
          interestPaid: 0,
        },
        {
          receiptDate: '2025-05-20',
          receiptAmount: 245200,
          principalPaid: 242000,
          indexationPaid: 1200,
          interestPaid: 0,
        },
      ],
      currentBalance: 0,
      statusLabel: 'הושלם',
      visualStatus: 'paid',
      statusBadge: 'שולם',
      paymentCategory: 'דירה',
    },
    {
      milestoneNumber: 3,
      milestoneDescription: 'גמר שלד',
      principalIncludingVat: 960000,
      indexMonth: 'יולי 2026',
      indexValue: 128.2,
      indexChangePercent: 2.97,
      indexationAmount: 14280,
      receipts: [],
      currentBalance: 974280,
      statusLabel: 'חלקי',
      visualStatus: 'due',
      statusBadge: 'פתוח',
      paymentCategory: 'דירה',
    },
    {
      milestoneNumber: 4,
      milestoneDescription: 'רישום זכויות',
      principalIncludingVat: 15000,
      indexMonth: null,
      indexValue: null,
      indexChangePercent: null,
      indexationAmount: null,
      receipts: [],
      currentBalance: 15000,
      statusLabel: null,
      visualStatus: 'future',
      statusBadge: 'רישום זכויות',
      paymentCategory: 'רישום זכויות',
    },
    {
      milestoneNumber: 5,
      milestoneDescription: 'מסירת דירה',
      principalIncludingVat: 1280000,
      indexMonth: null,
      indexValue: null,
      indexChangePercent: null,
      indexationAmount: null,
      receipts: [],
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
