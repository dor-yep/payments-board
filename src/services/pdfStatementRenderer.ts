/**
 * Renders payment statement PDF via HTML + Puppeteer (Hebrew RTL support).
 */

import fs from 'fs';
import path from 'path';
import puppeteer from 'puppeteer';
import type { PaymentStatementData } from './paymentStatement.js';
import { buildPaymentStatementHtml, type EmbeddedFonts } from './pdfStatementHtml.js';

let fontsCache: EmbeddedFonts | null = null;

function loadEmbeddedFonts(): EmbeddedFonts {
  if (fontsCache) return fontsCache;

  const regularPath = path.join(process.cwd(), 'assets/fonts/NotoSansHebrew-Regular.ttf');
  const boldPath = path.join(process.cwd(), 'assets/fonts/NotoSansHebrew-Bold.ttf');

  if (!fs.existsSync(regularPath) || !fs.existsSync(boldPath)) {
    throw new Error(
      'Hebrew fonts not found. Expected assets/fonts/NotoSansHebrew-Regular.ttf and NotoSansHebrew-Bold.ttf'
    );
  }

  fontsCache = {
    regularBase64: fs.readFileSync(regularPath).toString('base64'),
    boldBase64: fs.readFileSync(boldPath).toString('base64'),
  };

  return fontsCache;
}

export async function renderPaymentStatementPdf(data: PaymentStatementData): Promise<Buffer> {
  const fonts = loadEmbeddedFonts();
  const html = buildPaymentStatementHtml(data, fonts);

  const browser = await puppeteer.launch({
    headless: true,
    args: ['--no-sandbox', '--disable-setuid-sandbox', '--font-render-hinting=none'],
  });

  try {
    const page = await browser.newPage();
    await page.setContent(html, { waitUntil: 'load' });

    await page.evaluate(async () => {
      await document.fonts.ready;
    });

    const fontsLoaded = await page.evaluate(() => {
      const families = ['Noto Sans Hebrew'];
      return families.every((family) =>
        [...document.fonts].some((f) => f.family.includes(family) && f.status === 'loaded')
      );
    });

    if (!fontsLoaded) {
      await page.waitForFunction(
        () => [...document.fonts].every((f) => f.status === 'loaded'),
        { timeout: 10000 }
      );
    }

    const pdfBuffer = await page.pdf({
      format: 'A4',
      landscape: true,
      printBackground: true,
      preferCSSPageSize: true,
      margin: {
        top: '10mm',
        right: '12mm',
        bottom: '12mm',
        left: '12mm',
      },
    });

    return Buffer.from(pdfBuffer);
  } finally {
    await browser.close();
  }
}
