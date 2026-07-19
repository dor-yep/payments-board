/**
 * Renders payment statement PDF via HTML + Puppeteer (Hebrew RTL support).
 *
 * Local: uses full `puppeteer` (bundled Chrome).
 * monday code / serverless Linux: uses `@sparticuz/chromium` + `puppeteer-core`
 * because Chrome is not available in the runtime image cache.
 */

import fs from 'fs';
import path from 'path';
import type { Browser } from 'puppeteer-core';
import type { PaymentStatementData } from './paymentStatement.js';
import { buildPaymentStatementHtml, type EmbeddedFonts } from './pdfStatementHtml.js';
import { logger } from '../logger';

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

/** monday code / Cloud Native Buildpacks / Linux containers without system Chrome. */
function shouldUseServerlessChromium(): boolean {
  if (process.env.PUPPETEER_EXECUTABLE_PATH) return false;
  if (process.env.USE_SERVERLESS_CHROMIUM === '1') return true;
  if (process.env.USE_SERVERLESS_CHROMIUM === '0') return false;

  return (
    process.platform === 'linux' &&
    (process.cwd() === '/workspace' ||
      Boolean(process.env.CNB_STACK_ID) ||
      Boolean(process.env.HOME?.includes('/cnb')) ||
      Boolean(process.env.K_SERVICE))
  );
}

async function launchBrowser(): Promise<Browser> {
  const extraArgs = ['--font-render-hinting=none', '--disable-dev-shm-usage'];

  if (shouldUseServerlessChromium()) {
    const chromiumMod = await import('@sparticuz/chromium');
    const chromium = chromiumMod.default;
    const puppeteer = await import('puppeteer-core');
    const executablePath = await chromium.executablePath();
    logger.info('Launching PDF browser with @sparticuz/chromium', { executablePath });
    return puppeteer.default.launch({
      args: [...chromium.args, ...extraArgs],
      executablePath,
      headless: true,
    });
  }

  const puppeteer = await import('puppeteer');
  logger.info('Launching PDF browser with bundled puppeteer Chrome');
  return puppeteer.default.launch({
    headless: true,
    args: ['--no-sandbox', '--disable-setuid-sandbox', ...extraArgs],
  }) as Promise<Browser>;
}

export async function renderPaymentStatementPdf(data: PaymentStatementData): Promise<Buffer> {
  const fonts = loadEmbeddedFonts();
  const html = buildPaymentStatementHtml(data, fonts);

  const browser = await launchBrowser();

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
