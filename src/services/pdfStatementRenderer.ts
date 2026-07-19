/**
 * Renders payment statement PDF via HTML + Puppeteer (Hebrew RTL support).
 *
 * Local: uses full `puppeteer` (bundled Chrome).
 * monday code / serverless Linux: uses `@sparticuz/chromium` + `puppeteer-core`,
 * and explicitly unpacks the AL2023 shared libraries (libnspr4, etc.) that Monday's
 * runtime does not provide — Sparticuz normally only does this on AWS Lambda.
 */

import fs from 'fs';
import os from 'os';
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

function resolveSparticuzBinDir(): string {
  const candidates = [
    path.join(process.cwd(), 'node_modules/@sparticuz/chromium/bin'),
    // When cwd is nested (tests / scripts), walk up a few levels
    path.join(process.cwd(), '../node_modules/@sparticuz/chromium/bin'),
    path.join(process.cwd(), '../../node_modules/@sparticuz/chromium/bin'),
  ];
  for (const candidate of candidates) {
    if (fs.existsSync(path.join(candidate, 'al2023.tar.br'))) return candidate;
  }
  throw new Error('Could not locate @sparticuz/chromium/bin (al2023.tar.br missing)');
}

/**
 * Monday Code is not AWS Lambda, so @sparticuz/chromium skips unpacking al2023 shared
 * libs → Chromium fails with missing libnspr4.so. Force-extract and wire LD_LIBRARY_PATH.
 */
async function prepareServerlessChromiumLibs(
  inflate: (filePath: string) => Promise<string>,
  setupLambdaEnvironment: (baseLibPath: string) => void
): Promise<string> {
  const binDir = resolveSparticuzBinDir();
  const al2023Archive = path.join(binDir, 'al2023.tar.br');
  if (!fs.existsSync(al2023Archive)) {
    throw new Error(`Missing AL2023 library pack at ${al2023Archive}`);
  }

  const extractedRoot = await inflate(al2023Archive);
  // inflate("al2023.tar.br") → /tmp/al2023 ; libs live under /tmp/al2023/lib
  const libPath = fs.existsSync(path.join(extractedRoot, 'lib'))
    ? path.join(extractedRoot, 'lib')
    : extractedRoot;

  setupLambdaEnvironment(libPath);

  // Also put /tmp on the path — chromium binary and some .so files land there
  const tmp = os.tmpdir();
  const existing = process.env.LD_LIBRARY_PATH ?? '';
  const parts = [libPath, tmp, ...existing.split(':').filter(Boolean)];
  process.env.LD_LIBRARY_PATH = [...new Set(parts)].join(':');
  process.env.FONTCONFIG_PATH ??= path.join(tmp, 'fonts');

  logger.info('Prepared serverless Chromium shared libraries', {
    libPath,
    ldLibraryPath: process.env.LD_LIBRARY_PATH,
  });

  return libPath;
}

async function launchBrowser(): Promise<Browser> {
  const extraArgs = ['--font-render-hinting=none', '--disable-dev-shm-usage'];

  if (shouldUseServerlessChromium()) {
    // Hint Sparticuz toward AL2023 packs if it does any auto-detection later
    process.env.AWS_LAMBDA_JS_RUNTIME ??= `nodejs${process.versions.node.split('.')[0]}.x`;

    const chromiumMod = await import('@sparticuz/chromium');
    const chromium = chromiumMod.default;
    const { inflate, setupLambdaEnvironment } = chromiumMod;
    const puppeteer = await import('puppeteer-core');

    await prepareServerlessChromiumLibs(inflate, setupLambdaEnvironment);

    // Prefer headless shell without extra GPU/SwiftShader needs
    try {
      // Public setter on the Chromium class
      (chromium as unknown as { setGraphicsMode: boolean }).setGraphicsMode = false;
    } catch {
      // ignore if unavailable
    }

    const executablePath = await chromium.executablePath();
    logger.info('Launching PDF browser with @sparticuz/chromium', {
      executablePath,
      ldLibraryPath: process.env.LD_LIBRARY_PATH,
    });

    return puppeteer.default.launch({
      args: [...chromium.args, ...extraArgs],
      executablePath,
      headless: true,
      env: {
        ...process.env,
        LD_LIBRARY_PATH: process.env.LD_LIBRARY_PATH,
        FONTCONFIG_PATH: process.env.FONTCONFIG_PATH,
      },
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
