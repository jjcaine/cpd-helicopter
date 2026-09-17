import assert from 'node:assert/strict';
import { execFileSync, spawn } from 'node:child_process';
import { once } from 'node:events';
import { readFileSync } from 'node:fs';
import { createServer } from 'node:net';
import { join } from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

const siteDir = fileURLToPath(new URL('..', import.meta.url));

async function getOpenPort() {
  return await new Promise((resolve, reject) => {
    const server = createServer();
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const { port } = server.address();
      server.close((error) => (error ? reject(error) : resolve(port)));
    });
  });
}

async function waitForServer(url) {
  for (let attempt = 0; attempt < 50; attempt += 1) {
    try {
      if ((await fetch(url)).ok) return;
    } catch {
      // The static server is still starting.
    }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error(`Static site did not start at ${url}`);
}

async function within(promise, milliseconds, label) {
  let timeout;
  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => {
        timeout = setTimeout(() => reject(new Error(`${label} timed out after ${milliseconds}ms`)), milliseconds);
      }),
    ]);
  } finally {
    clearTimeout(timeout);
  }
}

async function stopServer(server) {
  if (!server || server.exitCode !== null) return;

  server.kill();
  try {
    await within(once(server, 'exit'), 5_000, 'static server shutdown');
  } catch {
    server.kill('SIGKILL');
    await within(once(server, 'exit'), 5_000, 'forced static server shutdown').catch(() => {});
  }
}

test('mobile cold load retains chart fallbacks and renders scrollable charts after JavaScript arrives', { timeout: 60_000 }, async () => {
  execFileSync(process.execPath, ['./node_modules/astro/bin/astro.mjs', 'build'], {
    cwd: siteDir,
    stdio: 'inherit',
  });
  const html = readFileSync(join(siteDir, 'dist/index.html'), 'utf8');
  const initialEntry = html.match(/<script\b[^>]*\btype="module"[^>]*\bsrc="([^"]+)"[^>]*>/)?.[1];
  assert.ok(initialEntry, 'the built page needs an initial chart module');

  const port = await getOpenPort();
  const baseUrl = `http://127.0.0.1:${port}`;
  const server = spawn('python3', ['-m', 'http.server', String(port), '--directory', 'dist'], {
    cwd: siteDir,
    stdio: 'ignore',
  });
  let browser;
  let context;
  let released = false;
  let releaseModule;
  const delayed = new Promise((resolve) => { releaseModule = resolve; });
  const releaseInitialChartModule = () => {
    if (!released) {
      released = true;
      releaseModule();
    }
  };

  try {
    await waitForServer(baseUrl);

    browser = await chromium.launch({
      headless: true,
      executablePath: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH || undefined,
    });
    context = await browser.newContext({
      viewport: { width: 390, height: 844 },
      isMobile: true,
      hasTouch: true,
    });
    const page = await context.newPage();

    await page.route(new URL(initialEntry, baseUrl).href, async (route) => {
      await delayed;
      await route.continue();
    });

    await page.goto(`${baseUrl}/`, { waitUntil: 'commit', timeout: 10_000 });
    await page.locator('#calendar-chart [data-chart-mark="calendar-day"]').first().waitFor({ state: 'visible', timeout: 10_000 });
    await page.locator('#histogram-chart [data-chart-mark="hour-bar"]').first().waitFor({ state: 'visible', timeout: 10_000 });

    releaseInitialChartModule();
    await page.waitForLoadState('domcontentloaded', { timeout: 10_000 });
    await page.locator('#calendar-chart svg').waitFor({ state: 'visible', timeout: 10_000 });
    await page.locator('#histogram-chart svg').waitFor({ state: 'visible', timeout: 10_000 });

    const calendarMetrics = await page.locator('.calendar-chart-viewport').evaluate((viewport) => {
      const svg = viewport.querySelector('svg');
      return {
        clientWidth: viewport.clientWidth,
        scrollWidth: viewport.scrollWidth,
        svgWidth: svg?.getBoundingClientRect().width ?? 0,
        pageOverflow: document.documentElement.scrollWidth > document.documentElement.clientWidth,
      };
    });
    assert.ok(
      calendarMetrics.scrollWidth > calendarMetrics.clientWidth,
      `calendar should scroll horizontally on mobile: ${JSON.stringify(calendarMetrics)}`,
    );
    assert.ok(
      calendarMetrics.svgWidth > calendarMetrics.clientWidth,
      `calendar cells should retain their readable width: ${JSON.stringify(calendarMetrics)}`,
    );
    assert.equal(calendarMetrics.pageOverflow, false, 'calendar scrolling must not overflow the page');
  } finally {
    releaseInitialChartModule();
    await within(context?.close() ?? Promise.resolve(), 5_000, 'browser context shutdown').catch(() => {});
    await within(browser?.close() ?? Promise.resolve(), 5_000, 'browser shutdown').catch(() => {});
    await stopServer(server);
  }
});

test('a failed calendar module preserves its fallback while the histogram and map initialize', { timeout: 60_000 }, async () => {
  execFileSync(process.execPath, ['./node_modules/astro/bin/astro.mjs', 'build'], {
    cwd: siteDir,
    stdio: 'inherit',
  });

  const port = await getOpenPort();
  const baseUrl = `http://127.0.0.1:${port}`;
  const server = spawn('python3', ['-m', 'http.server', String(port), '--directory', 'dist'], {
    cwd: siteDir,
    stdio: 'ignore',
  });
  let browser;
  let context;

  try {
    await waitForServer(baseUrl);
    browser = await chromium.launch({
      headless: true,
      executablePath: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH || undefined,
    });
    context = await browser.newContext({ viewport: { width: 390, height: 844 }, isMobile: true, hasTouch: true });
    const page = await context.newPage();

    await page.route(/\/calendar-chart\.[^/]+\.js$/, (route) => route.abort('failed'));
    await page.goto(`${baseUrl}/`, { waitUntil: 'domcontentloaded', timeout: 10_000 });

    await page.locator('#calendar-chart [data-chart-mark="calendar-day"]').first().waitFor({ state: 'visible', timeout: 10_000 });
    await page.locator('#calendar-chart [role="alert"]').waitFor({ state: 'visible', timeout: 10_000 });
    await page.locator('#histogram-chart svg').waitFor({ state: 'visible', timeout: 10_000 });
    await page.waitForFunction(
      () => document.querySelector('#flight-map [role="status"]') === null,
      undefined,
      { timeout: 10_000 },
    );
  } finally {
    await within(context?.close() ?? Promise.resolve(), 5_000, 'browser context shutdown').catch(() => {});
    await within(browser?.close() ?? Promise.resolve(), 5_000, 'browser shutdown').catch(() => {});
    await stopServer(server);
  }
});
