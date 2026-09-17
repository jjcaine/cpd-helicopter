import assert from 'node:assert/strict';
import test from 'node:test';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';

const homepagePath = fileURLToPath(new URL('../src/pages/index.astro', import.meta.url));
const researchPagePath = fileURLToPath(new URL('../src/pages/research.astro', import.meta.url));

async function readPage(pagePath) {
  return readFile(pagePath, 'utf8');
}

async function readHomepage() {
  return readPage(homepagePath);
}

async function readResearchPage() {
  return readPage(researchPagePath);
}

test('homepage action CTA prioritizes a DPC meeting without 311 advice', async () => {
  const page = await readHomepage();
  const text = page.replace(/\s+/g, ' ');

  assert.doesNotMatch(page, /\b311\b/i, '311 advice must not appear on the homepage');
  assert.match(text, /Find (and )?attend your District Policing Committee meeting/i);
  assert.match(text, /date, time, location, (and )?route you observed/i);
  assert.match(text, /specific question/i);
  assert.match(text, /href="\/action\/"[^>]*class="cta-button"/i);
});

test('research page action CTA prioritizes a DPC meeting without 311 advice', async () => {
  const page = await readResearchPage();
  const text = page.replace(/\s+/g, ' ');

  assert.doesNotMatch(text, /Call 311/i, '311 advice must not appear on the research page');
  assert.match(text, /Find (and )?attend your District Policing Committee meeting/i);
  assert.match(text, /date, time, location, (and )?route you observed/i);
  assert.match(text, /specific question/i);
  assert.match(text, /href="\/action\/"[^>]*class="cta-button"/i);
});
