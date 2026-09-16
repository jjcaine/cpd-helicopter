import assert from 'node:assert/strict';
import test from 'node:test';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';

const actionPagePath = fileURLToPath(new URL('../src/pages/action.astro', import.meta.url));

async function readActionPage() {
  return readFile(actionPagePath, 'utf8');
}

test('Take Action directs residents to DPC participation without 311 advice', async () => {
  const page = await readActionPage();
  const text = page.replace(/\s+/g, ' ');

  assert.doesNotMatch(page, /\b311\b/i, '311 advice must not appear on the action page');
  assert.match(text, /Attend (a |your )?District Policing Committee meeting/i);
  assert.match(text, /https:\/\/clecpc\.org\/get-involved\/dpc-meetings\//);
  assert.match(text, /https:\/\/www\.clecpc\.org\/events-calendar/);
  assert.match(text, /class="cta-button"[^>]*>Find your DPC meeting/i);
  assert.match(text, /https:\/\/clevelandnp\.org\/cleveland-cdcs\//);
  assert.match(text, /date, time, location, (and )?route/i);
  assert.match(text, /specific question/i);
  assert.match(text, /flight data (cannot|can['’]t) establish (a )?flight['’]?s purpose/i);
  assert.match(text, /meetings (do not|don['’]t) guarantee an immediate answer/i);
  assert.match(text, /block clubs/i);
  assert.match(text, /neighborhood organizations/i);
  assert.match(text, /elected secretary of Cleveland['’]s Second District Policing Committee/i);
  assert.match(text, /independent/i);
  assert.match(text, /not an official DPC\/City\/CDP project/i);

  const desktopStyles = page.slice(page.indexOf('@media (min-width: 1024px)'));
  assert.match(desktopStyles, /action-card--secondary:nth-child\(3\).*?grid-column:\s*1\s*\/\s*span\s*6/s);
  assert.match(desktopStyles, /action-card--secondary:nth-child\(4\).*?grid-column:\s*7\s*\/\s*span\s*6/s);
});
