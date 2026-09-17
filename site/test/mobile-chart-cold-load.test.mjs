import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const siteDir = fileURLToPath(new URL('..', import.meta.url));

test('mobile cold loads ship data-derived fallbacks in a scrollable readable calendar', () => {
  execFileSync(process.execPath, ['./node_modules/astro/bin/astro.mjs', 'build'], {
    cwd: siteDir,
    stdio: 'inherit',
  });

  const html = readFileSync(join(siteDir, 'dist/index.html'), 'utf8');
  assert.match(
    html,
    /id="calendar-chart"[^>]*>[\s\S]*?data-chart-mark="calendar-day"/i,
    'the calendar must ship useful data-derived marks before its delayed JavaScript runs',
  );
  assert.match(
    html,
    /id="histogram-chart"[^>]*>[\s\S]*?data-chart-mark="hour-bar"/i,
    'the histogram must ship useful data-derived marks before its delayed JavaScript runs',
  );
  assert.match(
    html,
    /class="[^"]*calendar-chart-viewport[^"]*"[^>]*role="region"[^>]*aria-label="Helicopter activity calendar; scroll horizontally to see all dates"/i,
    'the calendar needs an accessible horizontal scroll region on mobile',
  );

  const pageStyles = readFileSync(join(siteDir, 'src/pages/index.astro'), 'utf8');
  assert.match(pageStyles, /\.calendar-chart-viewport\s*\{[\s\S]*?overflow-x:\s*auto/, 'calendar viewport should scroll rather than shrink');
  assert.match(pageStyles, /\.calendar-chart-viewport\s+svg\s*\{[\s\S]*?max-width:\s*none/, 'calendar SVG must retain readable cell dimensions');
});
