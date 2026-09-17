import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { basename, join } from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const siteDir = fileURLToPath(new URL('..', import.meta.url));

test('home page prioritizes charts and loads the map separately', () => {
  execFileSync(process.execPath, ['./node_modules/astro/bin/astro.mjs', 'build'], {
    cwd: siteDir,
    stdio: 'inherit',
  });

  const html = readFileSync(join(siteDir, 'dist/index.html'), 'utf8');
  assert.match(
    html,
    /id="flight-map"[^>]*>[\s\S]*?role="status"[^>]*>Loading flight map/i,
    'the deferred map should show a loading state instead of an empty panel',
  );
  const entries = [...html.matchAll(/<script\b[^>]*\btype="module"[^>]*\bsrc="([^"]+)"[^>]*>/g)]
    .map((match) => match[1]);

  assert.equal(entries.length, 1, 'both charts should share one initial module entry');

  const entryPath = join(siteDir, 'dist', entries[0].replace(/^\//, ''));
  const entry = readFileSync(entryPath, 'utf8');
  assert.match(entry, /calendar-chart/, 'initial entry should initialize the calendar');
  assert.match(entry, /histogram-chart/, 'initial entry should initialize the histogram');

  const dynamicImports = [...entry.matchAll(/import\(\s*["']([^"']+)["']\s*\)/g)]
    .map((match) => match[1]);
  assert.ok(
    dynamicImports.some((specifier) => basename(specifier).startsWith('flight-map.')),
    'the map implementation should be a deferred chunk',
  );
  assert.ok(
    dynamicImports.some((specifier) => basename(specifier).startsWith('hex-map.')),
    'the map data should be a deferred chunk',
  );
});

test('home page ships data-derived calendar and histogram marks before scripts run', () => {
  execFileSync(process.execPath, ['./node_modules/astro/bin/astro.mjs', 'build'], {
    cwd: siteDir,
    stdio: 'inherit',
  });

  const html = readFileSync(join(siteDir, 'dist/index.html'), 'utf8');
  const calendarData = JSON.parse(readFileSync(join(siteDir, 'src/data/calendar.json'), 'utf8'));
  const histogramData = JSON.parse(readFileSync(join(siteDir, 'src/data/histogram.json'), 'utf8'));
  const chartContent = (id) => {
    const match = html.match(new RegExp(`<div[^>]*id="${id}"[^>]*>([\\s\\S]*?)</div>`));
    assert.ok(match, `built HTML should include #${id}`);
    return match[1];
  };

  const calendar = chartContent('calendar-chart');
  assert.equal(
    (calendar.match(/data-chart-mark="calendar-day"/g) ?? []).length,
    calendarData.length,
    'calendar fallback should ship one visual mark per tracked day',
  );
  assert.match(calendar, /data-date="\d{4}-\d{2}-\d{2}"/, 'calendar marks should expose their dates');
  assert.match(calendar, /aria-label="[^\"]*hours?[^\"]*flights?[^\"]*"/i, 'calendar marks should describe activity');

  const histogram = chartContent('histogram-chart');
  assert.equal(
    (histogram.match(/data-chart-mark="hour-bar"/g) ?? []).length,
    histogramData.length,
    'histogram fallback should ship one visual bar per hour bucket',
  );
  assert.match(histogram, /data-hour="\d+"/, 'histogram bars should expose their hour bucket');
  assert.match(histogram, /--bar-height:\s*\d+(?:\.\d+)?%/, 'histogram bars should encode data-derived height');
});
