import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { basename, join } from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const siteDir = fileURLToPath(new URL('..', import.meta.url));

test('home page prioritizes both charts and defers the map bundle', () => {
  execFileSync(process.execPath, ['./node_modules/astro/bin/astro.mjs', 'build'], {
    cwd: siteDir,
    stdio: 'inherit',
  });

  const html = readFileSync(join(siteDir, 'dist/index.html'), 'utf8');
  const entries = [...html.matchAll(/<script\b[^>]*\btype="module"[^>]*\bsrc="([^"]+)"[^>]*>/g)]
    .map((match) => match[1]);

  assert.equal(entries.length, 1, 'calendar and histogram should share one initial module entry');

  const entryPath = join(siteDir, 'dist', entries[0].replace(/^\//, ''));
  const entry = readFileSync(entryPath, 'utf8');
  assert.match(entry, /calendar-chart/, 'initial entry should initialize the calendar');
  assert.match(entry, /histogram-chart/, 'initial entry should initialize the histogram');

  const dynamicImports = [...entry.matchAll(/import\(\s*["']([^"']+)["']\s*\)/g)]
    .map((match) => match[1]);
  assert.equal(dynamicImports.length, 2, 'map implementation and data should be separate dynamic imports');
  assert.ok(
    dynamicImports.some((specifier) => basename(specifier).startsWith('flight-map.')),
    'flight-map should be a deferred chunk',
  );
  assert.ok(
    dynamicImports.some((specifier) => basename(specifier).startsWith('hex-map.')),
    'hex-map data should be a deferred chunk',
  );
});
