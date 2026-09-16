import assert from 'node:assert/strict';
import test from 'node:test';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';

const globalStylesPath = fileURLToPath(new URL('../src/styles/global.css', import.meta.url));

test('critical styles do not depend on a remote stylesheet', async () => {
  const styles = await readFile(globalStylesPath, 'utf8');

  assert.doesNotMatch(
    styles,
    /@import\s+(?:url\()?['"]?https?:\/\//i,
    'remote CSS imports block the first render when the provider is slow',
  );
  assert.match(
    styles,
    /@fontsource-variable\/newsreader/i,
    'Newsreader should be bundled locally instead of fetched at runtime',
  );
});
