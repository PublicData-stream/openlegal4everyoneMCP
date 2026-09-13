import { build } from 'esbuild';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
const license = await readFile('../../LICENSE', 'utf8');
const escapedLicense = license.replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;');
const notices = await readFile('THIRD_PARTY_NOTICES.md', 'utf8');
const escapedNotices = notices.replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;');
const marker = '__OPENLEGAL_SOURCE_URL__';
await mkdir('dist', { recursive: true });
for (const [entry, filename, title, maximum] of [
  ['src/main.tsx', 'index.html', 'Synthetic record browser', 1024 * 1024],
  ['src/text-diff.tsx', 'text-diff.html', 'Text comparison', 3 * 1024 * 1024],
]) {
  const result = await build({ entryPoints: [entry], bundle: true, minify: true, write: false, outdir: 'dist', target: 'es2022', format: 'iife', legalComments: 'inline', define: { 'process.env.NODE_ENV': '"production"' } });
  const js = result.outputFiles.find(file => file.path.endsWith('.js')).text.replaceAll('</script', '<\\/script');
  const css = result.outputFiles.find(file => file.path.endsWith('.css')).text;
  const html = `<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><meta name="openlegal-source-url" content="${marker}"><title>${title}</title><style>${css}</style></head><body><div id="root"></div><footer aria-label="Software license"><p>Copyright © 2026 PiQuark6046. Licensed under GNU AGPL version 3 only (AGPL-3.0-only).</p><p>This program comes with no warranty. You may redistribute it under the terms of this license.</p><details><summary>Read the full GNU AGPLv3 license</summary><pre id="license-text">${escapedLicense}</pre></details>${filename === 'text-diff.html' ? `<details><summary>Third-party dependency notices</summary><pre id="dependency-notices">${escapedNotices}</pre></details>` : ''}</footer><script>${js}</script></body></html>`;
  if (html.split(marker).length !== 2) throw new Error('Widget requires exactly one source URL placeholder');
  if (Buffer.byteLength(html) > maximum) throw new Error(`${filename} exceeds its ${maximum} byte resource limit`);
  await writeFile(`dist/${filename}`, html);
  console.log(`Built self-contained dist/${filename} (${Buffer.byteLength(html)} bytes)`);
}
