// Local-only simulated MCP Apps host; never contacts legal-data providers.
import { build } from 'esbuild';
import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
await build({ entryPoints: ['tests/database-host.ts'], bundle: true, outfile: '.harness/database-host.js', platform: 'browser', format: 'esm', target: 'es2022' });
await build({ entryPoints: ['tests/diff-host.ts'], bundle: true, outfile: '.harness/diff-host.js', platform: 'browser', format: 'esm', target: 'es2022' });
await build({ entryPoints: ['tests/host.ts'], bundle: true, outfile: '.harness/host.js', platform: 'browser', format: 'esm', target: 'es2022' });
const widget = await readFile('dist/index.html', 'utf8');
const diffWidget = await readFile('dist/text-diff.html', 'utf8');
const databaseWidget = await readFile('dist/database.html', 'utf8');
const marker = '__OPENLEGAL_SOURCE_URL__';
if (widget.split(marker).length !== 2) throw new Error('Expected exactly one source URL placeholder');
// Fictional source location; host requests are recorded, never followed.
const sourceUrl = 'https://source.example/release?name="widget"&part=server';
const escapedSourceUrl = sourceUrl.replaceAll('&', '&amp;').replaceAll('"', '&quot;').replaceAll("'", '&#39;').replaceAll('<', '&lt;').replaceAll('>', '&gt;');
const resources = new Map([
  ['/database-widget', ['text/html', databaseWidget.replace('__OPENLEGAL_SOURCE_URL__', 'https://source.example/release')]],
  ['/database-host.js', ['application/javascript', await readFile('.harness/database-host.js')]],
  ['/database', ['text/html', '<!doctype html><html lang="en"><title>Fictional database fixture</title><body><p>Fictional database fixture</p><iframe title="Legal corpus" sandbox="allow-scripts" style="width:100%;height:1800px;border:0"></iframe><pre id="calls" hidden>[]</pre><script type="module" src="/database-host.js"></script></body></html>']],
  ['/diff-widget', ['text/html', diffWidget.replace('__OPENLEGAL_SOURCE_URL__', 'https://source.example/release')]],
  ['/diff-host.js', ['application/javascript', await readFile('.harness/diff-host.js')]],
  ['/diff', ['text/html', '<!doctype html><html lang="en"><title>Offline comparison host</title><body><iframe title="Text comparison" sandbox="allow-scripts" style="width:100%;height:1600px;border:0"></iframe><pre id="calls" hidden>[]</pre><pre id="links" hidden>[]</pre><script type="module" src="/diff-host.js"></script></body></html>']],
  ['/widget', ['text/html', widget.replace(marker, escapedSourceUrl)]],
  ['/host.js', ['application/javascript', await readFile('.harness/host.js')]],
  ['/', ['text/html', '<!doctype html><html lang="en"><title>Offline MCP Apps test host</title><body><iframe title="Record browser" sandbox="allow-scripts" style="width:100%;height:900px;border:0"></iframe><pre id="calls" hidden>[]</pre><pre id="links" hidden>[]</pre><script type="module" src="/host.js"></script></body></html>']],
]);
const server = createServer((request, response) => {
  const url = new URL(request.url, 'http://127.0.0.1');
  const resource = resources.get(url.pathname);
  if (!resource) { response.writeHead(404).end(); return; }
  let body = resource[1];
  if (url.pathname === '/widget' && url.searchParams.has('invalid-source')) body = widget.replace(marker, 'http://source.example/');
  if (url.pathname === '/widget' && url.searchParams.has('duplicate-source')) body = widget.replace(marker, escapedSourceUrl).replace('</head>', '<meta name="openlegal-source-url" content="https://other.example/"></head>');
  response.writeHead(200, { 'Content-Type': resource[0], 'Cache-Control': 'no-store' }); response.end(body);
});
server.listen(4173, '127.0.0.1', () => console.log('Offline host: http://127.0.0.1:4173'));
for (const signal of ['SIGTERM', 'SIGINT']) process.on(signal, () => server.close());
