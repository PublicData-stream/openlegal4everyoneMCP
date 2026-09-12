// Local-only simulated MCP Apps host; never contacts legal-data providers.
import { build } from 'esbuild';
import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
await build({ entryPoints: ['tests/host.ts'], bundle: true, outfile: '.harness/host.js', platform: 'browser', format: 'esm', target: 'es2022' });
const resources = new Map([
  ['/widget', ['text/html', await readFile('dist/index.html')]],
  ['/host.js', ['application/javascript', await readFile('.harness/host.js')]],
  ['/', ['text/html', '<!doctype html><html lang="en"><title>Offline MCP Apps test host</title><body><iframe title="Record browser" sandbox="allow-scripts" style="width:100%;height:900px;border:0"></iframe><pre id="calls" hidden>[]</pre><script type="module" src="/host.js"></script></body></html>']],
]);
const server = createServer((request, response) => {
  const resource = resources.get(new URL(request.url, 'http://127.0.0.1').pathname);
  if (!resource) { response.writeHead(404).end(); return; }
  response.writeHead(200, { 'Content-Type': resource[0], 'Cache-Control': 'no-store' }); response.end(resource[1]);
});
server.listen(4173, '127.0.0.1', () => console.log('Offline host: http://127.0.0.1:4173'));
for (const signal of ['SIGTERM', 'SIGINT']) process.on(signal, () => server.close());
