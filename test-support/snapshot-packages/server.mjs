// Synthetic HTTPS origin. It is reachable only on the disposable internal bridge.
import https from 'node:https';
import fs from 'node:fs';
import path from 'node:path';

const root = '/fixture/repository';
const scenario = process.env.SCENARIO;
const stats = { packageRequests: 0, metadataRequests: 0, failures: 0, stalls: 0 };
const save = () => fs.writeFileSync('/tmp/stats.json', JSON.stringify(stats));
save();

https.createServer({
  key: fs.readFileSync('/fixture/server.key'),
  cert: fs.readFileSync('/fixture/ca.crt'),
}, (request, response) => {
  const pathname = new URL(request.url, 'https://snapshot.debian.org').pathname;
  const isPackage = pathname.endsWith('.deb');
  const isMetadata = pathname.endsWith('/InRelease');
  if (isPackage) stats.packageRequests += 1;
  if (isMetadata) stats.metadataRequests += 1;
  const fail = (isPackage && (scenario === 'persistent' ||
    (['baseline', 'recovery'].includes(scenario) && stats.packageRequests <= 4))) ||
    (scenario === 'stale' && stats.metadataRequests > 1 && !isPackage);
  if (fail) {
    stats.failures += 1;
    save();
    response.writeHead(503, { 'Content-Type': 'text/plain' });
    response.end('Synthetic TooManyRequests: no healthy backends\n');
    return;
  }
  if (scenario === 'deadline') {
    stats.stalls += 1;
    save();
    // Keep the connection open until the fixture's outer transaction deadline.
    return;
  }
  save();
  const filename = path.resolve(root, `.${pathname}`);
  if (!filename.startsWith(`${root}/`) || !fs.existsSync(filename) ||
      !fs.statSync(filename).isFile()) {
    response.writeHead(404);
    response.end();
    return;
  }
  let body = fs.readFileSync(filename);
  if (isPackage && scenario === 'stall') {
    stats.stalls += 1;
    save();
    // APT 3.0.3 retries header I/O internally before counting an acquisition
    // failure (methods/basehttp.cc, RUN_HEADERS_IO_ERROR). Sending the headers
    // first isolates its data timeout: one connection per acquisition attempt.
    response.writeHead(200, { 'Content-Length': body.length });
    response.flushHeaders();
    return;
  }
  if (scenario === 'signature' && pathname.endsWith('/InRelease')) {
    body = Buffer.from(body.toString().replace('Origin: Openlegal', 'Origin: Corrupted'));
  }
  if (scenario === 'hash' && isPackage) {
    body = Buffer.from(body);
    body[body.length - 1] ^= 1;
  }
  response.writeHead(200, { 'Content-Length': body.length });
  response.end(body);
}).listen(443, '0.0.0.0', () => fs.writeFileSync('/tmp/ready', 'ready\n'));
