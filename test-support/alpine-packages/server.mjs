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
  const pathname = new URL(request.url, 'https://dl-cdn.alpinelinux.org').pathname;
  console.log(request.method, pathname);
  const isPackage = pathname.endsWith('.apk');
  const isMetadata = pathname.endsWith('/APKINDEX.tar.gz');
  if (isPackage) stats.packageRequests += 1;
  if (isMetadata) stats.metadataRequests += 1;
  if (scenario === 'unknown' && isMetadata) {
    save(); response.writeHead(404); response.end(); return;
  }
  const fail = (isPackage && (scenario === 'persistent' ||
    (scenario === 'recovery' && stats.packageRequests <= 4))) ||
    (scenario === 'stale' && stats.metadataRequests > 2 && !isPackage) ||
    (scenario === 'mixed' && pathname.includes('/community/'));
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
    // Stall headers so APK reports its explicit network timeout diagnostic.
    return;
  }
  if (['signature', 'mixed'].includes(scenario) && pathname.endsWith('/APKINDEX.tar.gz')) {
    body = fs.readFileSync(path.join(path.dirname(filename), 'bad-index.tar.gz'));
  }
  if (scenario === 'hash' && isPackage) {
    body = fs.readFileSync(path.join(path.dirname(filename), 'bad-package.apk'));
  }
  response.writeHead(200, { 'Content-Length': body.length });
  response.end(body);
}).listen(443, '0.0.0.0', () => fs.writeFileSync('/tmp/ready', 'ready\n'));
