// Synthetic image acceptance only. No external requests or runtime dependencies.
import assert from 'node:assert/strict';
import http from 'node:http';
import { readFile } from 'node:fs/promises';

const modern = '2026-07-28';
const legacy = '2025-11-25';
const { source, authority } = JSON.parse(await readFile('/fixture/client.json', 'utf8'));
const maxBody = 16 * 1024 * 1024;

function exchange(path, body, protocol = legacy, session, overrides = {}) {
  return new Promise((resolve, reject) => {
    const headers = {};
    if (body) {
      Object.assign(headers, {
        'Content-Type': 'application/json', Accept: 'application/json, text/event-stream',
        'MCP-Protocol-Version': protocol, Host: authority,
      });
      if (session) headers['Mcp-Session-Id'] = session;
      if (protocol === modern) {
        headers['Mcp-Method'] = body.method;
        const name = body.params?.name ?? body.params?.uri;
        if (name) headers['Mcp-Name'] = name;
      }
    }
    Object.assign(headers, overrides);
    const request = http.request({
      hostname: 'server', port: body ? 8080 : 9090, path,
      method: body ? 'POST' : 'GET', headers,
    });
    // An absolute deadline also bounds peers which continually trickle bytes.
    const timer = setTimeout(() => request.destroy(new Error('request deadline exceeded')), 10000);
    request.on('error', reject);
    request.on('close', () => clearTimeout(timer));
    request.on('response', (response) => {
      let bytes = 0;
      const chunks = [];
      const finish = (value) => {
        resolve({ status: response.statusCode, headers: response.headers, value });
        response.destroy();
        request.destroy();
      };
      response.on('error', reject);
      response.on('data', (chunk) => {
        try {
          bytes += chunk.length;
          assert.ok(bytes <= maxBody, 'response exceeded bound');
          chunks.push(chunk);
          if (response.headers['content-type']?.startsWith('text/event-stream')) {
            const text = Buffer.concat(chunks).toString('utf8');
            // Ignore a final partial line, including a partial UTF-8 character.
            for (const line of text.slice(0, text.lastIndexOf('\n') + 1).split('\n')) {
              if (!line.startsWith('data:')) continue;
              const message = JSON.parse(line.slice(5).trim());
              if (message.id === body?.id && body?.id !== undefined) finish(message);
            }
          }
        } catch (error) { request.destroy(error); }
      });
      response.on('end', () => {
        try {
          const text = Buffer.concat(chunks).toString('utf8');
          finish(response.headers['content-type']?.startsWith('application/json') && text
            ? JSON.parse(text) : text);
        } catch (error) { reject(error); }
      });
    });
    request.end(body ? JSON.stringify(body) : undefined);
  });
}

const readinessDeadline = Date.now() + 60000;
let ready = false;
while (Date.now() < readinessDeadline) {
  try {
    const live = await exchange('/live');
    const readiness = await exchange('/ready');
    ready = live.status === 200 && readiness.status === 200;
    if (ready) break;
  } catch { /* startup polling is bounded by the overall deadline */ }
  await new Promise((resolve) => setTimeout(resolve, 500));
}
assert.ok(ready, 'server did not become live and ready');

const rejectedRequest = { jsonrpc: '2.0', id: 1, method: 'initialize', params: {
  protocolVersion: legacy, capabilities: {}, clientInfo: { name: 'image-smoke', version: '1' },
} };
for (const headers of [{ Host: 'untrusted.example:8080' }, { Origin: 'https://untrusted.example' }]) {
  const reply = await exchange('/mcp', rejectedRequest, legacy, undefined, headers);
  assert.equal(reply.status, 403, 'untrusted Host/Origin must be rejected');
}

let nextId = 0;
for (const protocol of [legacy, modern]) {
  let session;
  async function rpc(method, params = {}) {
    if (protocol === modern) params._meta = {
      'io.modelcontextprotocol/protocolVersion': protocol,
      'io.modelcontextprotocol/clientInfo': { name: 'image-smoke', version: '1' },
      'io.modelcontextprotocol/clientCapabilities': {},
    };
    const id = ++nextId;
    const reply = await exchange('/mcp', { jsonrpc: '2.0', id, method, params }, protocol, session);
    assert.equal(reply.status, 200, JSON.stringify(reply.value));
    assert.equal(reply.value?.id, id);
    assert.equal(reply.value.error, undefined, JSON.stringify(reply.value));
    if (method === 'initialize') session = reply.headers['mcp-session-id'];
    return reply.value.result;
  }
  if (protocol === legacy) {
    const initialized = await rpc('initialize', {
      protocolVersion: protocol, capabilities: {}, clientInfo: { name: 'image-smoke', version: '1' },
    });
    assert.equal(initialized.protocolVersion, protocol);
    const reply = await exchange('/mcp', {
      jsonrpc: '2.0', method: 'notifications/initialized',
    }, protocol, session);
    assert.ok([200, 202, 204].includes(reply.status));
  } else {
    const discovery = await rpc('server/discover');
    assert.equal(discovery.resultType, 'complete');
    assert.deepEqual(new Set(discovery.supportedVersions), new Set([legacy, modern]));
  }
  const listed = await rpc('tools/list');
  for (const name of ['server_info', 'text.diff']) {
    assert.ok(listed.tools.some((tool) => tool.name === name), `missing ${name}`);
  }
  async function call(name, args) {
    const result = await rpc('tools/call', { name, arguments: args });
    assert.ok(!result.isError, JSON.stringify(result));
    return result.structuredContent;
  }
  const info = await call('server_info', {});
  assert.equal(info.license, 'AGPL-3.0-only');
  assert.equal(info.sourceUrl, source);
  const diff = await call('text.diff', { before: 'first\nold\n', after: 'first\nnew\n' });
  assert.equal(diff.comparison.equal, false);
  assert.equal(diff.comparison.additions, 1);
  assert.equal(diff.comparison.deletions, 1);
  assert.ok(diff.patch.sealed);
  const patch = await call('text.attachment.read', { attachment_id: diff.patch.attachment_id });
  assert.ok(patch.text.includes('-old\n+new\n'), 'worker must produce the expected patch');
  const resource = await rpc('resources/read', { uri: 'ui://openlegal/text-diff-v1.html' });
  assert.equal(resource.contents.length, 1);
  assert.equal(resource.contents[0].mimeType, 'text/html;profile=mcp-app');
  const packaged = await readFile('/fixture/text-diff.html', 'utf8');
  assert.equal(resource.contents[0].text, packaged.replace('__OPENLEGAL_SOURCE_URL__', source));
  console.log(`HTTP ${protocol}: identity, packaged widget, text.diff subprocess passed`);
}
