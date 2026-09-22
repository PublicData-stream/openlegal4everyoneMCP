// Synthetic API for packaged kubectl acceptance. This is not Kubernetes admission,
// RBAC, token projection, a scheduler, or a document-worker sandbox.
import assert from 'node:assert/strict';
import { appendFileSync, readFileSync } from 'node:fs';
import https from 'node:https';

const namespace = 'openlegal-documents';
const base = `/api/v1/namespaces/${namespace}`;
let pod;
const status = (code, reason) => ({ apiVersion: 'v1', kind: 'Status', status: 'Failure', reason, code, message: `Synthetic API: ${reason}` });
const resource = (name, kind, verbs) => ({ name, singularName: name.slice(0, -1), namespaced: true, kind, verbs });
const schema = {
  openapi: '3.0.0', info: { title: 'Synthetic core API', version: 'v1' },
  // kubectl probes fieldValidation support on the GVK PATCH operation, even for create.
  paths: { '/api/v1/namespaces/{namespace}/pods/{name}': { patch: { parameters: [{ name: 'fieldValidation', in: 'query', schema: { type: 'string' } }], responses: { 201: { description: 'Created' } }, 'x-kubernetes-group-version-kind': { group: '', version: 'v1', kind: 'Pod' } } } },
  components: { schemas: { 'io.k8s.api.core.v1.Pod': {
    type: 'object', required: ['apiVersion', 'kind', 'metadata', 'spec'],
    properties: { apiVersion: { type: 'string' }, kind: { type: 'string' }, metadata: { type: 'object' }, spec: { type: 'object' } },
    'x-kubernetes-group-version-kind': [{ group: '', version: 'v1', kind: 'Pod' }],
  } } },
};
const server = https.createServer({ key: readFileSync('/fixture/tls.key'), cert: readFileSync('/fixture/tls.crt') }, async (req, res) => {
  const url = new URL(req.url, 'https://api:8443');
  const expected = readFileSync('/fixture/token', 'utf8').trim();
  const authorization = req.headers.authorization;
  const authorized = authorization === `Bearer ${expected}`;
  const forbidden = authorization === 'Bearer synthetic-forbidden';
  const event = { method: req.method, path: url.pathname, authenticated: authorized, forbidden, generation: expected === 'synthetic-first' ? 1 : 2 };
  appendFileSync('/tmp/events.jsonl', `${JSON.stringify(event)}\n`);
  const reply = (code, body) => { res.writeHead(code, { 'content-type': 'application/json' }); res.end(JSON.stringify(body)); };
  if (forbidden) return reply(403, status(403, 'Forbidden'));
  if (!authorized) return reply(401, status(401, 'Unauthorized'));
  if (req.method === 'GET' && url.pathname === '/api') return reply(200, { apiVersion: 'v1', kind: 'APIVersions', versions: ['v1'], serverAddressByClientCIDRs: [] });
  if (req.method === 'GET' && url.pathname === '/apis') return reply(200, { apiVersion: 'v1', kind: 'APIGroupList', groups: [] });
  if (req.method === 'GET' && url.pathname === '/api/v1') return reply(200, { apiVersion: 'v1', kind: 'APIResourceList', groupVersion: 'v1', resources: [resource('pods', 'Pod', ['create', 'get', 'delete']), resource('resourcequotas', 'ResourceQuota', ['get'])] });
  if (req.method === 'GET' && url.pathname === '/openapi/v3') return reply(200, { paths: { 'api/v1': { serverRelativeURL: '/openapi/v3/api/v1?hash=synthetic' } } });
  if (req.method === 'GET' && url.pathname === '/openapi/v3/api/v1') return reply(200, schema);
  if (req.method === 'GET' && url.pathname === `${base}/resourcequotas/document-budget`) return reply(200, { apiVersion: 'v1', kind: 'ResourceQuota', metadata: { name: 'document-budget', namespace }, spec: { hard: { pods: '2' } } });
  if (req.method === 'POST' && url.pathname === `${base}/pods`) {
    const chunks = [];
    let size = 0;
    for await (const chunk of req) { size += chunk.length; if (size > 65536) return reply(413, status(413, 'RequestEntityTooLarge')); chunks.push(chunk); }
    try {
      const candidate = JSON.parse(Buffer.concat(chunks).toString());
      assert.equal(url.searchParams.get('fieldValidation'), 'Strict');
      assert.equal(candidate.apiVersion, 'v1'); assert.equal(candidate.kind, 'Pod');
      assert.equal(candidate.metadata.namespace, namespace); assert.equal(candidate.metadata.name, 'document-image-fixture');
      assert.equal(candidate.spec.containers[0].name, 'worker');
      assert.equal(candidate.spec.containers[0].image, 'synthetic.invalid/worker@sha256:' + '0'.repeat(64));
      pod = { ...candidate, metadata: { ...candidate.metadata, uid: 'synthetic-pod-uid', resourceVersion: '1' } };
      return reply(201, pod);
    } catch { return reply(422, status(422, 'Invalid')); }
  }
  if (url.pathname === `${base}/pods/document-image-fixture`) {
    if (req.method === 'GET') return pod ? reply(200, pod) : reply(404, status(404, 'NotFound'));
    if (req.method === 'DELETE') { pod = undefined; return reply(200, { apiVersion: 'v1', kind: 'Status', status: 'Success', details: { name: 'document-image-fixture', kind: 'pods' } }); }
  }
  return reply(404, status(404, 'NotFound'));
});
server.requestTimeout = 10000;
server.listen(8443, '0.0.0.0', () => console.log('Synthetic TLS API ready'));
