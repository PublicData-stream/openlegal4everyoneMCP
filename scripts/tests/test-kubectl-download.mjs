import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import test from 'node:test';
import { downloadArtifact, kubectlArtifacts } from '../download-kubectl.mjs';

const pins = '6129359f4e1f3848a5572ccb0b26cf28b8ca08cef38c95a765b2f64a2c961a2f  amd64\n'
  + '922df28df248cc00a9e025f947704f1d1482de64ece54cfe57e61f19eaf1eef3  arm64\n';
const bytes = Buffer.from('tool');
const artifact = {
  name: 'kubectl', url: 'https://dl.k8s.io/release/v1.37.0/bin/linux/amd64/kubectl',
  checksum: createHash('sha256').update(bytes).digest('hex'), maxBytes: 4, mode: 0o755,
};
const timeout = () => new TypeError('fetch failed', { cause: Object.assign(Error('connect'), {
  code: 'UND_ERR_CONNECT_TIMEOUT',
}) });
function fixture(fetchImpl) {
  let time = 0;
  const calls = [];
  const waits = [];
  const writes = [];
  return {
    calls, waits, writes,
    advance: (milliseconds) => { time += milliseconds; },
    options: {
      now: () => time,
      fetchImpl: async (url, options) => {
        calls.push({ url, ...options });
        return fetchImpl(calls.length, options);
      },
      sleep: async (milliseconds, value, options) => {
        waits.push({ milliseconds, value, ...options });
        time += milliseconds;
      },
      write: (...args) => { writes.push(args); },
    },
  };
}
const success = () => new Response(bytes);

test('fixed artifacts retain both architecture pins, origins, versions, caps and modes', () => {
  for (const [arch, pin] of [
    ['amd64', pins.split('  ')[0]],
    ['arm64', '922df28df248cc00a9e025f947704f1d1482de64ece54cfe57e61f19eaf1eef3'],
  ]) {
    assert.deepEqual(kubectlArtifacts(arch, pins), [
      { name: 'kubectl', url: `https://dl.k8s.io/release/v1.37.0/bin/linux/${arch}/kubectl`,
        checksum: pin, maxBytes: 134217728, mode: 0o755 },
      { name: 'kubernetes.tar.gz', url: 'https://codeload.github.com/kubernetes/kubernetes/tar.gz/refs/tags/v1.37.0',
        checksum: '956ddae3b12acc08a715aea0411a168a36d5989b3e7185deeb6bf46b7dac19cf',
        maxBytes: 268435456, mode: 0o644 },
      { name: 'GO-LICENSE', url: 'https://raw.githubusercontent.com/golang/go/go1.26.6/LICENSE',
        checksum: '911f8f5782931320f5b8d1160a76365b83aea6447ee6c04fa6d5591467db9dad',
        maxBytes: 1048576, mode: 0o644 },
    ]);
  }
});

test('unsupported architecture and missing, malformed or ambiguous pins fail before acquisition', () => {
  assert.throws(() => kubectlArtifacts('386', pins), /Unsupported/);
  for (const value of ['', 'invalid  amd64', pins + pins, pins.replace('6129', 'ABCD')]) {
    assert.throws(() => kubectlArtifacts('amd64', value), /checksum/);
  }
});

for (const failures of [0, 1, 2]) {
  test(`${failures} connection timeouts then success use one signal, URL and bounded waits`, async () => {
    const f = fixture((attempt) => { if (attempt <= failures) throw timeout(); return success(); });
    await downloadArtifact(artifact, '/artifacts', f.options);
    assert.equal(f.calls.length, failures + 1);
    assert.deepEqual(f.waits.map((wait) => wait.milliseconds), [1000, 2000].slice(0, failures));
    assert.ok(f.calls[0].signal instanceof AbortSignal);
    for (const call of [...f.calls, ...f.waits]) assert.equal(call.signal, f.calls[0].signal);
    assert.ok(f.calls.every((call) => call.url === artifact.url));
    assert.deepEqual(f.writes, [['/artifacts/kubectl', bytes, { mode: 0o755 }]]);
  });
}

test('persistent connection timeout stops at three attempts without publishing', async () => {
  const f = fixture(() => { throw timeout(); });
  await assert.rejects(downloadArtifact(artifact, '/artifacts', f.options), /fetch failed/);
  assert.equal(f.calls.length, 3);
  assert.deepEqual(f.waits.map((wait) => wait.milliseconds), [1000, 2000]);
  assert.equal(f.writes.length, 0);
});

for (const error of [
  Object.assign(Error('TLS'), { cause: { code: 'CERT_HAS_EXPIRED' } }),
  Object.assign(Error('reset'), { cause: { code: 'ECONNRESET' } }),
  Object.assign(Error('top-level code'), { code: 'UND_ERR_CONNECT_TIMEOUT' }),
  Object.assign(Error('aggregate'), { cause: { errors: [{ code: 'UND_ERR_CONNECT_TIMEOUT' }] } }),
  Error('unknown'),
]) {
  test(`${error.message} is terminal without transport broadening`, async () => {
    const f = fixture(() => { throw error; });
    await assert.rejects(downloadArtifact(artifact, '/artifacts', f.options), (actual) => actual === error);
    assert.equal(f.calls.length, 1);
    assert.equal(f.waits.length, 0);
    assert.equal(f.writes.length, 0);
  });
}

for (const status of [301, 404, 429, 502, 503, 504]) {
  test(`HTTP ${status} is terminal and cancels the response body`, async () => {
    let cancelled = false;
    const f = fixture(() => new Response(new ReadableStream({ cancel() { cancelled = true; } }), { status }));
    await assert.rejects(downloadArtifact(artifact, '/artifacts', f.options), /Download failed/);
    assert.equal(f.calls.length, 1);
    assert.equal(f.waits.length, 0);
    assert.equal(f.writes.length, 0);
    assert.equal(cancelled, true);
  });
}

test('all retries retain the first absolute deadline', async () => {
  const f = fixture(() => { f.advance(149000); throw timeout(); });
  await assert.rejects(downloadArtifact(artifact, '/artifacts', f.options), /deadline expired/);
  assert.equal(f.calls.length, 2);
  assert.deepEqual(f.waits.map((wait) => wait.milliseconds), [1000]);
  assert.equal(f.writes.length, 0);
});

test('expired fetch failure is not retried', async () => {
  const f = fixture(() => { f.advance(300000); throw timeout(); });
  await assert.rejects(downloadArtifact(artifact, '/artifacts', f.options), /deadline expired/);
  assert.equal(f.calls.length, 1);
  assert.equal(f.waits.length, 0);
});

test('backoff arriving at the original deadline starts no next attempt', async () => {
  const f = fixture(() => { throw timeout(); });
  f.options.sleep = async () => { f.advance(300000); };
  await assert.rejects(downloadArtifact(artifact, '/artifacts', f.options), /deadline expired/);
  assert.equal(f.calls.length, 1);
  assert.equal(f.writes.length, 0);
});

test('headers arriving at the deadline cannot admit a body or write', async () => {
  const f = fixture(() => { f.advance(300000); return success(); });
  await assert.rejects(downloadArtifact(artifact, '/artifacts', f.options), /deadline expired/);
  assert.equal(f.calls.length, 1);
  assert.equal(f.writes.length, 0);
});

test('body arriving at the deadline cannot publish', async () => {
  const f = fixture(() => new Response(new ReadableStream({
    pull(controller) { f.advance(300000); controller.enqueue(bytes); controller.close(); },
  })));
  await assert.rejects(downloadArtifact(artifact, '/artifacts', f.options), /deadline expired/);
  assert.equal(f.calls.length, 1);
  assert.equal(f.writes.length, 0);
});

for (const length of ['5', '9007199254740993', 'invalid']) {
  test(`declared size ${length} is rejected without reading or retry`, async () => {
    let cancelled = false;
    const f = fixture(() => new Response(new ReadableStream({ cancel() { cancelled = true; } }), {
      headers: { 'content-length': length },
    }));
    await assert.rejects(downloadArtifact(artifact, '/artifacts', f.options), /size limit/);
    assert.equal(f.calls.length, 1);
    assert.equal(cancelled, true);
    assert.equal(f.writes.length, 0);
  });
}

test('streaming size cap rejects chunked or understated responses and cancels', async () => {
  for (const headers of [{}, { 'content-length': '1' }]) {
    let cancelled = false;
    const f = fixture(() => new Response(new ReadableStream({
      start(controller) { controller.enqueue(bytes); controller.enqueue(new Uint8Array([1])); },
      cancel() { cancelled = true; },
    }), { headers }));
    await assert.rejects(downloadArtifact(artifact, '/artifacts', f.options), /size limit/);
    assert.equal(cancelled, true);
    assert.equal(f.calls.length, 1);
    assert.equal(f.writes.length, 0);
  }
});

test('body failure resembling a connection timeout is never retried', async () => {
  const error = timeout();
  const f = fixture(() => new Response(new ReadableStream({ start(controller) { controller.error(error); } })));
  await assert.rejects(downloadArtifact(artifact, '/artifacts', f.options), (actual) => actual === error);
  assert.equal(f.calls.length, 1);
  assert.equal(f.waits.length, 0);
  assert.equal(f.writes.length, 0);
});

test('checksum mismatch never publishes or retries', async () => {
  const f = fixture(success);
  await assert.rejects(downloadArtifact({ ...artifact, checksum: '0'.repeat(64) }, '/artifacts', f.options), /checksum mismatch/);
  assert.equal(f.calls.length, 1);
  assert.equal(f.writes.length, 0);
});

test('write failure is propagated without retry', async () => {
  const f = fixture(success);
  const error = Object.assign(Error('full'), { code: 'ENOSPC' });
  let writes = 0;
  f.options.write = () => { writes++; throw error; };
  await assert.rejects(downloadArtifact(artifact, '/artifacts', f.options), (actual) => actual === error);
  assert.equal(f.calls.length, 1);
  assert.equal(writes, 1);
});

test('a write crossing the original deadline fails after exactly one write without retry', async () => {
  const f = fixture(success);
  const recordWrite = f.options.write;
  f.options.write = (...args) => {
    recordWrite(...args);
    f.advance(300000);
  };
  await assert.rejects(downloadArtifact(artifact, '/artifacts', f.options), /deadline expired/);
  assert.deepEqual(f.writes, [['/artifacts/kubectl', bytes, { mode: 0o755 }]]);
  assert.equal(f.calls.length, 1);
  assert.equal(f.waits.length, 0);
});

test('notice bytes retain mode 0644 after real SHA verification', async () => {
  const f = fixture(success);
  await downloadArtifact({ ...artifact, name: 'GO-LICENSE', mode: 0o644 }, '/artifacts', f.options);
  assert.deepEqual(f.writes, [['/artifacts/GO-LICENSE', bytes, { mode: 0o644 }]]);
});
