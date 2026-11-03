// Build-only acquisition of the fixed, checksum-pinned ingestion tools/notices.
import { createHash } from 'node:crypto';
import { readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { setTimeout as pause } from 'node:timers/promises';
import { pathToFileURL } from 'node:url';

const TIMEOUT_MS = 300_000;
const MIB = 1024 * 1024;

export function kubectlArtifacts(arch, pins) {
  if (!['amd64', 'arm64'].includes(arch)) throw Error('Unsupported architecture');
  const matches = pins.split('\n').filter((line) => line.endsWith(`  ${arch}`));
  if (matches.length !== 1 || !/^[a-f0-9]{64}  (amd64|arm64)$/.test(matches[0])) {
    throw Error('Missing or ambiguous checksum');
  }
  return [
    { name: 'kubectl', url: `https://dl.k8s.io/release/v1.37.0/bin/linux/${arch}/kubectl`,
      checksum: matches[0].split(' ')[0], maxBytes: 128 * MIB, mode: 0o755 },
    { name: 'kubernetes.tar.gz', url: 'https://codeload.github.com/kubernetes/kubernetes/tar.gz/refs/tags/v1.37.0',
      checksum: '956ddae3b12acc08a715aea0411a168a36d5989b3e7185deeb6bf46b7dac19cf',
      maxBytes: 256 * MIB, mode: 0o644 },
    { name: 'GO-LICENSE', url: 'https://raw.githubusercontent.com/golang/go/go1.26.6/LICENSE',
      checksum: '911f8f5782931320f5b8d1160a76365b83aea6447ee6c04fa6d5591467db9dad',
      maxBytes: MIB, mode: 0o644 },
  ];
}

// Dependencies are source-owned test seams; the CLI supplies no overrides.
export async function downloadArtifact(artifact, directory, {
  fetchImpl = globalThis.fetch,
  now = () => performance.now(),
  sleep = pause,
  write = writeFileSync,
} = {}) {
  const deadline = now() + TIMEOUT_MS;
  const signal = AbortSignal.timeout(TIMEOUT_MS);
  const checkDeadline = () => {
    signal.throwIfAborted();
    if (now() >= deadline) throw Error('Artifact download deadline expired');
  };
  let response;
  for (let attempt = 1; attempt <= 3; attempt++) {
    checkDeadline();
    try {
      // Only this pre-headers operation is retryable. Body/hash/write never are.
      response = await fetchImpl(artifact.url, { signal });
    } catch (error) {
      checkDeadline();
      if (error?.cause?.code !== 'UND_ERR_CONNECT_TIMEOUT' || attempt === 3) throw error;
      const wait = attempt * 1000;
      if (now() + wait >= deadline) throw Error('Artifact download deadline expired');
      await sleep(wait, undefined, { signal });
      checkDeadline();
      continue;
    }
    break;
  }
  const cancelBody = () => { void response.body?.cancel().catch(() => {}); };
  try {
    checkDeadline();
    if (!response.ok) throw Error(`Download failed: ${response.status}`);
    const length = response.headers.get('content-length');
    if (length !== null && (!/^\d+$/.test(length) || BigInt(length) > BigInt(artifact.maxBytes))) {
      throw Error(`${artifact.name} size limit exceeded or invalid Content-Length`);
    }
    if (!response.body) throw Error(`${artifact.name} missing response body`);
  } catch (error) {
    cancelBody();
    throw error;
  }
  const reader = response.body.getReader();
  const chunks = [];
  const hash = createHash('sha256');
  let size = 0;
  try {
    while (true) {
      checkDeadline();
      const { done, value } = await reader.read();
      checkDeadline();
      if (done) break;
      size += value.byteLength;
      if (size > artifact.maxBytes) throw Error(`${artifact.name} size limit exceeded`);
      hash.update(value);
      chunks.push(Buffer.from(value));
    }
  } catch (error) {
    void reader.cancel().catch(() => {});
    throw error;
  } finally {
    reader.releaseLock();
  }
  if (hash.digest('hex') !== artifact.checksum) throw Error(`${artifact.name} checksum mismatch`);
  const bytes = Buffer.concat(chunks, size);
  checkDeadline();
  write(join(directory, artifact.name), bytes, { mode: artifact.mode });
  checkDeadline();
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const artifacts = kubectlArtifacts(process.env.TARGETARCH, readFileSync('/pins/kubectl.sha256', 'utf8'));
  for (const artifact of artifacts) await downloadArtifact(artifact, '/artifacts');
}
