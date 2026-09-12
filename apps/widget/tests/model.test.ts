import { test } from 'node:test';
import assert from 'node:assert/strict';
import { parseDetail, parseInitial, parseSearch } from '../src/model.ts';
const record = { source: 'layout_a', id: 'demo-1', title: 'Synthetic', body: 'Text', synthetic: true };
const envelope = (data: unknown) => ({ data, provenance: { provider: 'synthetic', dataset: 'records', source_reference: 'synthetic:fixture', payload_sha256: 'a'.repeat(64), processor_version: '1.0.0', retrieved_at: 100, validated_at: 100 }, freshness: { state: 'fresh', age_seconds: 0 }, synthetic: true });
const response = (structuredContent: unknown) => ({ structuredContent });
test('rejects excessive results, invalid identity, false synthetic marker and age', () => {
  assert.throws(() => parseInitial(response({ synthetic: true, records: Array(21).fill(envelope(record)) })));
  for (const change of [{ source: 'other' }, { id: '' }, { id: 'a/b' }, { id: 'a b' }, { id: 'é' }, { id: 'a'.repeat(129) }, { synthetic: false }, { body: 'x'.repeat(32769) }]) assert.throws(() => parseDetail(response(envelope({ ...record, ...change }))));
  assert.throws(() => parseDetail(response({ ...envelope(record), freshness: { state: 'fresh', age_seconds: -1 } })));
});
test('validates pagination and reports errors without echoing source content', () => {
  assert.equal(parseSearch(response(envelope({ records: [record], page: 0, page_size: 5, total: 1 }))).records[0].record.id, 'demo-1');
  assert.throws(() => parseSearch(response(envelope({ records: [record], page: -1, page_size: 5, total: 1 }))));
  assert.throws(() => parseDetail({ isError: true, content: [{ text: 'private' }] }), error => error instanceof Error && !error.message.includes('private'));
});

test('retains bounded provenance and rejects malformed metadata', () => {
  const parsed = parseDetail(response(envelope(record)));
  assert.equal(parsed.provenance.processor_version, '1.0.0');
  assert.equal(parsed.provenance.source_reference, 'synthetic:fixture');
  for (const change of [{ payload_sha256: 'bad' }, { validated_at: -1 }, { retrieved_at: '100' }, { source_reference: 'x'.repeat(2049) }]) {
    assert.throws(() => parseDetail(response({ ...envelope(record), provenance: { ...envelope(record).provenance, ...change } })));
  }
});
