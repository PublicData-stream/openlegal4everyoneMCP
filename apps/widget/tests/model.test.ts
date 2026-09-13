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

import { parseCapabilities, parseSnapshot, parseSnapshotPage, parseSnapshotOrigin, snapshotLabel } from '../src/history-model.ts';
const snapshotSummary = { snapshot_id: 'b'.repeat(64), sequence: 2, captured_at: 100, processor_version: '0.9.0', schema_version: 1, payload_sha256: 'a'.repeat(64) };
const getQuery = { operation: 'get' as const, source: 'layout_a' as const, id: 'demo-1' };
const historical = { schema_version: 1, snapshot: snapshotSummary, query: getQuery, data: record, provenance: { ...envelope(record).provenance, processor_version: '0.9.0' }, historical: true, synthetic: true, clock_anomaly: false };
test('history capabilities default off for older hosts and reject impossible combinations', () => {
  assert.equal(parseCapabilities(response({ synthetic: true, records: [] })).history, false);
  assert.deepEqual(parseCapabilities(response({ capabilities: { history: true, comparison: false, processor_versions: { layout_a: '1.0.0' } } })).processor_versions, { layout_a: '1.0.0' });
  assert.throws(() => parseCapabilities(response({ capabilities: { history: false, comparison: true } })));
});
test('history reads preserve old processors and require exact identity, schema and provenance', () => {
  const expected = { query: getQuery, snapshot_id: snapshotSummary.snapshot_id };
  assert.equal(parseSnapshot(response(historical), expected).records[0].body, 'Text');
  assert.match(snapshotLabel(snapshotSummary, '1.0.0'), /Previous processor 0.9.0/);
  for (const change of [{ historical: false }, { schema_version: 2 }, { query: { ...getQuery, id: 'demo-2' } }, { data: { ...record, source: 'layout_b' } }, { provenance: envelope(record).provenance }, { snapshot: { ...snapshotSummary, snapshot_id: 'c'.repeat(64) } }]) assert.throws(() => parseSnapshot(response({ ...historical, ...change }), expected));
  assert.throws(() => parseSnapshot({ isError: true, structuredContent: historical }, expected));
});
test('historical search pages keep exact query and embedded data without current freshness', () => {
  const query = { operation: 'search' as const, source: 'layout_a' as const, query: ' exact ', page: 1, page_size: 5 };
  const value = { ...historical, query, data: { records: [record], page: 1, page_size: 5, total: 6 } };
  assert.equal(parseSnapshot(response(value), { query, snapshot_id: snapshotSummary.snapshot_id }).total, 6);
  assert.throws(() => parseSnapshot(response(value), { query: { ...query, query: 'exact' }, snapshot_id: snapshotSummary.snapshot_id }));
  assert.throws(() => parseSnapshot(response({ ...value, data: { ...value.data, page: 0 } }), { query, snapshot_id: snapshotSummary.snapshot_id }));
  for (const change of [{ total: 20001 }, { total: 7 }, { records: [record, record], total: 7 }, { records: [], total: 6 }]) assert.throws(() => parseSnapshot(response({ ...value, data: { ...value.data, ...change } }), { query, snapshot_id: snapshotSummary.snapshot_id }));
});
test('history pagination rejects unbounded, duplicate and unordered summaries', () => {
  const value = { snapshots: [snapshotSummary], next_cursor: null, synthetic: true };
  assert.equal(parseSnapshotPage(response(value)).snapshots.length, 1);
  for (const snapshots of [Array(21).fill(snapshotSummary), [snapshotSummary, snapshotSummary], [snapshotSummary, { ...snapshotSummary, snapshot_id: 'c'.repeat(64), sequence: 3 }]]) assert.throws(() => parseSnapshotPage(response({ ...value, snapshots })));
  assert.throws(() => parseSnapshotPage(response({ ...value, snapshots: [], next_cursor: 'loop' })));
  assert.throws(() => parseSnapshotPage(response({ ...value, next_cursor: 'x'.repeat(257) })));
  assert.throws(() => parseSnapshotPage(response({ ...value, next_cursor: '가'.repeat(86) })));
});
test('comparison origin only admits the exact supported projection and bounded source identity', () => {
  const origin = { source: 'layout_a', record_id: 'demo-1', before: snapshotSummary, after: { ...snapshotSummary, snapshot_id: 'c'.repeat(64), sequence: 3 }, projection: 'title_lf_lf_body_v1' };
  assert.equal(parseSnapshotOrigin(origin).before.processor_version, '0.9.0');
  for (const change of [{ projection: 'body' }, { source: 'provider' }, { record_id: '../file' }, { before: { ...snapshotSummary, captured_at: -1 } }]) assert.throws(() => parseSnapshotOrigin({ ...origin, ...change }));
});
