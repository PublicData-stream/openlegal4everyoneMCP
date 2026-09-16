import test from 'node:test';
import assert from 'node:assert/strict';
import { getResult, identity, metadata, searchPage, mergeCatalog, historyPage } from '../src/database-model.ts';
const id = { jurisdiction: 'kr', provider: 'fixture', dataset: 'national_statute' as const, id: 'fictional' };
const meta = { object: id, revision_id: 'r1', capture_id: 'a'.repeat(64), title: 'Fictional', source_url: 'https://fixture.example/', retrieved_at: 1, captured_at: 2, validated_at: 3, raw_sha256: 'b'.repeat(64), processor_version: 'fixture', metadata: {}, freshness: null };
test('database object and selector validation prevents checkpoint substitution', () => {
  assert.deepEqual(identity(id), id);
  assert.throws(() => identity({ ...id, provider: '../escape' }));
  assert.throws(() => metadata(meta, { ...id, id: 'other' }, { kind: 'revision', id: 'r1' }));
  assert.throws(() => metadata(meta, id, { kind: 'revision', id: 'r2' }));
  assert.throws(() => metadata(meta, id, { kind: 'head' }));
});
test('database content continuation uses exact bytes and search pages are bounded', () => {
  const page = { structuredContent: { session: 'e'.repeat(64), schema_version: 1, metadata: meta, section: 'body', text: '한', offset: 0, next_offset: 3, section_count: 0, next_sections_offset: null, sections: [] } };
  assert.equal(getResult(page, id, { kind: 'capture', id: meta.capture_id }, 'body', 0).next_offset, 3);
  assert.throws(() => getResult({ structuredContent: { ...page.structuredContent, next_offset: 1 } }, id, { kind: 'capture', id: meta.capture_id }, 'body', 0));
  assert.throws(() => getResult(page, id, { kind: 'capture', id: meta.capture_id }, 'body', 0, 'f'.repeat(64)));
  assert.throws(() => searchPage({ structuredContent: { schema_version: 1, hits: Array(21).fill({}) } }));
});

test('catalog paging merges only the same immutable session without gaps or duplicates', () => {
  const selector = { kind: 'capture' as const, id: meta.capture_id };
  const wire = { session: 'e'.repeat(64), schema_version: 1, metadata: meta, section: 'body', text: '', offset: 0, next_offset: null, section_count: 2, next_sections_offset: 1, sections: [{ id: 'a', title: 'First', kind: 'provider_text', bytes: 0 }] };
  const first = getResult({ structuredContent: wire }, id, selector, 'body', 0);
  const second = getResult({ structuredContent: { ...wire, next_sections_offset: null, sections: [{ ...wire.sections[0], id: 'b'.repeat(256), title: 'Second' }] } }, id, selector, 'body', 0, first.session, 1);
  const merged = mergeCatalog(first, second, 1);
  assert.equal(merged.sections.length, 2);
  assert.equal(mergeCatalog(merged, first).sections.length, 2);
  assert.throws(() => mergeCatalog(first, second, 2));
  assert.throws(() => mergeCatalog(first, { ...second, sections: first.sections }, 1));
  assert.throws(() => getResult({ structuredContent: { ...wire, next_sections_offset: 0 } }, id, selector, 'body', 0));
});
test('catalog-only history has no invented capture observation time', () => {
  const result = historyPage({ structuredContent: { entries: [{ revision_id: 'r1', capture_id: null, captured_at: null, sequence: 1, publication_date: null, effective_date: null }], next_cursor: null, inventory_complete: false } });
  assert.equal(result.entries[0].captured_at, null);
});
