import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { DATASETS, datasetLabel, hasProviderRevisions, getResult, identity, metadata, searchPage, mergeCatalog, historyPage } from '../src/database-model.ts';
const id = { jurisdiction: 'kr', provider: 'fixture', dataset: 'national_statute' as const, id: 'fictional' };
const meta = { object: id, revision_id: 'r1', capture_id: 'a'.repeat(64), title: 'Fictional', source_url: 'https://fixture.example/', retrieved_at: 1, captured_at: 2, validated_at: 3, raw_sha256: 'b'.repeat(64), processor_version: 'fixture', metadata: {}, freshness: null };
test('browser dataset and history contracts match the closed Rust domain', () => {
  const domain = readFileSync(new URL('../../../crates/domain/src/legal.rs', import.meta.url), 'utf8');
  const names = new Map([...domain.split('pub fn as_str')[1].split('pub fn from_name')[0].matchAll(/Self::(\w+) => "([a-z_]+)"/g)].map(match => [match[1], match[2]]));
  const expected = [...domain.split('pub const ALL:')[1].split('];')[0].matchAll(/Self::(\w+)/g)].map(match => names.get(match[1]));
  assert.equal(DATASETS.length, 69);
  assert.equal(new Set(DATASETS).size, DATASETS.length);
  assert.deepEqual(DATASETS, expected);
  const revisionFamilies = [...domain.split('pub fn has_provider_revisions')[1].split('\n    }')[0].matchAll(/Self::(\w+)/g)].map(match => names.get(match[1]));
  assert.deepEqual(DATASETS.filter(hasProviderRevisions), revisionFamilies);
  assert.equal(datasetLabel('national_statute'), 'National statutes');
  assert.equal(datasetLabel('ppc_decision'), 'Ppc decision');
});
test('new source records and notices remain valid in mixed dataset search pages', () => {
  const datasets = ['english_statute', 'public_institution_rule', 'ppc_decision', 'mof_interpretation', 'audit_consultation'] as const;
  const hits = datasets.map(dataset => ({ object: { ...id, dataset }, revision_id: 'r1', capture_id: meta.capture_id, title: 'Fictional source', section: 'article:1', line: 1, text: 'Fictional text', derived_ocr: false, match_scope: 'line', excerpt_section: 'article:1', includes_ocr: false }));
  const notices = datasets.map(dataset => ({ dataset, scope: 'detail', code: 'source_data_invalid', affected_count: 1, last_seen_at: 100, retry_at: 3700 }));
  const page = searchPage({ structuredContent: { schema_version: 1, hits, next_cursor: null, generation: 1, corpus_complete: false, index_lag: 0, collection_notices: notices } });
  assert.deepEqual(page.hits.map(hit => hit.object.dataset), datasets);
  assert.deepEqual(page.collection_notices, notices);
  for (const dataset of DATASETS) {
    const object = { ...id, dataset };
    assert.deepEqual(identity(object), object);
    assert.deepEqual(metadata({ ...meta, object, collection_notices: [{ ...notices[0], dataset }] }, object, { kind: 'capture', id: meta.capture_id }).collection_notices[0].dataset, dataset);
  }
  for (const dataset of ['arbitrary_source', '../ppc_decision', 'PpcDecision']) {
    assert.throws(() => identity({ ...id, dataset }), /Unsupported dataset/);
    assert.throws(() => searchPage({ structuredContent: { schema_version: 1, hits: [], next_cursor: null, generation: 0, corpus_complete: false, index_lag: 0, collection_notices: [{ ...notices[0], dataset }] } }), /unsupported/);
  }
  assert.equal(hasProviderRevisions('english_statute'), true);
  assert.equal(hasProviderRevisions('ppc_decision'), false);
  assert.equal(hasProviderRevisions('audit_consultation'), false);
});
test('database object and selector validation prevents checkpoint substitution', () => {
  assert.deepEqual(identity(id), id);
  assert.throws(() => identity({ ...id, provider: '../escape' }));
  assert.throws(() => metadata(meta, { ...id, id: 'other' }, { kind: 'revision', id: 'r1' }));
  assert.throws(() => metadata(meta, id, { kind: 'revision', id: 'r2' }));
  assert.throws(() => metadata(meta, id, { kind: 'head' }));
});
test('incomplete attachment counts are exposed and bounded', () => {
  const partial = { ...meta, metadata: { attachment_status: 'incomplete', attachment_expected_count: '3', attachment_available_count: '2', attachment_failures: '[]' } };
  assert.deepEqual(metadata(partial, id, { kind: 'capture', id: meta.capture_id }).missingAttachments, { expected: 3, available: 2 });
  assert.equal(metadata(meta, id, { kind: 'capture', id: meta.capture_id }).missingAttachments, null);
  assert.throws(() => metadata({ ...partial, metadata: { ...partial.metadata, attachment_available_count: '3' } }, id, { kind: 'capture', id: meta.capture_id }));
});
test('collection notices are bounded, validated and retained alongside content', () => {
  const notice = { dataset: 'national_statute', scope: 'detail', code: 'source_data_invalid', affected_count: 1, last_seen_at: 100, retry_at: 3700 };
  assert.deepEqual(metadata({ ...meta, collection_notices: [notice] }, id, { kind: 'capture', id: meta.capture_id }).collection_notices, [notice]);
  assert.deepEqual(searchPage({ structuredContent: { schema_version: 1, hits: [], next_cursor: null, generation: 0, corpus_complete: false, index_lag: 0, collection_notices: [notice] } }).collection_notices, [notice]);
  assert.deepEqual(metadata({ ...meta, collection_notices: [{ ...notice, code: 'download_failed' }] }, id, { kind: 'capture', id: meta.capture_id }).collection_notices[0].code, 'download_failed');
  assert.throws(() => metadata({ ...meta, collection_notices: [{ ...notice, code: 'unknown' }] }, id, { kind: 'capture', id: meta.capture_id }));
  assert.throws(() => searchPage({ structuredContent: { schema_version: 1, hits: [], collection_notices: Array(65).fill(notice) } }));
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
