import { test } from 'node:test';
import assert from 'node:assert/strict';
import { decodeFile, editAsLf, localPatch, MAX_LINE_BYTES, MAX_TEXT_BYTES, parseCompare, parseDelete, parsePage, parsePair, parseShow, parseSummary, validateLabel, validateText } from '../src/text-diff-model.ts';
import { fixtureFragment, fixtureId, fixturePage, fixtureSummary } from './text-diff-fixtures.ts';
const response = (structuredContent: unknown) => ({ structuredContent });
const expected = { comparison_id: fixtureId, view: 'changes' as const, page: 0 };
test('UTF-8 files preserve BOM, CRLF, CR and exact Unicode; invalid bytes and NUL fail', () => {
  const raw = '\uFEFF가😀\r\n다\r끝';
  const bytes = new TextEncoder().encode(raw);
  assert.equal(decodeFile(bytes.buffer), raw);
  assert.equal(editAsLf(raw), '\uFEFF가😀\n다\n끝');
  assert.throws(() => decodeFile(Uint8Array.of(0xc3, 0x28).buffer), /UTF-8/);
  assert.throws(() => decodeFile(Uint8Array.of(0).buffer), /NUL/);
  assert.throws(() => validateText('\ud800'), /Unicode/);
  assert.throws(() => validateText('\udc00'), /Unicode/);
});
test('text limits count UTF-8 bytes and LF-delimited logical lines', () => {
  validateText('가'.repeat(Math.floor(MAX_LINE_BYTES / 3)));
  assert.throws(() => validateText('가'.repeat(Math.floor(MAX_LINE_BYTES / 3) + 1)), /16 KiB/);
  validateText('\n'.repeat(100000));
  assert.throws(() => validateText('\n'.repeat(100001)), /100,000/);
  assert.throws(() => validateText('x'.repeat(MAX_TEXT_BYTES + 1)), /1 MiB/);
  assert.throws(() => validateText('x'.repeat(MAX_LINE_BYTES) + '\r\n'), /16 KiB/);
  assert.throws(() => validateLabel('가'.repeat(43)), /128/);
  assert.throws(() => validateLabel(''), /1–128/);
  assert.throws(() => validateLabel('name\nline'), /control/);
});
test('versioned summary/show/delete parsing rejects inconsistent results without private messages', () => {
  const summary = fixtureSummary('before\n', 'after\n');
  assert.equal(parseCompare(response(summary)).comparison_id, fixtureId);
  assert.deepEqual(parseShow(response({ schema_version: 1, comparison: summary })), summary);
  assert.equal(parseShow(response({ schema_version: 1, comparison: null })), null);
  parseDelete(response({ schema_version: 1, deleted: true }));
  for (const change of [{ comparison_id: '../file' }, { schema_version: 2 }, { equal: true }, { additions: 999 }, { change_pages: 0 }]) assert.throws(() => parseSummary({ ...summary, ...change }));
  assert.throws(() => parseDelete(response({ deleted: true })));
  assert.throws(() => parseCompare({ isError: true, content: [{ type: 'text', text: 'private secret' }] }), error => error instanceof Error && !error.message.includes('private'));
});
test('fragment rebasing is bounded by its displayed rows, preserving all patch data and newline markers', () => {
  const fragment = fixtureFragment('가\r\nlast', '😀\r\nlast\n', 99999, 99999);
  const rendered = localPatch(fragment);
  assert.ok(rendered.includes('@@ -1,2 +1,2 @@'));
  assert.equal(rendered.slice(rendered.indexOf('\n-', rendered.indexOf('@@'))), fragment.patch.slice(fragment.patch.indexOf('\n-', fragment.patch.indexOf('@@'))));
  assert.ok(rendered.includes('\\ No newline at end of file'));
  assert.equal(localPatch(fixtureFragment('', 'added\n', 0, 99999)).includes('@@ -0,0 +1,1 @@'), true);
  assert.equal(localPatch(fixtureFragment('deleted\n', '', 99999, 0)).includes('@@ -1,1 +0,0 @@'), true);
});
test('pages validate identity, view, counters, row bounds and full source chunks before rendering', () => {
  const page = fixturePage('old\n', 'new\n');
  assert.equal(parsePage(response(page), expected).fragments.length, 1);
  for (const change of [{ comparison_id: 'c'.repeat(64) }, { view: 'before' }, { page: 1 }, { total_pages: 0 }, { schema_version: 2 }]) assert.throws(() => parsePage(response({ ...page, ...change }), expected));
  assert.throws(() => parsePage(response({ ...page, fragments: [{ ...page.fragments[0], before_count: 3 }] }), expected));
  assert.throws(() => parsePage(response({ ...page, fragments: [{ ...page.fragments[0], patch: page.fragments[0].patch + '@@ -2,1 +2,1 @@\n extra\n' }] }), expected));
  assert.throws(() => parsePage(response({ ...page, fragments: Array(201).fill(page.fragments[0]) }), expected));
  const source = { schema_version: 1, comparison_id: fixtureId, view: 'before', page: 0, total_pages: 1, text: '\uFEFFraw\r\n', fragments: [] };
  assert.equal(parsePage(response(source), { ...expected, view: 'before' }).text, source.text);
  assert.throws(() => parsePage(response({ ...source, text: 'x'.repeat(32769) }), { ...expected, view: 'before' }));
});
test('initial pairs distinguish an empty pair from an existing handle and retain labels exactly', () => {
  assert.equal(parsePair({ comparison_id: fixtureId }), null);
  assert.deepEqual(parsePair({ before: '', after: '', before_label: ' left ' }), { before: '', after: '', before_label: ' left ' });
  assert.throws(() => parsePair({ before: '' }));
});
