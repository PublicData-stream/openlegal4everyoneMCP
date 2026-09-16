import { test } from 'node:test';
import assert from 'node:assert/strict';
import { decodeFile, editAsLf, fragmentRows, scalarSegments, splitRows, MAX_LINE_BYTES, MAX_TEXT_BYTES, parseCompare, parseDelete, parsePage, parsePair, parseShow, parseSummary, validateLabel, validateText } from '../src/text-diff-model.ts';
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
test('bounded fragment rows preserve exact Unicode and newline data at large source offsets', () => {
  const fragment = fixtureFragment('가\r\nlast', '😀\r\nlast\n', 99999, 99999);
  const rows = fragmentRows(fragment);
  assert.deepEqual(rows.map(row => row.text), ['가\r\n', 'last', '😀\r\n', 'last\n']);
  assert.deepEqual(rows.map(row => [row.before, row.after]), [[1, undefined], [2, undefined], [undefined, 1], [undefined, 2]]);
  assert.equal(rows[1].noFinalNewline, true);
  assert.equal(rows[3].noFinalNewline, false);
  assert.deepEqual(splitRows(rows).map(pair => [pair.before?.text, pair.after?.text]), [['가\r\n', '😀\r\n'], ['last', 'last\n']]);
  assert.equal(fragmentRows(fixtureFragment('', 'added\n', 0, 99999))[0].after, 1);
  assert.equal(fragmentRows(fixtureFragment('deleted\n', '', 99999, 0))[0].before, 1);
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

test('Rust scalar offsets preserve supplementary characters, CJK, combining marks and separate changes', () => {
  const raw = '😀가e\u0301漢字A\r\n';
  assert.deepEqual(scalarSegments(raw, [[0, 1], [3, 4], [5, 6], [7, 9]]), [
    { text: '😀', changed: true }, { text: '가e', changed: false },
    { text: '\u0301', changed: true }, { text: '漢', changed: false },
    { text: '字', changed: true }, { text: 'A', changed: false }, { text: '\r\n', changed: true },
  ]);
  const fragment = fixtureFragment(raw, '😀나e\u0301漢語B\n');
  // Mark the shared emoji, deliberately leaving actual substitutions unmarked.
  // The renderer follows server metadata instead of deriving another diff.
  fragment.inline_changes = [{ row_index: 0, ranges: [[0, 1]] }, { row_index: 1, ranges: [] }];
  assert.deepEqual(fragmentRows(fragment).map(row => scalarSegments(row.text, row.ranges).filter(part => part.changed)), [[{ text: '😀', changed: true }], []]);
});
test('missing, duplicate, malformed, overlapping and UTF-16-like out-of-bounds highlights fail closed', () => {
  const page = fixturePage('😀\n', '가\n');
  const fragment = page.fragments[0];
  for (const inline_changes of [undefined, [], [{ row_index: 0, ranges: [] }],
    [{ row_index: 0, ranges: [] }, { row_index: 0, ranges: [] }],
    [{ row_index: 1, ranges: [] }, { row_index: 0, ranges: [] }],
    [{ row_index: 0, ranges: [[0, 3]] }, { row_index: 1, ranges: [] }],
    [{ row_index: 0, ranges: [[1, 1]] }, { row_index: 1, ranges: [] }],
    [{ row_index: 0, ranges: [[0, 2], [1, 2]] }, { row_index: 1, ranges: [] }],
    [{ row_index: 0, ranges: [[-1, 1]] }, { row_index: 1, ranges: [] }],
    [{ row_index: 0, ranges: [[0, 1.5]] }, { row_index: 1, ranges: [] }],
    [{ row_index: 0, ranges: [[0, 1, 2]] }, { row_index: 1, ranges: [] }]]) {
    assert.throws(() => parsePage(response({ ...page, fragments: [{ ...fragment, inline_changes }] }), expected));
  }
  const noNewline = fixtureFragment('😀', '字');
  noNewline.inline_changes[0].ranges = [[0, 2]];
  assert.throws(() => fragmentRows(noNewline));
  const context = { ...fragment, patch: '--- before\n+++ after\n@@ -1,1 +1,1 @@\n same\n', inline_changes: [{ row_index: 0, ranges: [] }] };
  assert.throws(() => fragmentRows(context));
  const invalidText = { ...fragment, patch: fragment.patch.replace('😀', '\ud800') };
  assert.throws(() => fragmentRows(invalidText));
});
test('range budgets include every fragment and accept exactly 4096 page ranges', () => {
  const fragment = fixtureFragment('x'.repeat(8192) + '\n', 'new\n');
  fragment.inline_changes = [{ row_index: 0, ranges: Array.from({ length: 4096 }, (_, index) => [index * 2, index * 2 + 1]) }, { row_index: 1, ranges: [] }];
  const page = { ...fixturePage('', ''), fragments: [fragment] };
  assert.equal(parsePage(response(page), expected).fragments.length, 1);
  assert.throws(() => parsePage(response({ ...page, fragments: [fragment, fixtureFragment('a', 'b')] }), expected));
  fragment.inline_changes[1].ranges = [[0, 1]];
  assert.throws(() => parsePage(response(page), expected));
});
test('split rows retain context and source order across unequal replacement blocks', () => {
  const fragment = fixtureFragment('first\nsecond\n', 'new\n');
  fragment.patch = fragment.patch.replace('@@ -1,2 +1,1 @@', '@@ -1,3 +1,2 @@') + ' same\n';
  fragment.before_count = 3; fragment.after_count = 2;
  const pairs = splitRows(fragmentRows(fragment));
  assert.deepEqual(pairs.map(pair => [pair.before?.text, pair.after?.text]), [['first\n', 'new\n'], ['second\n', undefined], ['same\n', 'same\n']]);
});
test('shared range budget rejects before reading overflowing ranges or later fragments', () => {
  const first = fixtureFragment('x'.repeat(8192) + '\n', '');
  first.inline_changes[0].ranges = Array.from({ length: 2048 }, (_, index) => [index * 2, index * 2 + 1]);
  const second = fixtureFragment('y'.repeat(8192) + '\n', '');
  const ranges = new Array(2049);
  Object.defineProperty(ranges, 0, { get() { throw new Error('overflowing range was processed'); } });
  second.inline_changes[0].ranges = ranges;
  const later = { get patch() { throw new Error('later fragment was processed'); } };
  assert.throws(() => parsePage(response({ ...fixturePage('', ''), fragments: [first, second, later] }), expected), /unsupported comparison response/);
});
test('shared row budget stops before later fragments and before validating an excess data row', () => {
  const full = fixtureFragment('', 'x\n'.repeat(400));
  const later = { get patch() { throw new Error('later fragment was processed'); } };
  assert.throws(() => parsePage(response({ ...fixturePage('', ''), fragments: [full, later] }), expected), /unsupported comparison response/);
  const excess = { ...full, patch: full.patch + '+\0\n' };
  // The 401st row is rejected before text validation or allocation. Metadata is
  // intentionally inaccessible, detecting any work after the row budget fails.
  Object.defineProperty(excess, 'inline_changes', { get() { throw new Error('annotations were processed'); } });
  assert.throws(() => parsePage(response({ ...fixturePage('', ''), fragments: [excess] }), expected), /unsupported comparison response/);
});
test('shared byte budget rejects an oversized next patch before inspecting its annotations', () => {
  const first = fixtureFragment('', ('\t'.repeat(16000) + '\n').repeat(8));
  const second = fixtureFragment('', 'x'.repeat(16000) + '\n');
  Object.defineProperty(second, 'inline_changes', { get() { throw new Error('oversized annotations were processed'); } });
  const later = { get patch() { throw new Error('later fragment was processed'); } };
  assert.throws(() => parsePage(response({ ...fixturePage('', ''), fragments: [first, second, later] }), expected), /unsupported comparison response/);
});
test('page byte accounting includes escaped patch data and highlight metadata at the exact limit', () => {
  const maximum = 256 * 1024;
  let padding = 0;
  const makePage = () => ({ ...fixturePage('', ''), fragments: [fixtureFragment('', ('\t'.repeat(16000) + '\n').repeat(8) + 'x'.repeat(padding) + '\n')] });
  let page = makePage();
  for (let attempt = 0; attempt < 5; attempt++) {
    const difference = maximum - new TextEncoder().encode(JSON.stringify(page)).byteLength;
    if (difference === 0) break;
    padding += difference;
    page = makePage();
  }
  assert.equal(new TextEncoder().encode(JSON.stringify(page)).byteLength, maximum);
  assert.equal(parsePage(response(page), expected).fragments.length, 1);
  padding++;
  assert.throws(() => parsePage(response(makePage()), expected), /unsupported comparison response/);
});

test('attachment chunks preserve scalar boundaries and bound escaping independently', async () => {
  const { attachmentChunks, decodePatchFile } = await import('../src/text-diff-model.ts');
  const text = '\uFEFF' + '한😀\r\n'.repeat(10000);
  const chunks = [...attachmentChunks(text)];
  assert.equal(chunks.map(x => x.chunk).join(''), text);
  let offset = 0;
  for (const chunk of chunks) { assert.equal(chunk.offset, offset); const bytes = new TextEncoder().encode(chunk.chunk).byteLength; assert.ok(bytes <= 32768); offset += bytes; }
  assert.equal(chunks.at(-1)?.final, true);
  assert.deepEqual([...attachmentChunks('')], [{ offset: 0, chunk: '', final: true }]);
  assert.equal(decodePatchFile(new TextEncoder().encode('\uFEFF한\r\n').buffer), '\uFEFF한\r\n');
});

test('attachment response validator rejects wrong identity and false completion', async () => {
  const { parseAttachment, parseAttachmentChunk } = await import('../src/text-diff-model.ts');
  const attachment = { schema_version: 1, attachment_id: 'a'.repeat(64), kind: 'text', total_bytes: 3, committed_bytes: 3, sealed: true, expires_at: 1234 };
  const expected = parseAttachment(attachment);
  const response = { structuredContent: { schema_version: 1, attachment, offset: 0, next_offset: 3, complete: true, text: '한' } };
  assert.equal(parseAttachmentChunk(response, expected, 0).text, '한');
  assert.throws(() => parseAttachmentChunk({ structuredContent: { ...response.structuredContent, complete: false } }, expected, 0));
  assert.throws(() => parseAttachmentChunk(response, { ...expected, attachment_id: 'b'.repeat(64) }, 0));
});
