import { parseSnapshotOrigin, type SnapshotOrigin } from './history-model.ts';
/** Version 1 supplied-text wire contract. Text is never normalized implicitly. */
export const MAX_TEXT_BYTES = 1024 * 1024;
export const MAX_LINE_BYTES = 16 * 1024;
export const MAX_LINES = 100_000;
const encoder = new TextEncoder();
export const utf8Length = (value: string) => encoder.encode(value).byteLength;
export class DiffResponseError extends Error {}
const invalid = () => new DiffResponseError('The server returned an unsupported comparison response.');
export interface TextInfo { label: string; bytes: number; lines: number; crlf: number; lf: number; bare_cr: number; bom: boolean; final_newline: boolean }
export interface Comparison { origin?: SnapshotOrigin; schema_version: 1; comparison_id: string; expires_at: number; before: TextInfo; after: TextInfo; additions: number; deletions: number; equal: boolean; change_pages: number }
export type ScalarRange = [number, number];
export interface InlineChange { row_index: number; ranges: ScalarRange[] }
export interface Fragment { inline_changes: InlineChange[]; patch: string; before_start: number; before_count: number; after_start: number; after_count: number }
export type PageView = 'changes' | 'before' | 'after';
export interface DiffPage { schema_version: 1; comparison_id: string; view: PageView; page: number; total_pages: number; text?: string; fragments: Fragment[] }
export interface TextPair { before: string; after: string; before_label?: string; after_label?: string }
function object(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw invalid();
  return value as Record<string, unknown>;
}
function integer(value: unknown, maximum: number): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0 || value > maximum) throw invalid();
  return value;
}
function string(value: unknown, maximum: number): string {
  if (typeof value !== 'string' || value.length > maximum || utf8Length(value) > maximum) throw invalid();
  return value;
}
function boolean(value: unknown): boolean {
  if (typeof value !== 'boolean') throw invalid();
  return value;
}
function identity(value: unknown): string {
  if (typeof value !== 'string' || !/^[0-9a-f]{64}$/.test(value)) throw invalid();
  return value;
}
function data(result: unknown): Record<string, unknown> {
  const envelope = object(result);
  if (envelope.isError) throw new DiffResponseError('The comparison request could not be completed. It may have expired. Try again.');
  return object(envelope.structuredContent);
}
function textInfo(value: unknown): TextInfo {
  const info = object(value);
  const result = { label: string(info.label, 128), bytes: integer(info.bytes, MAX_TEXT_BYTES), lines: integer(info.lines, MAX_LINES), crlf: integer(info.crlf, MAX_LINES), lf: integer(info.lf, MAX_LINES), bare_cr: integer(info.bare_cr, MAX_TEXT_BYTES), bom: boolean(info.bom), final_newline: boolean(info.final_newline) };
  validateLabel(result.label);
  if (result.crlf + result.lf > result.lines) throw invalid();
  return result;
}
export function parseSummary(value: unknown): Comparison {
  const summary = object(value);
  if (summary.schema_version !== 1) throw invalid();
  const result: Comparison = { schema_version: 1, comparison_id: identity(summary.comparison_id), expires_at: integer(summary.expires_at, 253402300799), before: textInfo(summary.before), after: textInfo(summary.after), additions: integer(summary.additions, MAX_LINES), deletions: integer(summary.deletions, MAX_LINES), equal: boolean(summary.equal), change_pages: integer(summary.change_pages, MAX_LINES * 2) };
  if (result.additions > result.after.lines || result.deletions > result.before.lines || (result.equal && (result.additions || result.deletions || result.change_pages)) || (!result.equal && result.change_pages === 0)) throw invalid();
  if (summary.origin !== undefined && summary.origin !== null) {
    try { result.origin = parseSnapshotOrigin(summary.origin); } catch { throw invalid(); }
  }
  return result;
}
export function parseCompare(result: unknown): Comparison { return parseSummary(data(result)); }
export function parseShow(result: unknown): Comparison | null {
  const value = data(result);
  if (value.schema_version !== 1) throw invalid();
  return value.comparison === null ? null : parseSummary(value.comparison);
}
export function parseDelete(result: unknown): void {
  const value = data(result);
  if (value.schema_version !== 1 || value.deleted !== true) throw invalid();
}
interface PageBudget { rows: number; ranges: number; bytes: number }
function consumeBytes(budget: PageBudget, bytes: number): void {
  if (bytes > budget.bytes) throw invalid();
  budget.bytes -= bytes;
}
function parseFragment(value: unknown, budget: PageBudget): Fragment {
  if (budget.rows === 0 || budget.bytes === 0) throw invalid();
  const fragment = object(value);
  // Check the remaining page width before splitting or inspecting annotations.
  // Serialize only this bounded, whitelisted skeleton; never the raw envelope.
  const result: Fragment = { inline_changes: [], patch: string(fragment.patch, budget.bytes), before_start: integer(fragment.before_start, MAX_LINES), before_count: integer(fragment.before_count, budget.rows), after_start: integer(fragment.after_start, MAX_LINES), after_count: integer(fragment.after_count, budget.rows) };
  consumeBytes(budget, utf8Length(JSON.stringify(result)));
  const rows = patchRows(result, budget.rows);
  budget.rows -= rows.length;
  if (!Array.isArray(fragment.inline_changes) || fragment.inline_changes.length > rows.length) throw invalid();
  for (const value of fragment.inline_changes) {
    const entry = object(value);
    if (!Array.isArray(entry.ranges) || entry.ranges.length > budget.ranges) throw invalid();
    budget.ranges -= entry.ranges.length;
    const parsed: InlineChange = { row_index: integer(entry.row_index, 399), ranges: [] };
    consumeBytes(budget, utf8Length(JSON.stringify(parsed)) + (result.inline_changes.length ? 1 : 0));
    // Every range requires at least five JSON bytes plus its separator. Reject
    // impossible widths before walking even an otherwise valid range array.
    if (entry.ranges.length * 5 > budget.bytes) throw invalid();
    for (const value of entry.ranges) {
      if (!Array.isArray(value) || value.length !== 2) throw invalid();
      const range: ScalarRange = [integer(value[0], MAX_LINE_BYTES + 1), integer(value[1], MAX_LINE_BYTES + 1)];
      consumeBytes(budget, utf8Length(JSON.stringify(range)) + (parsed.ranges.length ? 1 : 0));
      parsed.ranges.push(range);
    }
    result.inline_changes.push(parsed);
  }
  validateInlineChanges(result, rows);
  return result;
}
export function parsePage(result: unknown, expected: { comparison_id: string; view: PageView; page: number }): DiffPage {
  const value = data(result);
  if (value.schema_version !== 1) throw invalid();
  const id = identity(value.comparison_id);
  if (id !== expected.comparison_id || value.view !== expected.view || value.page !== expected.page) throw invalid();
  const total = integer(value.total_pages, expected.view === 'changes' ? MAX_LINES * 2 : 33);
  const page = integer(value.page, MAX_LINES * 2);
  if (total === 0 || page >= total || !Array.isArray(value.fragments) || value.fragments.length > 400) throw invalid();
  const parsed: DiffPage = { schema_version: 1, comparison_id: id, view: expected.view, page, total_pages: total, fragments: [], ...(value.text === undefined ? {} : { text: string(value.text, 32 * 1024) }) };
  if (expected.view === 'changes') {
    if (value.text !== undefined || value.fragments.length === 0) throw invalid();
    const budget: PageBudget = { rows: 400, ranges: 4096, bytes: 256 * 1024 };
    consumeBytes(budget, utf8Length(JSON.stringify(parsed)));
    for (const fragment of value.fragments) {
      if (parsed.fragments.length) consumeBytes(budget, 1);
      parsed.fragments.push(parseFragment(fragment, budget));
    }
  } else if (value.fragments.length || typeof value.text !== 'string') throw invalid();
  if (utf8Length(JSON.stringify(parsed)) > 256 * 1024) throw invalid();
  return parsed;
}
export interface DiffRow {
  kind: ' ' | '+' | '-'; text: string; noFinalNewline: boolean;
  before?: number; after?: number; ranges: ScalarRange[];
}
/** Parse bounded patch rows and validate all Rust-provided scalar offsets. */
export function fragmentRows(fragment: Fragment): DiffRow[] {
  const rows = patchRows(fragment, 400);
  validateInlineChanges(fragment, rows);
  return rows;
}
function patchRows(fragment: Fragment, maximumRows: number): DiffRow[] {
  const lines = fragment.patch.split('\n');
  if (lines[0] !== '--- before' || lines[1] !== '+++ after' || lines.at(-1) !== '') throw invalid();
  const match = /^@@ -(\d+),(\d+) \+(\d+),(\d+) @@$/.exec(lines[2]);
  if (!match || Number(match[1]) !== fragment.before_start || Number(match[2]) !== fragment.before_count || Number(match[3]) !== fragment.after_start || Number(match[4]) !== fragment.after_count) throw invalid();
  if (fragment.before_start + Math.max(fragment.before_count - 1, 0) > MAX_LINES || fragment.after_start + Math.max(fragment.after_count - 1, 0) > MAX_LINES || (fragment.before_count > 0 && fragment.before_start === 0) || (fragment.after_count > 0 && fragment.after_start === 0)) throw invalid();
  let before = 0, after = 0;
  const rows: DiffRow[] = [];
  for (let index = 3; index < lines.length - 1; index++) {
    if (rows.length >= maximumRows) throw invalid();
    const line = lines[index];
    const kind = line[0];
    if (kind !== ' ' && kind !== '+' && kind !== '-') throw invalid();
    const noFinalNewline = lines[index + 1] === '\\ No newline at end of file';
    const text = line.slice(1) + (noFinalNewline ? '' : '\n');
    try { validateText(text); } catch { throw invalid(); }
    if (noFinalNewline) index++;
    rows.push({ kind, text, noFinalNewline, ...(kind === '+' ? {} : { before: ++before }), ...(kind === '-' ? {} : { after: ++after }), ranges: [] });
  }
  if (rows.length === 0 || before !== fragment.before_count || after !== fragment.after_count) throw invalid();
  return rows;
}
function validateInlineChanges(fragment: Fragment, rows: DiffRow[]): void {
  if (!Array.isArray(fragment.inline_changes)) throw invalid();
  let previousRow = -1, rangeCount = 0;
  for (const entry of fragment.inline_changes) {
    const row = rows[entry.row_index];
    if (!Number.isSafeInteger(entry.row_index) || entry.row_index <= previousRow || !row || row.kind === ' ' || !Array.isArray(entry.ranges)) throw invalid();
    previousRow = entry.row_index;
    const length = Array.from(row.text).length;
    let previousEnd = 0;
    for (const range of entry.ranges) {
      if (!Array.isArray(range) || range.length !== 2) throw invalid();
      const [start, end] = range;
      if (!Number.isSafeInteger(start) || !Number.isSafeInteger(end) || start < previousEnd || start < 0 || end <= start || end > length || ++rangeCount > 4096) throw invalid();
      previousEnd = end;
    }
    row.ranges = entry.ranges;
  }
  if (fragment.inline_changes.length !== rows.filter(row => row.kind !== ' ').length) throw invalid();
}
/** Apply authoritative scalar ranges without comparing either source. */
export function scalarSegments(text: string, ranges: ScalarRange[]): { text: string; changed: boolean }[] {
  const scalars = Array.from(text);
  const segments: { text: string; changed: boolean }[] = [];
  let offset = 0;
  for (const [start, end] of ranges) {
    if (start > offset) segments.push({ text: scalars.slice(offset, start).join(''), changed: false });
    segments.push({ text: scalars.slice(start, end).join(''), changed: true });
    offset = end;
  }
  if (offset < scalars.length) segments.push({ text: scalars.slice(offset).join(''), changed: false });
  return segments;
}
/** Adjacent changed rows are paired in source order for split presentation only. */
export function splitRows(rows: DiffRow[]): { before?: DiffRow; after?: DiffRow }[] {
  const result: { before?: DiffRow; after?: DiffRow }[] = [];
  for (let index = 0; index < rows.length;) {
    if (rows[index].kind === ' ') { result.push({ before: rows[index], after: rows[index] }); index++; continue; }
    const before: DiffRow[] = [], after: DiffRow[] = [];
    while (index < rows.length && rows[index].kind !== ' ') {
      const row = rows[index++];
      (row.kind === '-' ? before : after).push(row);
    }
    for (let offset = 0; offset < Math.max(before.length, after.length); offset++) result.push({ before: before[offset], after: after[offset] });
  }
  return result;
}
export function sourceRange(start: number, count: number): string { return count ? `${start}–${start + count - 1}` : `none (after ${start})`; }
export function validateText(value: string): void {
  if (utf8Length(value) > MAX_TEXT_BYTES) throw new Error('Each text must fit within 1 MiB of UTF-8.');
  if (value.includes('\0')) throw new Error('Text containing NUL is not supported.');
  for (let index = 0; index < value.length; index++) {
    const code = value.charCodeAt(index);
    if (code >= 0xd800 && code <= 0xdbff) {
      const next = value.charCodeAt(++index);
      if (!(next >= 0xdc00 && next <= 0xdfff)) throw new Error('Text must contain valid Unicode.');
    } else if (code >= 0xdc00 && code <= 0xdfff) throw new Error('Text must contain valid Unicode.');
  }
  const lines = value.split('\n');
  if (lines.length - (value.endsWith('\n') || value === '' ? 1 : 0) > MAX_LINES) throw new Error('Each text may contain at most 100,000 lines.');
  if (lines.some(line => utf8Length(line) > MAX_LINE_BYTES)) throw new Error('Each line must fit within 16 KiB of UTF-8.');
}
export function validateLabel(value: string): void {
  if (!value || utf8Length(value) > 128 || /[\u0000-\u001f\u007f-\u009f]/u.test(value)) throw new Error('Labels must contain 1–128 UTF-8 bytes and contain no control characters.');
}
export function decodeFile(buffer: ArrayBuffer): string {
  if (buffer.byteLength > MAX_TEXT_BYTES) throw new Error('Each file must fit within 1 MiB of UTF-8.');
  let text: string;
  try { text = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true }).decode(buffer); }
  catch { throw new Error('Choose a UTF-8 text file; invalid byte sequences are not replaced.'); }
  validateText(text);
  return text;
}
export function parsePair(value: unknown): TextPair | null {
  const input = object(value);
  if (input.before === undefined && input.after === undefined) return null;
  if (typeof input.before !== 'string' || typeof input.after !== 'string') throw invalid();
  validateText(input.before); validateText(input.after);
  const labels: { before_label?: string; after_label?: string } = {};
  for (const key of ['before_label', 'after_label'] as const) if (input[key] !== undefined && input[key] !== null) { labels[key] = string(input[key], 128); validateLabel(labels[key]!); }
  return { before: input.before, after: input.after, ...labels };
}
export const editAsLf = (text: string) => text.replace(/\r\n?/g, '\n');
export function metadata(info: TextInfo): string {
  return `${info.bytes.toLocaleString()} UTF-8 bytes · ${info.lines.toLocaleString()} lines · LF ${info.lf}, CRLF ${info.crlf}, bare CR ${info.bare_cr} · ${info.bom ? 'BOM present' : 'no BOM'} · ${info.final_newline ? 'final newline' : 'no final newline'}`;
}

export type Attachment = { attachment_id: string; kind: 'text' | 'patch'; total_bytes: number; committed_bytes: number; sealed: boolean; expires_at: number };
export function parseAttachment(value: unknown): Attachment {
  const item = object(value);
  if (item.schema_version !== 1 || (item.kind !== 'text' && item.kind !== 'patch')) throw new DiffResponseError('The server returned an invalid attachment.');
  const total = integer(item.total_bytes, item.kind === 'text' ? MAX_TEXT_BYTES : 8 * MAX_TEXT_BYTES);
  const committed = integer(item.committed_bytes, total);
  const sealed = boolean(item.sealed);
  if (sealed && committed !== total) throw new DiffResponseError('The server returned an incomplete attachment.');
  return { attachment_id: identity(item.attachment_id), kind: item.kind, total_bytes: total, committed_bytes: committed, sealed, expires_at: integer(item.expires_at, Number.MAX_SAFE_INTEGER) };
}
export function parseAttachmentUpload(result: unknown): Attachment { return parseAttachment(data(result)); }
export function parsePatchResult(result: unknown): Attachment {
  const value = data(result);
  if (value.schema_version !== 1) throw new DiffResponseError('The patch result version is unsupported.');
  const attachment = parseAttachment(value.result);
  if (attachment.kind !== 'text' || !attachment.sealed) throw new DiffResponseError('The patch result is not complete text.');
  return attachment;
}
export function parseAttachmentChunk(result: unknown, expected: Attachment, offset: number): { text: string; next_offset: number; complete: boolean } {
  const value = data(result);
  const attachment = parseAttachment(value.attachment);
  if (value.schema_version !== 1 || attachment.attachment_id !== expected.attachment_id || attachment.kind !== expected.kind || attachment.total_bytes !== expected.total_bytes || attachment.expires_at !== expected.expires_at || !attachment.sealed || value.offset !== offset) throw new DiffResponseError('The server returned inconsistent attachment data.');
  const text = string(value.text, 32 * 1024);
  const next = integer(value.next_offset, attachment.total_bytes);
  const complete = boolean(value.complete);
  if (next !== offset + utf8Length(text) || complete !== (next === attachment.total_bytes) || (!complete && next <= offset)) throw new DiffResponseError('The server returned an invalid attachment chunk.');
  return { text, next_offset: next, complete };
}
export function* attachmentChunks(text: string): Generator<{ offset: number; chunk: string; final: boolean }> {
  let chunk = ''; let bytes = 0; let offset = 0;
  for (const scalar of text) {
    const point = scalar.codePointAt(0)!;
    if (point >= 0xd800 && point <= 0xdfff) throw new Error('Text must contain valid Unicode.');
    const size = point <= 0x7f ? 1 : point <= 0x7ff ? 2 : point <= 0xffff ? 3 : 4;
    if (bytes + size > 32 * 1024) { yield { offset, chunk, final: false }; offset += bytes; chunk = ''; bytes = 0; }
    chunk += scalar; bytes += size;
  }
  yield { offset, chunk, final: true };
}
export function decodePatchFile(buffer: ArrayBuffer): string {
  if (buffer.byteLength > 8 * MAX_TEXT_BYTES) throw new Error('The patch must fit within 8 MiB.');
  const value = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true }).decode(buffer);
  if (value.includes('\0')) throw new Error('Patches cannot contain NUL.');
  return value;
}

export function validatePatch(text: string): void {
  if (utf8Length(text) > 8 * MAX_TEXT_BYTES || text.includes('\0')) throw new Error('The patch must fit within 8 MiB and contain no NUL.');
  for (const scalar of text) { const code = scalar.codePointAt(0)!; if (code >= 0xd800 && code <= 0xdfff) throw new Error('The patch must contain valid Unicode.'); }
}
