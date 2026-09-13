/** Version 1 supplied-text wire contract. Text is never normalized implicitly. */
export const MAX_TEXT_BYTES = 1024 * 1024;
export const MAX_LINE_BYTES = 16 * 1024;
export const MAX_LINES = 100_000;
const encoder = new TextEncoder();
export const utf8Length = (value: string) => encoder.encode(value).byteLength;
export class DiffResponseError extends Error {}
const invalid = () => new DiffResponseError('The server returned an unsupported comparison response.');
export interface TextInfo { label: string; bytes: number; lines: number; crlf: number; lf: number; bare_cr: number; bom: boolean; final_newline: boolean }
export interface Comparison { schema_version: 1; comparison_id: string; expires_at: number; before: TextInfo; after: TextInfo; additions: number; deletions: number; equal: boolean; change_pages: number }
export interface Fragment { patch: string; before_start: number; before_count: number; after_start: number; after_count: number }
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
function parseFragment(value: unknown): Fragment {
  const fragment = object(value);
  const result: Fragment = { patch: string(fragment.patch, 256 * 1024), before_start: integer(fragment.before_start, MAX_LINES), before_count: integer(fragment.before_count, 400), after_start: integer(fragment.after_start, MAX_LINES), after_count: integer(fragment.after_count, 400) };
  validateFragment(result);
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
  const fragments = value.fragments.map(parseFragment);
  if (expected.view === 'changes') {
    if (value.text !== undefined || fragments.length === 0 || fragments.reduce((count, fragment) => count + validateFragment(fragment).rows, 0) > 400) throw invalid();
  } else if (fragments.length || typeof value.text !== 'string') throw invalid();
  const parsed: DiffPage = { schema_version: 1, comparison_id: id, view: expected.view, page, total_pages: total, fragments, ...(value.text === undefined ? {} : { text: string(value.text, 32 * 1024) }) };
  if (utf8Length(JSON.stringify(parsed)) > 256 * 1024) throw invalid();
  return parsed;
}
/** Validate the server's single-hunk fragment before the library allocates its display. */
function validateFragment(fragment: Fragment): { headerIndex: number; rows: number } {
  const lines = fragment.patch.split('\n');
  const headerIndex = lines.findIndex(line => /^@@ /.test(line));
  if (headerIndex < 2 || !lines.slice(0, headerIndex).some(line => line.startsWith('--- ')) || !lines.slice(0, headerIndex).some(line => line.startsWith('+++ '))) throw invalid();
  const match = /^@@ -(\d+),(\d+) \+(\d+),(\d+) @@(?:.*)$/.exec(lines[headerIndex]);
  if (!match || Number(match[1]) !== fragment.before_start || Number(match[2]) !== fragment.before_count || Number(match[3]) !== fragment.after_start || Number(match[4]) !== fragment.after_count) throw invalid();
  if (fragment.before_start + Math.max(fragment.before_count - 1, 0) > MAX_LINES || fragment.after_start + Math.max(fragment.after_count - 1, 0) > MAX_LINES) throw invalid();
  let before = 0, after = 0, rows = 0, previousData = false;
  for (let index = headerIndex + 1; index < lines.length; index++) {
    const line = lines[index];
    if (index === lines.length - 1 && line === '') break;
    if (line === '\\ No newline at end of file') {
      if (!previousData) throw invalid();
      previousData = false;
      continue;
    }
    if (![' ', '+', '-'].includes(line[0]) || utf8Length(line.slice(1)) > MAX_LINE_BYTES) throw invalid();
    if (line[0] !== '+') before++;
    if (line[0] !== '-') after++;
    rows++;
    previousData = true;
  }
  if (rows === 0 || rows > 400 || before !== fragment.before_count || after !== fragment.after_count) throw invalid();
  return { headerIndex, rows };
}
/** Only display positions change: the patch's data lines remain byte-for-byte intact. */
export function localPatch(fragment: Fragment): string {
  const { headerIndex } = validateFragment(fragment);
  const lines = fragment.patch.split('\n');
  lines[headerIndex] = `@@ -${fragment.before_count ? 1 : 0},${fragment.before_count} +${fragment.after_count ? 1 : 0},${fragment.after_count} @@`;
  return lines.join('\n');
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
