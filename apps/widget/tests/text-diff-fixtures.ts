import type { InlineChange, Comparison, DiffPage, Fragment, TextInfo } from '../src/text-diff-model.ts';
export const fixtureId = 'b'.repeat(64);
export function info(text: string, label: string): TextInfo {
  const crlf = text.match(/\r\n/g)?.length ?? 0;
  return { label, bytes: new TextEncoder().encode(text).byteLength, lines: text === '' ? 0 : text.split('\n').length - (text.endsWith('\n') ? 1 : 0), crlf, lf: (text.match(/\n/g)?.length ?? 0) - crlf, bare_cr: (text.match(/\r/g)?.length ?? 0) - crlf, bom: text.startsWith('\uFEFF'), final_newline: text.endsWith('\n') };
}
export function fixtureSummary(before: string, after: string, id = fixtureId): Comparison {
  const equal = before === after;
  return { schema_version: 1, comparison_id: id, expires_at: Math.floor(Date.now() / 1000) + 600, before: info(before, 'Before'), after: info(after, 'After'), additions: equal ? 0 : info(after, '').lines, deletions: equal ? 0 : info(before, '').lines, equal, change_pages: equal ? 0 : 1 };
}
export function fixtureFragment(before: string, after: string, beforeStart = 1, afterStart = 1): Fragment {
  const beforeCount = info(before, '').lines, afterCount = info(after, '').lines;
  const startBefore = beforeCount ? beforeStart : 0, startAfter = afterCount ? afterStart : 0;
  const patchLines = (text: string, sign: string) => {
    if (text === '') return '';
    const lines = text.split('\n');
    if (text.endsWith('\n')) lines.pop();
    return lines.map((line, index) => `${sign}${line}\n${index === lines.length - 1 && !text.endsWith('\n') ? '\\ No newline at end of file\n' : ''}`).join('');
  };
  // Synthetic server metadata: every changed source scalar is marked, with no
  // client diff calculation. Dedicated fixtures supply narrower explicit ranges.
  const inline_changes: InlineChange[] = [];
  for (const text of [before, after]) {
    const lines = text.match(/[^\n]*\n|[^\n]+$/g) ?? [];
    for (const line of lines) inline_changes.push({ row_index: inline_changes.length, ranges: [[0, Array.from(line).length]] });
  }
  return { inline_changes, patch: `--- before\n+++ after\n@@ -${startBefore},${beforeCount} +${startAfter},${afterCount} @@\n${patchLines(before, '-')}${patchLines(after, '+')}`, before_start: startBefore, before_count: beforeCount, after_start: startAfter, after_count: afterCount };
}
export function fixturePage(before: string, after: string): DiffPage {
  return { schema_version: 1, comparison_id: fixtureId, view: 'changes', page: 0, total_pages: 1, fragments: [fixtureFragment(before, after)] };
}
