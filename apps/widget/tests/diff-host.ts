/** Local-only bridge fixtures. No tool call leaves this browser harness. */
import { AppBridge, PostMessageTransport } from '@modelcontextprotocol/ext-apps/app-bridge';
import { fixtureFragment, fixtureId, fixtureSummary } from './text-diff-fixtures.ts';
import type { Comparison, DiffPage } from '../src/text-diff-model.ts';
const iframe = document.querySelector('iframe')!;
const params = new URLSearchParams(location.search);
const bridge = new AppBridge(null, { name: 'Offline comparison host', version: '1.0.0' }, { serverTools: {}, openLinks: {} });
const sourceBefore = '가\r\nOriginal before\r끝';
const sourceAfter = '😀\r\nOriginal after\n';
const records = new Map<string, { before: string; after: string; summary: Comparison }>();
let counter = 0, deletions = 0;
function retain(before: string, after: string, id = (++counter).toString(16).padStart(64, '0')) {
  const summary = fixtureSummary(before, after, id);
  records.set(id, { before, after, summary });
  return summary;
}
function result(structuredContent: object) { return { content: [], structuredContent }; }
function failure() { return { isError: true, content: [{ type: 'text' as const, text: 'Private internal failure with input text' }] }; }
bridge.onopenlink = async ({ url }) => {
  document.getElementById('links')!.textContent = JSON.stringify([url]);
  return { isError: false };
};
bridge.oncalltool = async ({ name, arguments: args }) => {
  const input = args ?? {};
  const calls = JSON.parse(document.getElementById('calls')!.textContent || '[]');
  calls.push({ name, arguments: input });
  document.getElementById('calls')!.textContent = JSON.stringify(calls);
  if (name === 'compare_texts') {
    if (input.before === 'slow') await new Promise(resolve => setTimeout(resolve, 350));
    if (input.before === 'error') return failure();
    const summary = retain(String(input.before), String(input.after));
    summary.before.label = String(input.before_label ?? 'Before');
    summary.after.label = String(input.after_label ?? 'After');
    return result(input.before === 'malformed' ? { schema_version: 9 } : summary);
  }
  if (name === 'delete_text_diff') {
    if (params.has('deletefail') && deletions++ === 0) return failure();
    records.delete(String(input.comparison_id));
    return result({ schema_version: 1, deleted: true });
  }
  if (name === 'get_text_diff_page') {
    if (params.has('slowpage')) await new Promise(resolve => setTimeout(resolve, 400));
    const record = records.get(String(input.comparison_id));
    if (!record) return failure();
    const view = input.view as DiffPage['view'];
    const page = Number(input.page);
    const output: DiffPage = { schema_version: 1, comparison_id: record.summary.comparison_id, view, page, total_pages: 1, fragments: [] };
    if (view === 'changes') {
      output.fragments = [fixtureFragment(record.before, record.after)];
      if (params.has('pages')) {
        output.total_pages = 2;
        output.fragments = [fixtureFragment(`old ${page}\n`, `new ${page}\n`, page === 0 ? 99998 : 100000, page === 0 ? 99998 : 100000)];
      }
      if (params.has('highlights')) output.fragments[0].inline_changes = [{ row_index: 0, ranges: [[0, 1], [7, 9]] }, { row_index: 1, ranges: [] }];
      if (params.has('missinghighlights')) delete (output.fragments[0] as Partial<typeof output.fragments[0]>).inline_changes;
      if (params.has('badhighlights')) output.fragments[0].inline_changes[0].ranges = [[0, 20000]];
      if (params.has('badpage')) output.comparison_id = 'c'.repeat(64);
    } else {
      const raw = record[view];
      // Small deterministic chunks test ordered reconstruction, including retained CR/BOM.
      const middle = Math.floor(raw.length / 2);
      const boundary = raw.charCodeAt(middle - 1) >= 0xd800 && raw.charCodeAt(middle - 1) <= 0xdbff ? middle + 1 : middle;
      const chunks = raw.length > 1 ? [raw.slice(0, boundary), raw.slice(boundary)] : [raw];
      output.total_pages = chunks.length;
      output.text = chunks[page] ?? '';
    }
    return result(output);
  }
  return failure();
};
bridge.oninitialized = async () => {
  if (params.has('initial') || params.has('pair') || params.has('pages') || params.has('expired')) {
    const before = params.has('highlights') ? '😀가e\u0301漢字A\r\n' : params.has('pair') ? '<b>before</b>\n' : sourceBefore;
    const after = params.has('highlights') ? '😀나e\u0301漢語B' : params.has('pair') ? '<img src="https://evil.example/x">\n' : sourceAfter;
    const summary = retain(before, after, fixtureId);
    if (params.has('pages')) { summary.before.lines = 100000; summary.after.lines = 100000; summary.before.bytes = 200000; summary.after.bytes = 200000; summary.change_pages = 2; }
    if (params.has('expired')) summary.expires_at = Math.floor(Date.now() / 1000) - 1;
    await bridge.sendToolInput({ arguments: params.has('pair') ? { before, after } : { comparison_id: fixtureId } });
    await bridge.sendToolResult(result({ schema_version: 1, comparison: summary }));
  } else {
    await bridge.sendToolInput({ arguments: {} });
    await bridge.sendToolResult(result(params.has('malformed') ? { comparison: null } : { schema_version: 1, comparison: null }));
  }
};
window.addEventListener('cancel-comparison', () => { void bridge.sendToolCancelled({ reason: 'Fixture cancellation' }); });
if (!params.has('disconnected')) await bridge.connect(new PostMessageTransport(iframe.contentWindow!, iframe.contentWindow!));
iframe.src = '/diff-widget';
