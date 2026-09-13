import { AppBridge, PostMessageTransport } from '@modelcontextprotocol/ext-apps/app-bridge';
import { fixtureFragment, fixtureSummary } from './text-diff-fixtures.ts';
import type { HistoryQuery, SnapshotSummary } from '../src/history-model.ts';
import type { Comparison } from '../src/text-diff-model.ts';
const iframe = document.querySelector('iframe')!;
const records = Array.from({ length: 7 }, (_, index) => ({ source: 'layout_a', id: `demo-${index + 1}`, title: `Synthetic record ${index + 1}`, body: `Synthetic body ${index + 1}. <b>Plain source text</b>`, synthetic: true }));
function envelope(data: unknown, stale = false) { return { data, provenance: { provider: 'synthetic', dataset: 'records', source_reference: 'synthetic:fixture', payload_sha256: 'a'.repeat(64), processor_version: '1.0.0', retrieved_at: 100, validated_at: 100 }, freshness: { state: stale ? 'stale' : 'fresh', age_seconds: stale ? 70 : 0 }, synthetic: true }; }
const params = new URLSearchParams(location.search);
const bridge = new AppBridge(null, { name: 'Offline test host', version: '1.0.0' }, { serverTools: {}, ...(params.has('unsupported') ? {} : { openLinks: {} }) });
if (!params.has('unsupported')) bridge.onopenlink = async ({ url }) => {
  const links = JSON.parse(document.getElementById('links')!.textContent || '[]');
  links.push(url);
  document.getElementById('links')!.textContent = JSON.stringify(links);
  if (params.has('throws')) throw new Error('Synthetic host navigation failure');
  return { isError: params.has('denied') };
};
const summary = (sequence: number): SnapshotSummary => ({ snapshot_id: String(sequence).repeat(64), sequence, captured_at: sequence * 100, processor_version: sequence === 3 ? '1.0.0' : '0.9.0', schema_version: 1, payload_sha256: String(sequence).repeat(64) });
const historicRecord = (sequence: number, source: string, id = 'demo-1') => ({ source, id, title: `Historical title ${sequence}`, body: `Snapshot body ${sequence}. <b>Exact text</b>`, synthetic: true });
const original = (sequence: number) => { const record = historicRecord(sequence, 'layout_a'); return record.title + '\n\n' + record.body; };
const comparisons = new Map<string, { before: string; after: string; summary: Comparison }>();
let created = 0, deletions = 0;
bridge.oncalltool = async ({ name, arguments: args }) => {
  const input = args ?? {};
  const calls = JSON.parse(document.getElementById('calls')!.textContent || '[]');
  calls.push({ name, arguments: input });
  document.getElementById('calls')!.textContent = JSON.stringify(calls);
  if (input.query === 'slow') await new Promise(resolve => setTimeout(resolve, 250));
  if (input.query === 'error') return { isError: true, content: [{ type: 'text', text: 'Private internal failure' }] };
  if (input.query === 'malformed') return { content: [], structuredContent: { data: { records: 'invalid' } } };
  if (name === 'demo_list_snapshots') {
    if (params.has('slowhistory')) await new Promise(resolve => setTimeout(resolve, 350));
    if (params.has('historyerror')) return { isError: true, content: [{ type: 'text', text: 'Private disk path' }] };
    return { content: [], structuredContent: { snapshots: input.cursor ? [summary(1)] : [summary(3), summary(2)], next_cursor: input.cursor ? null : 'older', synthetic: true } };
  }
  if (name === 'demo_get_snapshot') {
    const query = input.query as HistoryQuery, sequence = Number(String(input.snapshot_id)[0]);
    const item = summary(sequence), record = historicRecord(sequence, query.source, query.operation === 'get' ? query.id : undefined);
    return { content: [], structuredContent: { schema_version: 1, snapshot: item, query: params.has('badhistory') ? { ...query, source: 'layout_b' } : query, data: query.operation === 'get' ? record : { records: Array.from({ length: Math.min(query.page_size, Math.max(0, 7 - query.page * query.page_size)) }, (_, index) => ({ ...record, id: `demo-${index + 1}`, title: index === 0 ? record.title : `${record.title} (${index + 1})` })), page: query.page, page_size: query.page_size, total: 7 }, provenance: { ...envelope(record).provenance, processor_version: item.processor_version, payload_sha256: item.payload_sha256 }, historical: true, synthetic: true, clock_anomaly: false } };
  }
  if (name === 'demo_compare_record_snapshots') {
    if (params.has('slowcompare')) await new Promise(resolve => setTimeout(resolve, 500));
    const beforeSnapshot = summary(Number(String(input.before_snapshot_id)[0])), afterSnapshot = summary(Number(String(input.after_snapshot_id)[0]));
    const before = original(beforeSnapshot.sequence), after = original(afterSnapshot.sequence);
    const value: Comparison = { ...fixtureSummary(before, after, (++created).toString(16).padStart(64, '0')), origin: { source: params.has('badorigin') ? 'layout_b' : input.source as 'layout_a', record_id: String(input.id), before: beforeSnapshot, after: afterSnapshot, projection: 'title_lf_lf_body_v1' } };
    if (params.has('expiredcompare')) value.expires_at = Math.floor(Date.now() / 1000) - 1;
    comparisons.set(value.comparison_id, { before, after, summary: value });
    return { content: [], structuredContent: value };
  }
  if (name === 'delete_text_diff') {
    if (params.has('deletefail') && deletions++ === 0) return { isError: true, content: [] };
    comparisons.delete(String(input.comparison_id)); return { content: [], structuredContent: { schema_version: 1, deleted: true } };
  }
  if (name === 'get_text_diff_page') {
    if (params.has('slowpage')) await new Promise(resolve => setTimeout(resolve, 300));
    const item = comparisons.get(String(input.comparison_id));
    if (!item) return { isError: true, content: [] };
    return { content: [], structuredContent: { schema_version: 1, comparison_id: input.comparison_id, view: input.view, page: 0, total_pages: 1, fragments: input.view === 'changes' ? [fixtureFragment(item.before, item.after)] : [], ...(input.view === 'changes' ? {} : { text: input.view === 'before' ? item.before : item.after }) } };
  }
  if (name === 'demo_search_records') {
    const page = Number(input.page);
    const filtered = input.query === 'empty' ? [] : records;
    return { content: [], structuredContent: envelope({ records: filtered.slice(page * 5, page * 5 + 5).map(record => ({ ...record, source: input.source })), page, page_size: 5, total: filtered.length }, input.query === 'stale') };
  }
  if (name === 'demo_get_record') {
    const record = records.find(record => record.id === input.id);
    return { content: [], structuredContent: envelope({ ...record, source: input.source }) };
  }
  return { isError: true, content: [] };
};
bridge.oninitialized = async () => {
  await bridge.sendToolInput({ arguments: { records: [] } });
  await bridge.sendToolResult({ content: [], structuredContent: params.has('malformed') ? { synthetic: true, records: [null] } : { synthetic: true, ...(params.has('history') ? { capabilities: { history: true, comparison: !params.has('nocompare'), processor_versions: { layout_a: '1.0.0', layout_b: '1.0.0' } } } : {}), records: params.has('initial') ? [envelope(records[0], true), envelope({ ...records[0], source: 'layout_b' })] : [] } });
};
window.addEventListener('cancel-history', () => { void bridge.sendToolCancelled({ reason: 'Synthetic cancellation' }); });
if (!params.has('disconnected')) await bridge.connect(new PostMessageTransport(iframe.contentWindow!, iframe.contentWindow!));
iframe.src = `/widget${params.has('invalid-source') ? '?invalid-source' : params.has('duplicate-source') ? '?duplicate-source' : ''}`;
