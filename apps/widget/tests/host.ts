import { AppBridge, PostMessageTransport } from '@modelcontextprotocol/ext-apps/app-bridge';
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
bridge.oncalltool = async ({ name, arguments: args }) => {
  const input = args ?? {};
  const calls = JSON.parse(document.getElementById('calls')!.textContent || '[]');
  calls.push({ name, arguments: input });
  document.getElementById('calls')!.textContent = JSON.stringify(calls);
  if (input.query === 'slow') await new Promise(resolve => setTimeout(resolve, 250));
  if (input.query === 'error') return { isError: true, content: [{ type: 'text', text: 'Private internal failure' }] };
  if (input.query === 'malformed') return { content: [], structuredContent: { data: { records: 'invalid' } } };
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
  await bridge.sendToolResult({ content: [], structuredContent: params.has('malformed') ? { synthetic: true, records: [null] } : { synthetic: true, records: params.has('initial') ? [envelope(records[0], true), envelope({ ...records[0], source: 'layout_b' })] : [] } });
};
if (!params.has('disconnected')) await bridge.connect(new PostMessageTransport(iframe.contentWindow!, iframe.contentWindow!));
iframe.src = `/widget${params.has('invalid-source') ? '?invalid-source' : params.has('duplicate-source') ? '?duplicate-source' : ''}`;
