/** Entirely fictional corpus responses for local MCP Apps bridge verification. */
import { AppBridge, PostMessageTransport } from '@modelcontextprotocol/ext-apps/app-bridge';
import { fixtureSummary, fixtureFragment } from './text-diff-fixtures.ts';
const frame = document.querySelector('iframe')!;
const bridge = new AppBridge(null, { name: 'Fictional database test host', version: '1' }, { serverTools: {}, openLinks: {} });
const object = { jurisdiction: 'kr', provider: 'fixture', dataset: 'national_statute', id: 'fictional-1' };
const firstId = 'a'.repeat(64); const secondId = 'b'.repeat(64);
const now = Math.floor(Date.now() / 1000);
function metadata(selector: Record<string, unknown>) { const second = selector.id === 'r2' || selector.id === secondId; return { object, revision_id: second ? 'r2' : 'r1', capture_id: second ? secondId : firstId, title: 'Fictional sample statute', metadata: { fictional: 'true' }, publication_date: null, effective_date: null, source_url: 'https://fixture.example/fictional', retrieved_at: now - 30, captured_at: now - 20, validated_at: now - 10, processor_version: 'fictional-v1', raw_sha256: 'c'.repeat(64), freshness: selector.kind === 'head' ? { state: 'fresh', served_at: now, cached_at: now - 20, age_seconds: 10, fresh_ttl_seconds: 300, fresh_remaining_seconds: 290 } : null }; }
const summary = fixtureSummary('old\n', 'new\n', 'd'.repeat(64));
const ok = (structuredContent: object) => ({ content: [], structuredContent });
bridge.onopenlink = async () => ({ isError: false });
bridge.oncalltool = async ({ name, arguments: args }) => {
  const input = args ?? {}; const log = document.getElementById('calls')!;
  log.textContent = JSON.stringify([...JSON.parse(log.textContent || '[]'), { name, arguments: input }]);
  if (name === 'database.query' || name === 'database.rg') return ok({ schema_version: 1, hits: [{ object, revision_id: 'r1', capture_id: firstId, title: 'Fictional sample statute', match_scope: name === 'database.query' ? 'object' : 'line', excerpt_section: 'body', includes_ocr: false, section: name === 'database.query' ? 'object' : 'body', line: name === 'database.query' ? 0 : 1, text: '<b>fictional evidence</b>', byte_start: 0, byte_end: 10, derived_ocr: false }], next_cursor: null, generation: 7, corpus_complete: false, scanned_bytes: 10, analyzer_version: 'fixture', index_lag: 2 });
  if (name === 'database.get') return ok({ session: input.session ?? 'e'.repeat(64), schema_version: 1, metadata: metadata(input.selector as Record<string, unknown>), section: input.section, text: input.section !== 'body' ? 'Fictional section content' : input.offset ? 'second fictional page' : 'first fictional page', offset: input.offset, next_offset: input.section !== 'body' || input.offset ? null : new TextEncoder().encode('first fictional page').length, section_count: 2, next_sections_offset: input.sections_offset ? null : 1, sections: [{ id: input.sections_offset ? 'x'.repeat(256) : 'first', title: input.sections_offset ? 'Second fictional section' : 'First fictional section', kind: 'provider_text', bytes: 25 }] });
  if (name === 'database.history') return ok({ entries: ['r1', 'r2'].map((revision_id, index) => ({ revision_id, capture_id: input.kind === 'captures' ? (index ? secondId : firstId) : null, sequence: index + 1, captured_at: input.kind === 'captures' ? now - 20 : null, publication_date: null, effective_date: null })), next_cursor: null, inventory_complete: false });
  if (name === 'database.diff') return ok({ schema_version: 1, before: metadata(input.before as Record<string, unknown>), after: metadata(input.after as Record<string, unknown>), comparison: summary, includes_ocr: false });
  if (name === 'text.diff.page') return ok({ schema_version: 1, comparison_id: summary.comparison_id, view: 'changes', page: 0, total_pages: 1, fragments: [fixtureFragment('old\n', 'new\n')] });
  if (name === 'text.diff.delete') return ok({ schema_version: 1, deleted: true });
  return { isError: true, content: [{ type: 'text' as const, text: 'Fictional unavailable operation' }] };
};
await bridge.connect(new PostMessageTransport(frame.contentWindow!, frame.contentWindow!));
frame.src = '/database-widget';
