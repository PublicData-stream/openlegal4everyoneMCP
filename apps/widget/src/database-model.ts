/** Bounded wire validation for the legal database browser. Fixture data is explicitly synthetic. */
export type ObjectId = { jurisdiction: string; provider: string; dataset: 'national_statute' | 'ordinance' | 'precedent'; id: string };
export type Selector = { kind: 'head' } | { kind: 'revision' | 'capture'; id: string };
export type Hit = { object: ObjectId; revision_id: string; capture_id: string; title: string; section: string; line: number; text: string; derived_ocr: boolean; match_scope: 'line' | 'object'; excerpt_section: string; includes_ocr: boolean };
export type SearchPage = { hits: Hit[]; next_cursor: string | null; generation: number; corpus_complete: boolean; index_lag: number };
export type HistoryEntry = { revision_id: string; capture_id: string | null; captured_at: number | null; sequence: number; publication_date: string | null; effective_date: string | null };
const bytes = (value: string) => new TextEncoder().encode(value).length;
export function object(value: unknown): Record<string, unknown> { if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('The server returned an invalid database response.'); return value as Record<string, unknown>; }
export function text(value: unknown, max = 16384): string { if (typeof value !== 'string' || value.length > max || bytes(value) > max) throw new Error('The server returned invalid or excessive text.'); return value; }
export function number(value: unknown): number { if (!Number.isSafeInteger(value) || Number(value) < 0) throw new Error('The server returned an invalid counter.'); return Number(value); }
function boolean(value: unknown): boolean { if (typeof value !== 'boolean') throw new Error('The server returned an invalid status.'); return value; }
function optionalText(value: unknown, max: number): string | null { return value === null || value === undefined ? null : text(value, max); }
function hash(value: unknown): string { const result = text(value, 64); if (!/^[0-9a-f]{64}$/.test(result)) throw new Error('The server returned an invalid content identifier.'); return result; }
export function data(result: unknown): Record<string, unknown> { const response = object(result); if (response.isError) throw new Error('The database operation failed. Data may be unavailable, incomplete or expired.'); const value = object(response.structuredContent); if (bytes(JSON.stringify(value)) > 2 * 1024 * 1024) throw new Error('The response exceeds the browser display budget.'); return value; }
export function identity(value: unknown): ObjectId {
  const id = object(value); const dataset = text(id.dataset, 32);
  if (!['national_statute', 'ordinance', 'precedent'].includes(dataset)) throw new Error('Unsupported dataset.');
  const result = { jurisdiction: text(id.jurisdiction, 32), provider: text(id.provider, 64), dataset: dataset as ObjectId['dataset'], id: text(id.id, 128) };
  if (![result.jurisdiction, result.provider, result.id].every(s => /^[A-Za-z0-9_-]+$/.test(s))) throw new Error('The server returned an invalid object identity.');
  return result;
}
export function sameObject(a: ObjectId, b: ObjectId): boolean { return a.jurisdiction === b.jurisdiction && a.provider === b.provider && a.dataset === b.dataset && a.id === b.id; }
export function searchPage(result: unknown): SearchPage {
  const value = data(result);
  if (value.schema_version !== 1 || !Array.isArray(value.hits) || value.hits.length > 20) throw new Error('The search page is unsupported.');
  return { hits: value.hits.map(raw => { const h = object(raw); const match_scope = text(h.match_scope, 8); const section = text(h.section, 256); const line = number(h.line); const excerpt_section = text(h.excerpt_section, 256); if (match_scope !== 'line' && match_scope !== 'object' || match_scope === 'object' && (section !== 'object' || line !== 0) || match_scope === 'line' && (line === 0 || section !== excerpt_section)) throw new Error('The search evidence scope is invalid.'); return { match_scope, excerpt_section, includes_ocr: boolean(h.includes_ocr), object: identity(h.object), revision_id: text(h.revision_id, 256), capture_id: hash(h.capture_id), title: text(h.title), section: text(h.section, 256), line: number(h.line), text: text(h.text, 65536), derived_ocr: boolean(h.derived_ocr) }; }), next_cursor: optionalText(value.next_cursor, 2048), generation: number(value.generation), corpus_complete: boolean(value.corpus_complete), index_lag: number(value.index_lag) };
}
export function historyPage(result: unknown): { entries: HistoryEntry[]; next_cursor: string | null; inventory_complete: boolean } {
  const value = data(result); if (!Array.isArray(value.entries) || value.entries.length > 20) throw new Error('The history page is unsupported.');
  return { entries: value.entries.map(raw => { const h = object(raw); return { revision_id: text(h.revision_id, 256), capture_id: h.capture_id == null ? null : hash(h.capture_id), captured_at: h.captured_at === null ? null : number(h.captured_at), sequence: number(h.sequence), publication_date: optionalText(h.publication_date, 8), effective_date: optionalText(h.effective_date, 8) }; }), next_cursor: optionalText(value.next_cursor, 2048), inventory_complete: boolean(value.inventory_complete) };
}
export function metadata(value: unknown, expected: ObjectId, selector: Selector) {
  const m = object(value); const id = identity(m.object);
  if (!sameObject(id, expected) || (selector.kind === 'revision' && m.revision_id !== selector.id) || (selector.kind === 'capture' && m.capture_id !== selector.id)) throw new Error('The returned checkpoint does not match the requested object.');
  const freshness = m.freshness === null ? null : object(m.freshness);
  if ((selector.kind === 'head') !== (freshness !== null)) throw new Error('The server returned inconsistent HEAD freshness.');
  if (freshness) { if (!['fresh', 'stale'].includes(String(freshness.state))) throw new Error('The freshness status is invalid.'); for (const key of ['served_at', 'cached_at', 'age_seconds', 'fresh_ttl_seconds', 'fresh_remaining_seconds']) number(freshness[key]); }
  return { capture_id: hash(m.capture_id), revision_id: text(m.revision_id, 256), title: text(m.title), source_url: text(m.source_url, 2048), retrieved_at: number(m.retrieved_at), captured_at: number(m.captured_at), validated_at: number(m.validated_at), raw_sha256: hash(m.raw_sha256), processor_version: text(m.processor_version, 256), metadata: object(m.metadata), freshness };
}
export function getResult(result: unknown, expected: ObjectId, selector: Selector, section: string, offset: number, session?: string, sectionsOffset = 0) {
  const value = data(result);
  if (value.schema_version !== 1 || value.section !== section || value.offset !== offset || !Array.isArray(value.sections) || value.sections.length > 100) throw new Error('The content page does not match the request.');
  const content = text(value.text, 32768);
  const returnedSession = hash(value.session);
  if (session !== undefined && returnedSession !== session) throw new Error('The content session changed during paging.');
  const next = value.next_offset === null ? null : number(value.next_offset);
  if (next !== null && (next !== offset + bytes(content) || next <= offset)) throw new Error('The content continuation is invalid.');
  const section_count = number(value.section_count);
  const next_sections_offset = value.next_sections_offset === null ? null : number(value.next_sections_offset);
  const sectionEnd = sectionsOffset + value.sections.length;
  if (section_count > 10000 || sectionEnd > section_count || (next_sections_offset === null ? sectionEnd !== section_count : next_sections_offset !== sectionEnd || sectionEnd <= sectionsOffset || sectionEnd >= section_count)) throw new Error('The section catalog continuation is invalid.');
  const sections = value.sections.map(raw => { const s = object(raw); const kind = text(s.kind, 32); if (!['provider_text', 'extracted', 'ocr'].includes(kind)) throw new Error('Unsupported content section.'); return { id: text(s.id, 256), title: text(s.title), kind, bytes: number(s.bytes) }; });
  if (sections.reduce((used, s) => used + bytes(s.id) + bytes(s.title) + 128, 0) > 32768 || new Set(sections.map(s => s.id)).size !== sections.length) throw new Error('The section catalog exceeds its display budget or repeats an identifier.');
  return { section_count, next_sections_offset, session: returnedSession, metadata: metadata(value.metadata, expected, selector), section, text: content, offset, next_offset: next, sections };
}
export function mergeCatalog(previous: ReturnType<typeof getResult> | null, page: ReturnType<typeof getResult>, sectionsOffset = 0): ReturnType<typeof getResult> {
  if (!previous || previous.session !== page.session) { if (sectionsOffset !== 0) throw new Error('The section catalog has no retained first page.'); return page; }
  if (previous.metadata.capture_id !== page.metadata.capture_id || previous.section_count !== page.section_count) throw new Error('The section catalog changed during paging.');
  if (sectionsOffset === 0) {
    if (JSON.stringify(previous.sections.slice(0, page.sections.length)) !== JSON.stringify(page.sections)) throw new Error('The section catalog changed during paging.');
    return { ...page, sections: previous.sections, next_sections_offset: previous.next_sections_offset };
  }
  if (sectionsOffset !== previous.sections.length || sectionsOffset !== previous.next_sections_offset) throw new Error('The section catalog skipped a page.');
  const sections = [...previous.sections, ...page.sections];
  if (new Set(sections.map(s => s.id)).size !== sections.length || bytes(JSON.stringify(sections)) > 2 * 1024 * 1024) throw new Error('The retained section catalog exceeds its display budget or repeats an identifier.');
  return { ...page, sections };
}
