/** Immutable local observations are separate from current freshness and legal revisions. */
import { parseRecord, parseProvenance, ResponseError, type DemoRecord, type Provenance, type Source } from './model.ts';
export type HistoryQuery = { operation: 'get'; source: Source; id: string } | { operation: 'search'; source: Source; query: string; page: number; page_size: number };
export interface SnapshotSummary { snapshot_id: string; sequence: number; captured_at: number; processor_version: string; schema_version: number; payload_sha256: string }
export interface SnapshotPage { snapshots: SnapshotSummary[]; next_cursor: string | null }
export interface SnapshotEnvelope { snapshot: SnapshotSummary; query: HistoryQuery; records: DemoRecord[]; provenance: Provenance; clock_anomaly: boolean; total?: number }
export interface SnapshotOrigin { source: Source; record_id: string; before: SnapshotSummary; after: SnapshotSummary; projection: 'title_lf_lf_body_v1' }
export interface HistoryCapabilities { history: boolean; comparison: boolean; processor_versions: Partial<Record<Source, string>> }
const invalid = () => new ResponseError('The server returned an unsupported history response.');
const bytes = (value: string) => new TextEncoder().encode(value).byteLength;
function object(value: unknown): Record<string, unknown> { if (!value || typeof value !== 'object' || Array.isArray(value)) throw invalid(); return value as Record<string, unknown>; }
function integer(value: unknown, max = Number.MAX_SAFE_INTEGER): number { if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0 || value > max) throw invalid(); return value; }
function string(value: unknown, max: number): string { if (typeof value !== 'string' || value.length > max || bytes(value) > max) throw invalid(); return value; }
function digest(value: unknown): string { const id = string(value, 64); if (!/^[0-9a-f]{64}$/.test(id)) throw invalid(); return id; }
function source(value: unknown): Source { if (value !== 'layout_a' && value !== 'layout_b') throw invalid(); return value; }
function recordId(value: unknown): string { const id = string(value, 128); if (!/^[A-Za-z0-9_-]+$/.test(id)) throw invalid(); return id; }
function structured(value: unknown): Record<string, unknown> { const result = object(value); if (result.isError) throw new ResponseError('The history request could not be completed. Try again.'); return object(result.structuredContent); }
export function parseCapabilities(result: unknown): HistoryCapabilities {
  const value = structured(result);
  if (value.capabilities === undefined) return { history: false, comparison: false, processor_versions: {} };
  const capability = object(value.capabilities);
  if (typeof capability.history !== 'boolean' || typeof capability.comparison !== 'boolean' || (capability.comparison && !capability.history)) throw invalid();
  const versions = capability.processor_versions === undefined ? {} : object(capability.processor_versions);
  const processor_versions: Partial<Record<Source, string>> = {};
  for (const key of ['layout_a', 'layout_b'] as const) if (versions[key] !== undefined) { const version = string(versions[key], 128); if (!version) throw invalid(); processor_versions[key] = version; }
  return { history: capability.history, comparison: capability.comparison, processor_versions };
}
export function parseSnapshotSummary(value: unknown): SnapshotSummary {
  const raw = object(value);
  const result = { snapshot_id: digest(raw.snapshot_id), sequence: integer(raw.sequence), captured_at: integer(raw.captured_at), processor_version: string(raw.processor_version, 128), schema_version: integer(raw.schema_version, 0xffffffff), payload_sha256: digest(raw.payload_sha256) };
  if (!result.processor_version || result.schema_version !== 1 || result.sequence === 0) throw invalid();
  return result;
}
export function parseHistoryQuery(value: unknown): HistoryQuery {
  const raw = object(value);
  const selected = source(raw.source);
  if (raw.operation === 'get') return { operation: 'get', source: selected, id: recordId(raw.id) };
  if (raw.operation !== 'search') throw invalid();
  const pageSize = integer(raw.page_size, 20);
  if (pageSize === 0) throw invalid();
  return { operation: 'search', source: selected, query: string(raw.query, 256), page: integer(raw.page, 1000), page_size: pageSize };
}
export function sameQuery(left: HistoryQuery, right: HistoryQuery): boolean { return JSON.stringify(parseHistoryQuery(left)) === JSON.stringify(parseHistoryQuery(right)); }
export function parseSnapshotPage(result: unknown): SnapshotPage {
  const raw = structured(result);
  if (raw.synthetic !== true || !Array.isArray(raw.snapshots) || raw.snapshots.length > 20) throw invalid();
  const snapshots = raw.snapshots.map(parseSnapshotSummary);
  if (new Set(snapshots.map(item => item.snapshot_id)).size !== snapshots.length || snapshots.some((item, index) => index > 0 && item.sequence >= snapshots[index - 1].sequence)) throw invalid();
  const next_cursor = raw.next_cursor === null ? null : string(raw.next_cursor, 256);
  if (next_cursor === '' || (next_cursor !== null && snapshots.length === 0)) throw invalid();
  return { snapshots, next_cursor };
}
export function parseSnapshot(result: unknown, expected: { query: HistoryQuery; snapshot_id: string }): SnapshotEnvelope {
  const raw = structured(result);
  if (raw.schema_version !== 1 || raw.synthetic !== true || raw.historical !== true || typeof raw.clock_anomaly !== 'boolean') throw invalid();
  const snapshot = parseSnapshotSummary(raw.snapshot), query = parseHistoryQuery(raw.query), provenance = parseProvenance(raw.provenance);
  if (snapshot.snapshot_id !== expected.snapshot_id || !sameQuery(query, expected.query) || provenance.processor_version !== snapshot.processor_version || provenance.payload_sha256 !== snapshot.payload_sha256) throw invalid();
  let records: DemoRecord[], total: number | undefined;
  if (query.operation === 'get') { const record = parseRecord(raw.data); if (record.source !== query.source || record.id !== query.id) throw invalid(); records = [record]; }
  else { const data = object(raw.data); if (!Array.isArray(data.records) || data.records.length > query.page_size || data.page !== query.page || data.page_size !== query.page_size) throw invalid(); records = data.records.map(parseRecord); total = integer(data.total, 20000); if (records.length !== Math.min(query.page_size, Math.max(0, total - query.page * query.page_size)) || new Set(records.map(record => record.id)).size !== records.length || records.some(record => record.source !== query.source)) throw invalid(); }
  return { snapshot, query, records, provenance, clock_anomaly: raw.clock_anomaly, ...(total === undefined ? {} : { total }) };
}
export function parseSnapshotOrigin(value: unknown): SnapshotOrigin {
  const raw = object(value);
  if (raw.projection !== 'title_lf_lf_body_v1') throw invalid();
  return { source: source(raw.source), record_id: recordId(raw.record_id), before: parseSnapshotSummary(raw.before), after: parseSnapshotSummary(raw.after), projection: raw.projection };
}
export function snapshotLabel(snapshot: SnapshotSummary, currentVersion?: string): string {
  const version = currentVersion && currentVersion !== snapshot.processor_version ? `Previous processor ${snapshot.processor_version}` : `Processor ${snapshot.processor_version}`;
  return `Observation ${snapshot.sequence} · captured Unix ${snapshot.captured_at} · ${version}`;
}
