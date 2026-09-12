/** The browser accepts only the bounded synthetic demo contract. */
export type Source = 'layout_a' | 'layout_b';
export interface DemoRecord { source: Source; id: string; title: string; body: string; synthetic: true }
export interface Freshness { state: 'fresh' | 'stale'; age_seconds: number }
export interface Provenance { provider: string; dataset: string; source_reference: string; payload_sha256: string; processor_version: string; retrieved_at: number; validated_at: number }
export interface DisplayRecord { record: DemoRecord; freshness: Freshness; provenance: Provenance }
export interface RecordPage { records: DisplayRecord[]; page: number; pageSize: number; total: number }
export class ResponseError extends Error {}
const invalid = () => new ResponseError('The server returned an unsupported record response.');
function object(value: unknown): Record<string, unknown> {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) throw invalid();
  return value as Record<string, unknown>;
}
function integer(value: unknown, maximum: number): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0 || value > maximum) throw invalid();
  return value;
}
function string(value: unknown, maximum: number): string {
  if (typeof value !== 'string' || value.length > maximum) throw invalid();
  return value;
}
export function parseRecord(value: unknown): DemoRecord {
  const record = object(value);
  if (record.synthetic !== true || (record.source !== 'layout_a' && record.source !== 'layout_b')) throw invalid();
  const id = string(record.id, 128);
  if (!/^[A-Za-z0-9_-]+$/.test(id)) throw invalid();
  return { source: record.source, id, title: string(record.title, 1024), body: string(record.body, 32768), synthetic: true };
}
function envelope(value: unknown): { data: unknown; freshness: Freshness; provenance: Provenance } {
  const result = object(value);
  if (result.synthetic !== true) throw invalid();
  const freshness = object(result.freshness);
  if (freshness.state !== 'fresh' && freshness.state !== 'stale') throw invalid();
  const raw = object(result.provenance);
  const digest = string(raw.payload_sha256, 64);
  if (!/^[0-9a-f]{64}$/.test(digest)) throw invalid();
  const provenance: Provenance = { provider: string(raw.provider, 128), dataset: string(raw.dataset, 128), source_reference: string(raw.source_reference, 2048), payload_sha256: digest, processor_version: string(raw.processor_version, 128), retrieved_at: integer(raw.retrieved_at, Number.MAX_SAFE_INTEGER), validated_at: integer(raw.validated_at, Number.MAX_SAFE_INTEGER) };
  if (!provenance.provider || !provenance.dataset || !provenance.processor_version || provenance.validated_at < provenance.retrieved_at) throw invalid();
  return { data: result.data, provenance, freshness: { state: freshness.state, age_seconds: integer(freshness.age_seconds, 300) } };
}
function structured(result: unknown): Record<string, unknown> {
  const response = object(result);
  // Never display opaque error text or raw tool output from a remote source.
  if (response.isError === true) throw new ResponseError('The request could not be completed. Try again.');
  return object(response.structuredContent);
}
export function parseDetail(result: unknown): DisplayRecord {
  const value = envelope(structured(result));
  return { record: parseRecord(value.data), freshness: value.freshness, provenance: value.provenance };
}
export function parseSearch(result: unknown): RecordPage {
  const value = envelope(structured(result));
  const data = object(value.data);
  if (!Array.isArray(data.records) || data.records.length > 20) throw invalid();
  const pageSize = integer(data.page_size, 20);
  if (pageSize < 1 || data.records.length > pageSize) throw invalid();
  return { records: data.records.map(record => ({ record: parseRecord(record), freshness: value.freshness, provenance: value.provenance })), page: integer(data.page, 1000), pageSize, total: integer(data.total, 1000000) };
}
export function parseInitial(result: unknown): DisplayRecord[] {
  const value = structured(result);
  if (value.synthetic !== true || !Array.isArray(value.records) || value.records.length > 20) throw invalid();
  return value.records.map(item => {
    const parsed = envelope(item);
    return { record: parseRecord(parsed.data), freshness: parsed.freshness, provenance: parsed.provenance };
  });
}
export function freshLabel(value: Freshness): string {
  return `${value.state === 'fresh' ? 'Fresh when returned' : 'Stale fallback when returned'} · age at retrieval ${value.age_seconds}s`;
}
