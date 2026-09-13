import React, { useEffect, useRef, useState } from 'react';
import type { App } from '@modelcontextprotocol/ext-apps';
import { parseSnapshot, parseSnapshotPage, snapshotLabel, type HistoryCapabilities, type HistoryQuery, type SnapshotEnvelope, type SnapshotPage, type SnapshotSummary } from './history-model.ts';
import type { DemoRecord } from './model.ts';
import { SnapshotComparison } from './SnapshotComparison.tsx';

export function HistoryBrowser({ bridge, ready, query, capabilities, cancelVersion, onCurrent }: { bridge: App; ready: boolean; query: HistoryQuery | null; capabilities: HistoryCapabilities; cancelVersion: number; onCurrent: (record: DemoRecord) => void }) {
  const [page, setPage] = useState<SnapshotPage | null>(null), [snapshot, setSnapshot] = useState<SnapshotEnvelope | null>(null);
  const [embedded, setEmbedded] = useState<DemoRecord | null>(null);
  const [before, setBefore] = useState<SnapshotSummary | null>(null), [after, setAfter] = useState<SnapshotSummary | null>(null);
  const [busy, setBusy] = useState(false), [error, setError] = useState('');
  const [cursors, setCursors] = useState<(string | undefined)[]>([undefined]), [index, setIndex] = useState(0);
  const serial = useRef(0), live = useRef(true), queryRef = useRef(query); queryRef.current = query;
  const queryKey = JSON.stringify(query);
  useEffect(() => { live.current = true; return () => { live.current = false; serial.current++; }; }, []);
  useEffect(() => {
    serial.current++; setPage(null); setSnapshot(null); setEmbedded(null); setBefore(null); setAfter(null); setCursors([undefined]); setIndex(0); setError(''); setBusy(false);
    if (query && ready) void loadList(undefined, 0);
    // Query identity controls requests; readiness is included for a reconnected host.
  }, [queryKey, ready]);
  useEffect(() => { serial.current++; setBusy(false); }, [cancelVersion]);
  async function loadList(cursor: string | undefined, nextIndex: number) {
    const selected = queryRef.current; if (!selected || !ready) return;
    const request = ++serial.current;
    setBusy(true); setError(''); setSnapshot(null); setEmbedded(null);
    try {
      const result = await bridge.callServerTool({ name: 'demo_list_snapshots', arguments: { query: selected, limit: 10, ...(cursor === undefined ? {} : { cursor }) } }, { timeout: 35000 });
      if (!live.current || serial.current !== request) return;
      const parsed = parseSnapshotPage(result);
      if (cursor !== undefined && parsed.next_cursor === cursor) throw new Error('Cursor did not advance');
      setPage(parsed); setIndex(nextIndex);
      setCursors(previous => [...previous.slice(0, nextIndex), cursor]);
    } catch { if (live.current && serial.current === request) setError('The retained history could not be loaded. Retry history.'); }
    finally { if (live.current && serial.current === request) setBusy(false); }
  }
  async function open(selected: SnapshotSummary) {
    if (!query || !ready) return;
    const request = ++serial.current, expected = { query, snapshot_id: selected.snapshot_id };
    setBusy(true); setError(''); setSnapshot(null); setEmbedded(null);
    try {
      const result = await bridge.callServerTool({ name: 'demo_get_snapshot', arguments: expected }, { timeout: 35000 });
      if (!live.current || serial.current !== request) return;
      setSnapshot(parseSnapshot(result, expected));
    } catch { if (live.current && serial.current === request) setError('This retained snapshot could not be opened. Select it again to retry.'); }
    finally { if (live.current && serial.current === request) setBusy(false); }
  }
  const version = query ? capabilities.processor_versions[query.source] : undefined;
  function historicalRecord(record: DemoRecord) {
    return <article aria-label="Historical record">
      <h3>{record.title}</h3><p className="metadata">Synthetic · {record.source} / {record.id}</p><p className="body">{record.body}</p>
      <button disabled={!ready || busy} onClick={() => onCurrent(record)}>Open current record</button>
    </article>;
  }
  return <section aria-label="Retained history" className="history">
    {query && <>
      <h2>Retained local observations</h2>
      <p>Historical snapshots are immutable local observations, not provider or legal revisions. Reading history never refreshes upstream data.</p>
      <p className="metadata">{query.operation === 'get' ? `${query.source} / ${query.id}` : `${query.source} · search ${JSON.stringify(query.query)} · exact page ${query.page + 1} · page size ${query.page_size}`}</p>
      {query.operation === 'search' && <p>Each search page is captured independently. This history does not reconstruct a complete historical search.</p>}
      {busy && <p role="status">Loading retained history…</p>}
      {error && <p role="alert" className="error">{error}</p>}
      <button disabled={!ready || busy} onClick={() => void loadList(undefined, 0)}>Reload history</button>
      {page && <>
        {page.snapshots.length === 0 && <p>No retained snapshots for this exact request.</p>}
        <ol>{page.snapshots.map(item => <li key={item.snapshot_id}>
          <button disabled={!ready || busy} onClick={() => void open(item)}>{snapshotLabel(item, version)}</button>
          <p className="metadata snapshot-id">{item.snapshot_id}</p>
          {capabilities.comparison && query.operation === 'get' && <div className="diff-actions"><button disabled={busy} onClick={() => setBefore(item)} aria-label={`Use observation ${item.sequence} as before`}>Use as before</button><button disabled={busy} onClick={() => setAfter(item)} aria-label={`Use observation ${item.sequence} as after`}>Use as after</button></div>}
        </li>)}</ol>
        <nav aria-label="History pages"><button disabled={!ready || busy || index === 0} onClick={() => void loadList(cursors[index - 1], index - 1)}>Previous observations</button><span>History page {index + 1}</span><button disabled={!ready || busy || page.next_cursor === null} onClick={() => void loadList(page.next_cursor!, index + 1)}>Older observations</button></nav>
      </>}
      {snapshot && <section aria-label="Historical snapshot" className="historical-snapshot">
        <h3>Historical snapshot</h3><p>{snapshotLabel(snapshot.snapshot, version)}</p><p className="snapshot-id">{snapshot.snapshot.snapshot_id}</p>
        {snapshot.clock_anomaly && <p className="stale">Clock anomaly recorded. Observation order follows the sequence, not a legal timeline.</p>}
        {snapshot.query.operation === 'get' ? historicalRecord(snapshot.records[0]) : <>
          <p>Embedded records from this exact historical search page · recorded total {snapshot.total}</p>
          {embedded ? <><button onClick={() => setEmbedded(null)}>Back to historical page</button>{historicalRecord(embedded)}</> : <ul>{snapshot.records.map((record, position) => <li key={`${record.id}:${position}`}><button className="record" onClick={() => setEmbedded(record)}>{record.title}</button></li>)}</ul>}
        </>}
        <details><summary>Historical source and processing details</summary><dl>
          <dt>Provider / dataset</dt><dd>{snapshot.provenance.provider} / {snapshot.provenance.dataset}</dd><dt>Source reference</dt><dd>{snapshot.provenance.source_reference}</dd>
          <dt>Retrieved / validated (Unix seconds)</dt><dd>{snapshot.provenance.retrieved_at} / {snapshot.provenance.validated_at}</dd><dt>Payload SHA-256</dt><dd>{snapshot.provenance.payload_sha256}</dd>
        </dl></details>
      </section>}
      {capabilities.comparison && query.operation === 'get' && <dl aria-label="Selected snapshots"><dt>Before</dt><dd>{before ? `${snapshotLabel(before, version)} · ${before.snapshot_id}` : 'Choose a before observation.'}</dd><dt>After</dt><dd>{after ? `${snapshotLabel(after, version)} · ${after.snapshot_id}` : 'Choose an after observation.'}</dd></dl>}
    </>}
    {capabilities.comparison && <SnapshotComparison bridge={bridge} ready={ready} query={query} before={before?.snapshot_id ?? ''} after={after?.snapshot_id ?? ''} cancelVersion={cancelVersion} />}
  </section>;
}
