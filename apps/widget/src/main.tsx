import React, { useEffect, useRef, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { App } from '@modelcontextprotocol/ext-apps';
import { ResponseError, freshLabel, parseDetail, parseInitial, parseSearch, type DemoRecord, type DisplayRecord, type RecordPage, type Source } from './model.ts';
import './style.css';
import { HistoryBrowser } from './HistoryBrowser.tsx';
import { parseCapabilities, type HistoryCapabilities, type HistoryQuery } from './history-model.ts';
import { SourceOffer } from './SourceOffer.tsx';

const bridge = new App({ name: 'Synthetic record browser', version: '0.1.0' }, {});
function Browser() {
  const [ready, setReady] = useState(false);
  const [capabilities, setCapabilities] = useState<HistoryCapabilities>({ history: false, comparison: false, processor_versions: {} });
  const [historyQuery, setHistoryQuery] = useState<HistoryQuery | null>(null);
  const [searchQuery, setSearchQuery] = useState<HistoryQuery | null>(null);
  const [cancelVersion, setCancelVersion] = useState(0);
  const [query, setQuery] = useState('');
  const [source, setSource] = useState<Source>('layout_a');
  const [freshOnly, setFreshOnly] = useState(false);
  const [records, setRecords] = useState<DisplayRecord[]>([]);
  const [page, setPage] = useState<RecordPage | null>(null);
  const [detail, setDetail] = useState<DisplayRecord | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const serial = useRef(0);
  const heading = useRef<HTMLHeadingElement>(null);
  useEffect(() => {
    let alive = true;
    bridge.ontoolresult = result => {
      if (!alive) return;
      serial.current++;
      setLoading(false);
      setError('');
      setDetail(null);
      setPage(null);
      setHistoryQuery(null); setSearchQuery(null); setCancelVersion(value => value + 1);
      try { const initial = parseInitial(result); const features = parseCapabilities(result); setRecords(initial); setCapabilities(features); } catch (error) { setCapabilities({ history: false, comparison: false, processor_versions: {} }); setRecords([]); setError((error as Error).message); }
    };
    bridge.ontoolcancelled = () => { serial.current++; setCancelVersion(value => value + 1); setLoading(false); setError('The request was cancelled.'); };
    bridge.onclose = () => { if (alive) setReady(false); };
    bridge.connect(undefined, { timeout: 10000 }).then(() => { if (alive) setReady(true); }).catch(() => {
      if (alive) setError('Connect this record browser through an MCP Apps host to search.');
    });
    return () => { alive = false; serial.current++; void bridge.close(); };
  }, []);
  async function search(nextPage: number) {
    if (new TextEncoder().encode(query).byteLength > 256) {
      setError('Search text must fit within 256 UTF-8 bytes. Shorten your search.');
      return;
    }
    const request = ++serial.current;
    setLoading(true); setError(''); setDetail(null); setRecords([]); setPage(null); setHistoryQuery(null); setSearchQuery(null);
    try {
      const result = await bridge.callServerTool({ name: 'demo_search_records', arguments: { source, query, page: nextPage, page_size: 5, fresh_only: freshOnly } }, { timeout: 35000 });
      if (request !== serial.current) return;
      const parsed = parseSearch(result);
      setRecords(parsed.records); setPage(parsed); setSearchQuery({ operation: 'search', source, query, page: nextPage, page_size: 5 });
    } catch (error) { if (request === serial.current) setError(error instanceof ResponseError ? error.message : 'The search could not be completed. Try again.'); }
    finally { if (request === serial.current) setLoading(false); }
  }
  async function openRecord(item: { record: DemoRecord }) {
    const request = ++serial.current;
    setLoading(true); setError('');
    try {
      const result = await bridge.callServerTool({ name: 'demo_get_record', arguments: { source: item.record.source, id: item.record.id, fresh_only: freshOnly } }, { timeout: 35000 });
      if (request !== serial.current) return;
      const parsed = parseDetail(result);
      if (parsed.record.source !== item.record.source || parsed.record.id !== item.record.id) throw new Error('The server returned an unsupported record response.');
      setDetail(parsed); setHistoryQuery(null);
      requestAnimationFrame(() => heading.current?.focus());
    } catch { if (request === serial.current) setError('The record could not be opened. Try again.'); }
    finally { if (request === serial.current) setLoading(false); }
  }
  return <main>
    <header><p className="eyebrow">openlegal4everyone</p><h1>Synthetic record browser</h1><p>Demonstration data only. These records are not legislation or legal advice.</p></header>
    <div className="search" role="search">
      <label>Source<select aria-label="Source" value={source} disabled={loading} onChange={event => { setSource(event.target.value as Source); setPage(null); }}><option value="layout_a">Layout A</option><option value="layout_b">Layout B</option></select></label>
      <label className="query">Search records<input value={query} maxLength={256} disabled={loading} onChange={event => { setQuery(event.target.value); setPage(null); }} onKeyDown={event => { if (event.key === 'Enter' && ready && !loading) { event.preventDefault(); void search(0); } }} placeholder="Enter a title or phrase" /></label>
      <button className="submit" type="button" onClick={() => void search(0)} disabled={!ready || loading}>Search</button>
      <label className="check"><input type="checkbox" checked={freshOnly} disabled={loading} onChange={event => { setFreshOnly(event.target.checked); setPage(null); }} />Require fresh results</label>
    </div>
    <div role="status" aria-live="polite">{loading ? 'Loading records…' : !ready && !error ? 'Connecting to host…' : ''}</div>
    {error && <p role="alert" className="error">{error}</p>}
    {detail ? <article aria-busy={loading}>
      <button onClick={() => { setDetail(null); }} disabled={loading}>Back to results</button>
      <h2 ref={heading} tabIndex={-1}>{detail.record.title}</h2>
      <p className="metadata">Synthetic · {detail.record.source} / {detail.record.id}</p>
      <p className={detail.freshness.state === 'stale' ? 'stale' : 'metadata'}>{freshLabel(detail.freshness)}</p>
      <p className="body">{detail.record.body}</p>
      {detail.snapshot && <p className="metadata snapshot-id">Retained observation {detail.snapshot.snapshot_id} · captured Unix {detail.snapshot.captured_at}</p>}
      {capabilities.history && <button disabled={!ready || loading} onClick={() => setHistoryQuery({ operation: 'get', source: detail.record.source, id: detail.record.id })}>Browse record history</button>}
      <details><summary>Source and processing details</summary><dl>
        <dt>Provider / dataset</dt><dd>{detail.provenance.provider} / {detail.provenance.dataset}</dd>
        <dt>Source reference</dt><dd>{detail.provenance.source_reference}</dd>
        <dt>Processor version</dt><dd>{detail.provenance.processor_version}</dd>
        <dt>Retrieved / validated (Unix seconds)</dt><dd>{detail.provenance.retrieved_at} / {detail.provenance.validated_at}</dd>
        <dt>Payload SHA-256</dt><dd>{detail.provenance.payload_sha256}</dd>
      </dl></details>
    </article> : <section aria-label="Search results" aria-busy={loading}>
      {!loading && !error && records.length === 0 && <p className="empty">{page ? 'No records match this search.' : 'Search a synthetic source to explore records.'}</p>}
      <ul>{records.map((item, index) => <li key={`${item.record.source}:${item.record.id}:${index}`}>
        <button className="record" disabled={!ready || loading} onClick={() => void openRecord(item)}>{item.record.title}</button>
        <p className="metadata">Synthetic · {item.record.source} / {item.record.id}</p>
        <p className={item.freshness.state === 'stale' ? 'stale' : 'metadata'}>{freshLabel(item.freshness)}</p>
      </li>)}</ul>
      {page?.snapshot && <p className="metadata snapshot-id">Retained search observation {page.snapshot.snapshot_id} · captured Unix {page.snapshot.captured_at}</p>}
      {capabilities.history && searchQuery && <button disabled={!ready || loading} onClick={() => setHistoryQuery(searchQuery)}>Browse exact search page history</button>}
      {page && <nav aria-label="Results pages"><button disabled={loading || page.page === 0} onClick={() => void search(page.page - 1)}>Previous</button><span>Page {page.page + 1} · {page.total} records</span><button disabled={loading || (page.page + 1) * page.pageSize >= page.total} onClick={() => void search(page.page + 1)}>Next</button></nav>}
    </section>}
    {capabilities.history && <HistoryBrowser bridge={bridge} ready={ready && !loading} query={historyQuery} capabilities={capabilities} cancelVersion={cancelVersion} onCurrent={record => { void openRecord({ record }); }} />}
    <SourceOffer ready={ready} bridge={bridge} />
  </main>;
}
const root = document.getElementById('root');
if (root) createRoot(root).render(<Browser />);
