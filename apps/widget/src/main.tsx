import React, { useEffect, useRef, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { App } from '@modelcontextprotocol/ext-apps';
import { ResponseError, freshLabel, parseDetail, parseInitial, parseSearch, type DisplayRecord, type RecordPage, type Source } from './model.ts';
import './style.css';
import { readSourceUrl } from './source-offer.ts';

const bridge = new App({ name: 'Synthetic record browser', version: '0.1.0' }, {});
const sourceUrl = readSourceUrl(document);
function SourceOffer({ ready }: { ready: boolean }) {
  const [message, setMessage] = useState('');
  const [opening, setOpening] = useState(false);
  const supported = ready && Boolean(bridge.getHostCapabilities()?.openLinks);
  async function openSource() {
    if (!sourceUrl || !supported || opening) return;
    setOpening(true);
    setMessage('');
    try {
      const result = await bridge.openLink({ url: sourceUrl }, { timeout: 10000 });
      setMessage(result.isError ? 'The host declined to open the source. Copy the URL below.' : 'Source link sent to the host.');
    } catch { setMessage('The source link could not be opened. Copy the URL below.'); }
    finally { setOpening(false); }
  }
  return <section className="source-offer" aria-label="Corresponding source">
    <h2>Corresponding source</h2>
    {sourceUrl ? <>
      <p>Get the source code for this running server and widget.</p>
      <button type="button" disabled={!supported || opening} onClick={() => void openSource()}>Get source code</button>
      <label>Source code URL<input readOnly value={sourceUrl} onFocus={event => event.currentTarget.select()} /></label>
      <p className="metadata">{message || (!supported ? 'Copy the URL to open it when your host cannot open links.' : 'Your host controls opening this link.')}</p>
    </> : <p>The source URL is unavailable or invalid. Ask the operator for the corresponding source.</p>}
  </section>;
}
function Browser() {
  const [ready, setReady] = useState(false);
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
      try { setRecords(parseInitial(result)); } catch (error) { setRecords([]); setError((error as Error).message); }
    };
    bridge.ontoolcancelled = () => { serial.current++; setLoading(false); setError('The request was cancelled.'); };
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
    setLoading(true); setError(''); setDetail(null); setRecords([]); setPage(null);
    try {
      const result = await bridge.callServerTool({ name: 'demo_search_records', arguments: { source, query, page: nextPage, page_size: 5, fresh_only: freshOnly } }, { timeout: 15000 });
      if (request !== serial.current) return;
      const parsed = parseSearch(result);
      setRecords(parsed.records); setPage(parsed);
    } catch (error) { if (request === serial.current) setError(error instanceof ResponseError ? error.message : 'The search could not be completed. Try again.'); }
    finally { if (request === serial.current) setLoading(false); }
  }
  async function openRecord(item: DisplayRecord) {
    const request = ++serial.current;
    setLoading(true); setError('');
    try {
      const result = await bridge.callServerTool({ name: 'demo_get_record', arguments: { source: item.record.source, id: item.record.id, fresh_only: freshOnly } }, { timeout: 15000 });
      if (request !== serial.current) return;
      const parsed = parseDetail(result);
      if (parsed.record.source !== item.record.source || parsed.record.id !== item.record.id) throw new Error('The server returned an unsupported record response.');
      setDetail(parsed);
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
      {page && <nav aria-label="Results pages"><button disabled={loading || page.page === 0} onClick={() => void search(page.page - 1)}>Previous</button><span>Page {page.page + 1} · {page.total} records</span><button disabled={loading || (page.page + 1) * page.pageSize >= page.total} onClick={() => void search(page.page + 1)}>Next</button></nav>}
    </section>}
    <SourceOffer ready={ready} />
  </main>;
}
const root = document.getElementById('root');
if (root) createRoot(root).render(<Browser />);
