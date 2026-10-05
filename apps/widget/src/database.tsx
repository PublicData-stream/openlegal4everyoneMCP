import React, { useEffect, useRef, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { App } from '@modelcontextprotocol/ext-apps';
import { SourceOffer } from './SourceOffer.tsx';
import { DATASETS, datasetLabel, isDataset, hasProviderRevisions, type Dataset, data, getResult, mergeCatalog, historyPage, metadata, searchPage, type ObjectId, type Selector, type SearchPage, type HistoryEntry, type CollectionNotice, providerAdmission, pendingHead, type ProviderAdmission, type PendingHead } from './database-model.ts';
import { parseSummary, parsePage, parseDelete, type Comparison, type DiffPage } from './text-diff-model.ts';
import './style.css';
import './text-diff.css';
const bridge = new App({ name: 'Legal corpus browser', version: '1.0.0' }, {});
const date = (seconds: number) => seconds <= 8_640_000_000_000 ? new Date(seconds * 1000).toISOString() : 'unrepresentable timestamp';
function noticeText(n: CollectionNotice) { const reason = n.code === 'source_unavailable' ? '다운로드할 수 없음 / unavailable download' : n.code === 'source_data_invalid' ? '자료 손상 / invalid source data' : n.code === 'download_failed' ? '다운로드 실패 / failed download' : '첨부 누락 / incomplete attachment'; return `${n.dataset} · ${n.scope === 'page' ? '목록 / list' : '상세 / detail'} · ${reason} · ${n.affected_count}건 영향 / affected · 수집 재개 시 재시도 대상 / eligible for retry when collection resumes`; }
type Content = ReturnType<typeof getResult>;
function AdmissionStatus({ value }: { value: ProviderAdmission | null }) {
  if (!value) return <section aria-label="Provider admission"><p>공급자 상태 확인 불가 / Provider admission status unknown.</p></section>;
  const review = value.continuous.requires_operator_review || value.on_demand.requires_operator_review;
  return <section aria-label="Provider admission"><p role="status">{review ? '운영자 조치 필요 / Operator action required' : '현재 관측된 공급자 상태 / Observed provider admission status'}</p>
    <p>차단 시작 / Blocked since: {value.paused_since === null ? 'unknown' : date(value.paused_since)} · 불확실성 최초 관측 / Uncertainty first observed: {value.uncertainty_first_observed_at === null ? 'unknown' : date(value.uncertainty_first_observed_at)}</p>
    {(['continuous', 'on_demand'] as const).map(name => <p key={name}>{name}: {value[name].reason} · {value[name].ready ? 'admission available at observation' : 'blocked'} · 다음 상태 확인 / Next status recheck: {value[name].recheck_at === null ? 'unknown' : date(value[name].recheck_at)}</p>)}
    <p>확인 시각 / Observed: {date(value.observed_at)}. 재확인 간격은 복구 완료 예상 시간이 아닙니다 / Recheck timing is not a recovery estimate.</p></section>;
}
function DatabaseBrowser() {
  const [ready, setReady] = useState(false); const [busy, setBusy] = useState(false); const [error, setError] = useState('');
  const [query, setQuery] = useState(''); const [mode, setMode] = useState<'query' | 'rg'>('query'); const [dataset, setDataset] = useState<Dataset | ''>('');
  const [search, setSearch] = useState<SearchPage | null>(null); const [selected, setSelected] = useState<ObjectId | null>(null);
  const [content, setContent] = useState<Content | null>(null); const [history, setHistory] = useState<ReturnType<typeof historyPage> | null>(null);
  const [admission, setAdmission] = useState<ProviderAdmission | null>(null); const [pending, setPending] = useState<PendingHead | null>(null); const [diagnosticSeen, setDiagnosticSeen] = useState(false);
  const [historyKind, setHistoryKind] = useState<'revisions' | 'captures'>('revisions');
  const [before, setBefore] = useState<Selector | null>(null); const [after, setAfter] = useState<Selector | null>(null);
  const [comparison, setComparison] = useState<Comparison | null>(null); const [diffPage, setDiffPage] = useState<DiffPage | null>(null);
  const [diffProvenance, setDiffProvenance] = useState('');
  const working = useRef(false); const live = useRef(true); const handles = useRef(new Set<string>());
  useEffect(() => {
    live.current = true;
    bridge.ontoolresult = result => { if (live.current) updateAdmission(result); };
    bridge.onclose = () => { if (live.current) setReady(false); };
    void bridge.connect(undefined, { timeout: 10000 }).then(() => { if (live.current) setReady(true); }).catch(() => { if (live.current) setError('Connect through an MCP Apps host to browse the corpus.'); });
    return () => { live.current = false; void bridge.close(); };
  }, []);
  async function perform(action: () => Promise<void>) {
    if (!ready || working.current) return;
    working.current = true; setBusy(true); setError('');
    try { await action(); } catch (e) { if (live.current) setError(e instanceof Error ? e.message : 'The operation failed.'); }
    finally { working.current = false; if (live.current) setBusy(false); }
  }
  async function call(name: string, args: Record<string, unknown>) { const result = await bridge.callServerTool({ name, arguments: args }, { timeout: 30000 }); if (live.current && name.startsWith('database.')) updateAdmission(result); return result; }
  async function clearComparison() {
    for (const id of [...handles.current]) { parseDelete(await call('text.diff.delete', { comparison_id: id })); handles.current.delete(id); }
    setComparison(null); setDiffPage(null); setDiffProvenance('');
  }
  function updateAdmission(result: unknown) { setDiagnosticSeen(true); try { setAdmission(providerAdmission(result)); } catch { setAdmission(null); setError('공급자 진단 확인 불가 / Provider diagnostic unavailable.'); } }
  function find(cursor: string | null = null) { void perform(async () => {
    const result = await call(`database.${mode}`, { query, filters: { datasets: dataset ? [dataset] : [] }, limit: 20, cursor, include_ocr: false });
    const page = searchPage(result);
    if (live.current) { setSearch(page); updateAdmission(result); }
  }); }
  async function read(id: ObjectId, selector: Selector, section = 'body', offset = 0, session?: string, sectionsOffset = 0) {
    const result = await call('database.get', { object: id, selector, fresh_only: false, section, offset, sections_offset: sectionsOffset, ...(session ? { session } : {}) });
    const waiting = pendingHead(result, selector);
    if (waiting && (offset !== 0 || session !== undefined || sectionsOffset !== 0)) throw new Error('A retained content page cannot become pending.');
    if (waiting) { if (live.current) { setPending(waiting); setContent(null); updateAdmission(result); } return; }
    const page = getResult(result, id, selector, section, offset, session, sectionsOffset);
    const merged = mergeCatalog(content, page, sectionsOffset);
    if (live.current) { setContent(merged); setPending(null); updateAdmission(result); }
  }
  function choose(id: ObjectId, selector: Selector) { void perform(async () => {
    await clearComparison(); setSelected(id); setContent(null); setPending(null); setHistory(null); setBefore(null); setAfter(null); setHistoryKind(hasProviderRevisions(id.dataset) ? 'revisions' : 'captures');
    await read(id, selector);
  }); }
  function listHistory(cursor: string | null = null) { if (selected) void perform(async () => {
    const result = historyPage(await call('database.history', { object: selected, kind: historyKind, limit: 20, cursor }));
    if (historyKind === 'captures' && result.entries.some(entry => !entry.capture_id)) throw new Error('The capture history omitted an exact checkpoint identifier.');
    if (live.current) setHistory(result);
  }); }
  function checkpoint(entry: HistoryEntry): Selector { return historyKind === 'captures' && entry.capture_id ? { kind: 'capture', id: entry.capture_id } : { kind: 'revision', id: entry.revision_id }; }
  function compare() { if (selected && before && after) void perform(async () => {
    await clearComparison();
    const result = data(await call('database.diff', { object: selected, before, after, include_ocr: false }));
    const summary = parseSummary(result.comparison); handles.current.add(summary.comparison_id);
    try {
      if (result.schema_version !== 1 || result.includes_ocr !== false) throw new Error('The comparison response is inconsistent.');
      const a = metadata(result.before, selected, before); const b = metadata(result.after, selected, after);
      setDiffProvenance(`Before revision ${a.revision_id}, capture ${a.capture_id}; after revision ${b.revision_id}, capture ${b.capture_id}. OCR excluded.`);
      if (live.current) setComparison(summary);
      if (summary.change_pages) { const page = parsePage(await call('text.diff.page', { comparison_id: summary.comparison_id, view: 'changes', page: 0 }), { comparison_id: summary.comparison_id, view: 'changes', page: 0 }); if (live.current) setDiffPage(page); }
    } catch (e) { await clearComparison(); throw e; }
  }); }
  const meta = content?.metadata;
  return <main><header><p className="eyebrow">openlegal4everyone</p><h1>Legal corpus</h1><p>Search retained source records and compare checkpoints. Coverage may be incomplete; textual differences do not establish legal applicability.</p></header>
    <section aria-label="Corpus search"><label>Search expression<input value={query} disabled={busy} onChange={e => { setQuery(e.target.value); setSearch(null); }} /></label>
      <label>Search method<select value={mode} disabled={busy} onChange={e => { setMode(e.target.value as 'query' | 'rg'); setSearch(null); }}><option value="query">Analyzed query</option><option value="rg">Ripgrep regular expression</option></select></label>
      <label>Dataset<select value={dataset} disabled={busy} onChange={e => { const value = e.target.value; if (value === '' || isDataset(value)) { setDataset(value); setSearch(null); } }}><option value="">All available datasets</option>{DATASETS.map(value => <option key={value} value={value}>{datasetLabel(value)}</option>)}</select></label>
      <button disabled={!ready || busy} onClick={() => find()}>Search corpus</button></section>
    {busy && <p role="status">Loading corpus data…</p>}{error && <p role="alert">{error}</p>}
    {error && !comparison && handles.current.size > 0 && <button disabled={busy} onClick={() => void perform(clearComparison)}>Retry comparison cleanup</button>}
    {(diagnosticSeen || search || selected) && <AdmissionStatus value={admission} />}
    {search && <section aria-label="Search results"><p>{search.corpus_complete ? 'Reported corpus inventory complete' : 'Partial corpus coverage'} · generation {search.generation} · index lag {search.index_lag}</p>{search.collection_notices.map((n, i) => <p role="status" className="stale" key={i}>주의 사항 / Collection warning: {noticeText(n)}</p>)}{!search.hits.length && <p>No matches on this page.</p>}
      {search.hits.map((hit, index) => <article key={index}><h2><button disabled={busy} onClick={() => choose(hit.object, { kind: 'capture', id: hit.capture_id })}>{hit.title}</button></h2><p>{hit.object.dataset} · {hit.object.id} · revision {hit.revision_id} · {hit.match_scope === 'object' ? `Whole-object query match · illustrative excerpt from ${hit.excerpt_section}` : `Line match · ${hit.section}:${hit.line}`}{hit.derived_ocr ? ' · OCR-derived excerpt' : ''}{hit.includes_ocr ? ' · search scope includes OCR' : ''}</p><pre>{hit.text}</pre></article>)}
      <button disabled={busy || !search.next_cursor} onClick={() => find(search.next_cursor)}>Next search page</button></section>}
    {selected && <section aria-label="Selected object"><h2>{meta?.title ?? selected.id}</h2><p>{selected.jurisdiction} / {selected.provider} / {selected.dataset} / {selected.id}</p><button disabled={busy} onClick={() => void perform(() => read(selected, { kind: 'head' }))}>Read HEAD</button>
      {pending && <p role="status">HEAD 수집 대기 / HEAD collection pending: {pending.status} · 이전 접수 사유 / Earlier receipt reason: {pending.reason ?? 'reason unknown'} · {pending.retry_after_seconds}s 후 상태 재확인 / until status recheck; this is not a recovery estimate.</p>}
      {content && meta && <><p>Revision {meta.revision_id} · capture {meta.capture_id}</p><p>Fetched {date(meta.retrieved_at)} · captured {date(meta.captured_at)} · validated {date(meta.validated_at)}</p>
        <p>{meta.freshness ? `${meta.freshness.state}; cache age ${meta.freshness.age_seconds}s; TTL ${meta.freshness.fresh_ttl_seconds}s; fresh remaining ${meta.freshness.fresh_remaining_seconds}s` : 'Historical capture; no current freshness claim.'}</p>
        {meta.missingAttachments && <p role="status" className="stale">첨부 증거가 불완전합니다 / Attachment evidence is incomplete: {meta.missingAttachments.expected - meta.missingAttachments.available} / {meta.missingAttachments.expected}건의 첨부파일을 읽지 못했습니다 / advertised attachments could not be read. 아래 제공자 본문에는 누락된 첨부파일이 포함되지 않습니다 / The provider body below does not include those attachments.</p>}
        {meta.collection_notices.map((n, i) => <p role="status" className="stale" key={i}>주의 사항 및 메모 / Collection warning and note: {noticeText(n)}</p>)}
        <p>Source: {meta.source_url}</p><p>Processor {meta.processor_version} · source SHA-256 {meta.raw_sha256}</p><details><summary>Metadata</summary><pre>{JSON.stringify(meta.metadata, null, 2)}</pre></details>
        <label>Content section<select value={content.section} disabled={busy} onChange={e => void perform(() => read(selected, { kind: 'capture', id: meta.capture_id }, e.target.value, 0, content.session))}><option value="body">Body</option><option value="title">Title</option>{content.sections.map(s => <option key={s.id} value={s.id}>{s.title} ({s.kind})</option>)}</select></label>
        <p>{content.sections.length} of {content.section_count} section summaries loaded.</p><button disabled={busy || content.next_sections_offset === null} onClick={() => void perform(() => read(selected, { kind: 'capture', id: meta.capture_id }, content.section, content.offset, content.session, content.next_sections_offset!))}>Load more sections</button>
        <pre aria-label="Object content">{content.text}</pre><p>UTF-8 byte offset {content.offset}</p><button disabled={busy || content.next_offset === null} onClick={() => void perform(() => read(selected, { kind: 'capture', id: meta.capture_id }, content.section, content.next_offset!, content.session))}>Next content page</button></>}
      <label>History type<select value={historyKind} disabled={busy} onChange={e => { setHistoryKind(e.target.value as 'revisions' | 'captures'); setHistory(null); setBefore(null); setAfter(null); }}><option value="revisions" disabled={!hasProviderRevisions(selected.dataset)}>Provider revisions</option><option value="captures">Capture observations</option></select></label><button disabled={busy} onClick={() => listHistory()}>Load history</button>
      {history && <><p>{history.inventory_complete ? 'Reported history inventory complete' : 'History inventory incomplete; retained entries may have gaps.'}</p>{history.entries.map((entry, index) => <p key={index}>{entry.revision_id} · {entry.captured_at === null ? 'Catalog entry; no retained capture observation' : `observed ${date(entry.captured_at)}`}  · publication {entry.publication_date ?? 'unknown'} · effective {entry.effective_date ?? 'unknown'} <button disabled={busy} onClick={() => void perform(() => read(selected, checkpoint(entry)))}>Read checkpoint</button> <button disabled={busy} onClick={() => setBefore(checkpoint(entry))}>Use as before</button> <button disabled={busy} onClick={() => setAfter(checkpoint(entry))}>Use as after</button></p>)}<button disabled={busy || !history.next_cursor} onClick={() => listHistory(history.next_cursor)}>Next history page</button></>}
      <p>Before: {before ? JSON.stringify(before) : 'not selected'} · After: {after ? JSON.stringify(after) : 'not selected'}</p><button disabled={busy || !before || !after} onClick={compare}>Compare checkpoints</button>
      {comparison && <section aria-label="Checkpoint comparison"><p>{diffProvenance}</p><p>{comparison.additions} added lines · {comparison.deletions} deleted lines</p>{comparison.equal && <p>Texts are equal.</p>}{diffPage?.fragments.map((f, index) => <pre key={index}>{f.patch}</pre>)}<button disabled={busy || !diffPage || diffPage.page + 1 >= comparison.change_pages} onClick={() => void perform(async () => { const page = diffPage!.page + 1; const next = parsePage(await call('text.diff.page', { comparison_id: comparison.comparison_id, view: 'changes', page }), { comparison_id: comparison.comparison_id, view: 'changes', page }); setDiffPage(next); })}>Next diff page</button><button disabled={busy} onClick={() => void perform(clearComparison)}>Clear comparison</button></section>}
    </section>}
    <SourceOffer ready={ready} bridge={bridge} />
  </main>;
}
const root = document.getElementById('root'); if (root) createRoot(root).render(<DatabaseBrowser />);
