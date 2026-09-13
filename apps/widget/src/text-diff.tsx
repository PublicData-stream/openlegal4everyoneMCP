import React, { Component, useEffect, useMemo, useRef, useState, type ReactNode } from 'react';
import { createRoot } from 'react-dom/client';
import { App } from '@modelcontextprotocol/ext-apps';
import { SourceOffer } from './SourceOffer.tsx';
import { decodeFile, DiffResponseError, editAsLf, fragmentRows, scalarSegments, splitRows, MAX_TEXT_BYTES, metadata, parseCompare, parseDelete, parsePage, parsePair, parseShow, sourceRange, utf8Length, validateLabel, validateText, type DiffRow, type Comparison, type DiffPage, type Fragment, type PageView, type TextPair } from './text-diff-model.ts';
import './style.css';
import './text-diff.css';

const bridge = new App({ name: 'Text comparison', version: '0.1.0' }, {});
const blank = (): TextPair => ({ before: '', after: '', before_label: 'Before', after_label: 'After' });
const message = (error: unknown, fallback: string) => error instanceof DiffResponseError ? error.message : fallback;
class RenderBoundary extends Component<{ children: ReactNode }, { failed: boolean }> {
  state = { failed: false };
  static getDerivedStateFromError() { return { failed: true }; }
  render() { return this.state.failed ? <p role="alert">This fragment could not be displayed. The original texts remain available in the Before and After views.</p> : this.props.children; }
}
function RowText({ row }: { row: DiffRow }) {
  return <><code>{scalarSegments(row.text, row.ranges).map((segment, index) => {
    // Newline symbols describe the exact original bytes; unchanged LF is implicit.
    const content = segment.text.split(/([\r\n])/).map((part, offset) => part === '\r' ? <span key={offset} className="line-ending" title="Carriage return (CR)">␍</span> : part === '\n' ? (segment.changed ? <span key={offset} className="line-ending" title="Line feed (LF)">␊</span> : null) : part);
    return segment.changed ? <mark className="inline-change" key={index}>{content}</mark> : <React.Fragment key={index}>{content}</React.Fragment>;
  })}</code>{row.noFinalNewline && <span className="no-final-newline">\ No newline at end of file</span>}</>;
}
function SplitCells({ row, side }: { row?: DiffRow; side: 'before' | 'after' }) {
  const kind = row?.kind === '-' ? 'removed' : row?.kind === '+' ? 'added' : 'context';
  return <><td className={`line-number ${kind}`} data-line-num={row?.[side]}>{row?.[side]}</td><td className={`diff-content ${kind}`}>{row && <><span className="diff-sign" aria-hidden="true">{row.kind}</span><RowText row={row} /></>}</td></>;
}
function FragmentView({ fragment, mode, dark }: { fragment: Fragment; mode: 'split' | 'unified'; dark: boolean }) {
  const rows = useMemo(() => fragmentRows(fragment), [fragment]);
  return <section className="diff-fragment" aria-label="Change fragment" data-theme={dark ? 'dark' : 'light'}>
    <p className="metadata">Source lines: before {sourceRange(fragment.before_start, fragment.before_count)} · after {sourceRange(fragment.after_start, fragment.after_count)}. Gutter numbers start again within this fragment.</p>
    <table className={`${mode}-diff-view`} aria-label={`${mode === 'split' ? 'Split' : 'Unified'} changes`}>
      <thead>{mode === 'split' ? <tr><th colSpan={2}>Before</th><th colSpan={2}>After</th></tr> : <tr><th>Before</th><th>After</th><th>Text</th></tr>}</thead>
      <tbody>{mode === 'split' ? splitRows(rows).map((pair, index) => <tr key={index}><SplitCells row={pair.before} side="before" /><SplitCells row={pair.after} side="after" /></tr>) : rows.map((row, index) => <tr key={index} className={row.kind === '-' ? 'removed' : row.kind === '+' ? 'added' : 'context'}><td className="line-number" data-line-num={row.before}>{row.before}</td><td className="line-number" data-line-num={row.after}>{row.after}</td><td className="diff-content"><span className="diff-sign" aria-hidden="true">{row.kind}</span><RowText row={row} /></td></tr>)}</tbody>
    </table>
  </section>;
}
function TextComparison() {
  const [ready, setReady] = useState(false);
  const [pair, setPair] = useState<TextPair | null>(blank);
  const [comparison, setComparison] = useState<Comparison | null>(null);
  const [page, setPage] = useState<DiffPage | null>(null);
  const [view, setView] = useState<PageView>('changes');
  const [pageIndex, setPageIndex] = useState(0);
  const [busy, setBusy] = useState<'compare' | 'delete' | 'load' | null>(null);
  const [pageBusy, setPageBusy] = useState(false);
  const [error, setError] = useState('');
  const [dirty, setDirty] = useState(false);
  const [now, setNow] = useState(Date.now());
  const [wide, setWide] = useState(matchMedia('(min-width: 900px)').matches);
  const [dark, setDark] = useState(matchMedia('(prefers-color-scheme: dark)').matches);
  const [layout, setLayout] = useState('auto');
  const serial = useRef(0);
  const current = useRef(comparison);
  current.current = comparison;
  const pairRef = useRef(pair);
  pairRef.current = pair;
  const live = useRef(true);
  const busyRef = useRef(busy);
  busyRef.current = busy;
  const fileSerial = useRef({ before: 0, after: 0 });
  const suppliedPair = useRef(false);
  // A cancelled RPC may still create a server handle. Keep Clear blocked until
  // its response is consumed and any late handle is deleted or retained for retry.
  const creation = useRef<{ cancelled: boolean } | null>(null);
  const pendingDeletion = useRef(new Map<string, Comparison>());
  const expired = comparison !== null && now >= comparison.expires_at * 1000;
  const mode = layout === 'split' || (layout === 'auto' && wide) ? 'split' : 'unified';
  useEffect(() => {
    const width = matchMedia('(min-width: 900px)');
    const theme = matchMedia('(prefers-color-scheme: dark)');
    const resize = () => setWide(width.matches);
    const recolor = () => setDark(theme.matches);
    width.addEventListener('change', resize); theme.addEventListener('change', recolor);
    const timer = setInterval(() => setNow(Date.now()), 1000);
    return () => { width.removeEventListener('change', resize); theme.removeEventListener('change', recolor); clearInterval(timer); };
  }, []);
  useEffect(() => {
    live.current = true;
    bridge.ontoolinput = input => {
      serial.current++; fileSerial.current.before++; fileSerial.current.after++;
      if (!creation.current) setBusy(null); setPageBusy(false); setPage(null); setComparison(null); setError('');
      try {
        const parsed = parsePair(input.arguments ?? {});
        suppliedPair.current = parsed !== null;
        setPair(parsed ? { ...blank(), ...parsed } : (Object.hasOwn(input.arguments ?? {}, 'comparison_id') ? null : blank()));
        setDirty(false);
      } catch (error) { setError(error instanceof Error ? error.message : 'The supplied texts could not be opened.'); }
    };
    bridge.ontoolresult = result => {
      serial.current++;
      if (!creation.current) setBusy(null); setPageBusy(false); setPage(null); setView('changes'); setPageIndex(0); setError('');
      try {
        const summary = parseShow(result);
        setComparison(summary); setDirty(false); setNow(Date.now());
        if (!summary) setPair(blank());
        else if (!suppliedPair.current) setPair(null);
      } catch (error) { setComparison(null); setError(message(error, 'The comparison could not be opened.')); }
    };
    bridge.ontoolcancelled = () => {
      serial.current++; setPageBusy(false);
      if (creation.current) { creation.current.cancelled = true; setError('The request was cancelled. Waiting for the server response and cleanup.'); }
      else { setBusy(null); setError('The request was cancelled.'); }
    };
    bridge.onclose = () => { if (live.current) { serial.current++; setReady(false); if (!creation.current) setBusy(null); setPageBusy(false); } };
    void bridge.connect(undefined, { timeout: 10000 }).then(() => { if (live.current) setReady(true); }).catch(() => { if (live.current) setError('Connect this text comparison through an MCP Apps host to compare.'); });
    return () => { live.current = false; serial.current++; fileSerial.current.before++; fileSerial.current.after++; void bridge.close(); };
  }, []);
  useEffect(() => {
    if (!comparison || !ready || expired || busy || (view === 'changes' && comparison.change_pages === 0)) {
      setPage(null); setPageBusy(false);
      return;
    }
    const request = ++serial.current;
    const expected = { comparison_id: comparison.comparison_id, view, page: pageIndex };
    setPage(null); setPageBusy(true);
    void bridge.callServerTool({ name: 'get_text_diff_page', arguments: expected }, { timeout: 15000 }).then(result => {
      if (!live.current || request !== serial.current) return;
      const parsed = parsePage(result, expected);
      if (view === 'changes' && parsed.total_pages !== comparison.change_pages) throw new DiffResponseError('The server returned inconsistent comparison pages.');
      setPage(parsed);
    }).catch(error => { if (live.current && request === serial.current) setError(message(error, 'The page could not be loaded. Try again.')); }).finally(() => { if (live.current && request === serial.current) setPageBusy(false); });
    return () => { if (request === serial.current) serial.current++; };
  }, [comparison, ready, expired, busy, view, pageIndex]);
  function updatePair(change: Partial<TextPair>) {
    setPair(previous => ({ ...(previous ?? blank()), ...change })); setDirty(true); setError('');
  }
  async function readFile(side: 'before' | 'after', file: File | undefined) {
    if (!file) return;
    const request = ++fileSerial.current[side];
    try {
      if (file.size > MAX_TEXT_BYTES) throw new Error('Each file must fit within 1 MiB of UTF-8.');
      const text = decodeFile(await file.arrayBuffer());
      if (!live.current || request !== fileSerial.current[side] || busyRef.current) return;
      updatePair({ [side]: text });
    } catch (error) { if (live.current && request === fileSerial.current[side]) setError(error instanceof Error ? error.message : 'The file could not be read.'); }
  }
  async function remove(summary: Comparison, request: number): Promise<boolean> {
    const result = await bridge.callServerTool({ name: 'delete_text_diff', arguments: { comparison_id: summary.comparison_id } }, { timeout: 15000 });
    if (!live.current || request !== serial.current) return false;
    parseDelete(result);
    setComparison(previous => previous?.comparison_id === summary.comparison_id ? null : previous);
    if (current.current?.comparison_id === summary.comparison_id) setPage(null);
    return true;
  }
  async function compare() {
    if (!pairRef.current || !ready || busyRef.current || creation.current) return;
    const input = { ...pairRef.current };
    try { validateText(input.before); validateText(input.after); validateLabel(input.before_label ?? ''); validateLabel(input.after_label ?? ''); }
    catch (error) { setError((error as Error).message); return; }
    const request = ++serial.current;
    const operation = { cancelled: false };
    creation.current = operation;
    fileSerial.current.before++; fileSerial.current.after++;
    setBusy('compare'); setPageBusy(false); setError('');
    try {
      for (const retained of pendingDeletion.current.values()) {
        if (!(await remove(retained, request))) return;
        pendingDeletion.current.delete(retained.comparison_id);
      }
      if (current.current && !(await remove(current.current, request))) return;
      if (operation.cancelled || request !== serial.current) return;
      const result = await bridge.callServerTool({ name: 'compare_texts', arguments: { ...input } }, { timeout: 30000 });
      if (!live.current) return;
      const summary = parseCompare(result);
      if (operation.cancelled || request !== serial.current) {
        pendingDeletion.current.set(summary.comparison_id, summary);
        try {
          const deletion = await bridge.callServerTool({ name: 'delete_text_diff', arguments: { comparison_id: summary.comparison_id } }, { timeout: 15000 });
          parseDelete(deletion);
          pendingDeletion.current.delete(summary.comparison_id);
          if (live.current && operation.cancelled) setError('The request was cancelled. Its retained comparison was deleted.');
        } catch {
          if (live.current) {
            if (!current.current) setComparison(summary);
            setError('The cancelled comparison could not be deleted. Retry Clear to delete the retained text.');
          }
        }
        return;
      }
      setComparison(summary); setView('changes'); setPageIndex(0); setDirty(false); setNow(Date.now());
    } catch (error) {
      if (live.current && operation.cancelled) setError('The request was cancelled. If the host lost its response, any retained text expires within 10 minutes.');
      else if (live.current && request === serial.current) setError(message(error, 'The comparison could not be completed. Try again.'));
    }
    finally {
      if (creation.current === operation) { creation.current = null; if (live.current) setBusy(null); }
    }
  }
  async function clear() {
    if (busyRef.current || creation.current) return;
    if ((current.current || pendingDeletion.current.size) && !ready) { setError('Reconnect the host to delete the retained comparison, or wait for its expiry.'); return; }
    const request = ++serial.current;
    fileSerial.current.before++; fileSerial.current.after++;
    setBusy('delete'); setPageBusy(false); setError('');
    try {
      const retained = new Map(pendingDeletion.current);
      if (current.current) retained.set(current.current.comparison_id, current.current);
      for (const summary of retained.values()) {
        if (!(await remove(summary, request))) return;
        pendingDeletion.current.delete(summary.comparison_id);
      }
      setPair(blank()); setPage(null); setView('changes'); setPageIndex(0); setDirty(false);
    } catch (error) { if (live.current && request === serial.current) setError(`${message(error, 'The retained comparison could not be deleted.')} Clear has not completed; retry Clear.`); }
    finally { if (live.current && request === serial.current) setBusy(null); }
  }
  async function loadSources() {
    const summary = current.current;
    if (!summary || !ready || busyRef.current || expired) return;
    const request = ++serial.current;
    setBusy('load'); setPageBusy(false); setError('');
    try {
      const loaded: TextPair = { before: '', after: '', before_label: summary.before.label, after_label: summary.after.label };
      for (const side of ['before', 'after'] as const) {
        let total = 1;
        for (let index = 0; index < total; index++) {
          const expected = { comparison_id: summary.comparison_id, view: side, page: index };
          const result = await bridge.callServerTool({ name: 'get_text_diff_page', arguments: expected }, { timeout: 15000 });
          if (!live.current || request !== serial.current) return;
          const chunk = parsePage(result, expected);
          if (index > 0 && chunk.total_pages !== total) throw new DiffResponseError('The server returned inconsistent source pages.');
          total = chunk.total_pages;
          loaded[side] += chunk.text!;
          if (utf8Length(loaded[side]) > summary[side].bytes) throw new DiffResponseError('The server returned an inconsistent source text.');
        }
        if (utf8Length(loaded[side]) !== summary[side].bytes) throw new DiffResponseError('The server returned an incomplete source text.');
        validateText(loaded[side]);
      }
      setPair(loaded); setDirty(false);
    } catch (error) { if (live.current && request === serial.current) setError(message(error, 'The original texts could not be loaded. Try again.')); }
    finally { if (live.current && request === serial.current) setBusy(null); }
  }
  function changeView(next: PageView) { serial.current++; setError(''); setView(next); setPageIndex(0); setPage(null); }
  return <main className="text-comparison">
    <header><p className="eyebrow">openlegal4everyone</p><h1>Text comparison</h1><p>Compare supplied text with character highlights. Differences do not establish legal equivalence.</p><p className="metadata">Texts are sent to the server through your host and retained for up to 10 minutes. Anyone with the comparison handle can read them until deletion or expiry.</p></header>
    {pair ? <section aria-label="Texts to compare" className="text-inputs">
      {(['before', 'after'] as const).map(side => <div key={side} className="text-input">
        <label>{side === 'before' ? 'Before label' : 'After label'}<input value={pair[`${side}_label`] ?? ''} disabled={!!busy} onChange={event => updatePair({ [`${side}_label`]: event.target.value })} /></label>
        <label>{side === 'before' ? 'Before text' : 'After text'}<textarea aria-label={side === 'before' ? 'Before text' : 'After text'} value={pair[side]} spellCheck={false} disabled={!!busy} readOnly={pair[side].includes('\r')} onChange={event => { fileSerial.current[side]++; updatePair({ [side]: event.target.value }); }} onPaste={event => {
          event.preventDefault();
          if (pair[side].includes('\r')) { setError('Choose Edit as LF before pasting into text containing CR line endings.'); return; }
          const text = event.clipboardData.getData('text/plain');
          const field = event.currentTarget;
          const replacement = pair[side].slice(0, field.selectionStart) + text + pair[side].slice(field.selectionEnd);
          try { validateText(replacement); fileSerial.current[side]++; updatePair({ [side]: replacement }); } catch (error) { setError((error as Error).message); }
        }} /></label>
        <p className="metadata">{utf8Length(pair[side]).toLocaleString()} / 1,048,576 UTF-8 bytes. At most 100,000 lines; 16 KiB per line.</p>
        {pair[side].includes('\r') && <div className="newline-notice"><p>Original CR bytes are preserved. The text preview is read-only because editing in a browser uses LF.</p><button disabled={!!busy} onClick={() => updatePair({ [side]: editAsLf(pair[side]) })}>Edit {side} as LF</button></div>}
        <label>Load {side} UTF-8 file<input type="file" disabled={!!busy} onChange={event => { const file = event.currentTarget.files?.[0]; event.currentTarget.value = ''; void readFile(side, file); }} /></label>
      </div>)}
    </section> : <p><button disabled={!ready || !!busy || pageBusy || expired} onClick={() => void loadSources()}>Load original texts for editing</button></p>}
    <div className="diff-actions"><button className="submit" disabled={!ready || !!busy || !pair} onClick={() => void compare()}>Compare</button><button disabled={!!busy} onClick={() => void clear()}>Clear</button></div>
    <div role="status" aria-live="polite">{busy === 'compare' ? 'Comparing texts…' : busy === 'delete' ? 'Deleting retained comparison…' : busy === 'load' ? 'Loading original texts…' : pageBusy ? 'Loading comparison page…' : !ready && !error ? 'Connecting to host…' : ''}</div>
    {error && <p role="alert" className="error">{error}</p>}
    {comparison && <section className="comparison-result" aria-label="Comparison result" aria-busy={!!busy || pageBusy}>
      <h2>{dirty ? 'Previous comparison' : 'Comparison result'}</h2>
      {dirty && <p>Inputs have changed. Compare again to update these results.</p>}
      <p>{comparison.equal ? 'The supplied texts are identical.' : `${comparison.additions} added lines · ${comparison.deletions} deleted lines`}</p>
      <p className="metadata">{expired ? 'This comparison has expired. Compare again to create a new result.' : `Expires at ${new Date(comparison.expires_at * 1000).toISOString()}. Clear deletes the retained result.`}</p>
      <dl className="text-metadata"><dt>{comparison.before.label}</dt><dd>{metadata(comparison.before)}</dd><dt>{comparison.after.label}</dt><dd>{metadata(comparison.after)}</dd></dl>
      {!expired && <>
        <div className="diff-actions"><label>View<select aria-label="View" disabled={!!busy || pageBusy} value={view} onChange={event => changeView(event.target.value as PageView)}><option value="changes">Changes</option><option value="before">Before text</option><option value="after">After text</option></select></label><label>Layout<select aria-label="Layout" value={layout} onChange={event => setLayout(event.target.value)}><option value="auto">Automatic</option><option value="split">Split</option><option value="unified">Unified</option></select></label></div>
        {page && view === 'changes' && <RenderBoundary key={`${comparison.comparison_id}:${page.page}`}>{page.fragments.map((fragment, index) => <FragmentView key={index} fragment={fragment} mode={mode} dark={dark} />)}</RenderBoundary>}
        {page && view !== 'changes' && <><p className="metadata">Exact source chunk {page.page + 1} of {page.total_pages}. Chunks may divide a line.</p><pre className="source-chunk">{page.text}</pre></>}
        {page && <nav aria-label="Comparison pages"><button disabled={!!busy || pageBusy || page.page === 0} onClick={() => setPageIndex(page.page - 1)}>Previous page</button><span>Page {page.page + 1} of {page.total_pages}</span><button disabled={!!busy || pageBusy || page.page + 1 >= page.total_pages} onClick={() => setPageIndex(page.page + 1)}>Next page</button></nav>}
        {!page && !pageBusy && error && <button disabled={!!busy} onClick={() => { setError(''); setComparison({ ...comparison }); }}>Retry page</button>}
      </>}
    </section>}
    <SourceOffer ready={ready} bridge={bridge} />
  </main>;
}
const root = document.getElementById('root');
if (root) createRoot(root).render(<TextComparison />);
