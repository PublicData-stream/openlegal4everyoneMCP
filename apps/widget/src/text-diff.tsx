import React, { useEffect, useRef, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { App } from '@modelcontextprotocol/ext-apps';
import { SourceOffer } from './SourceOffer.tsx';
import { decodeFile, DiffResponseError, editAsLf, MAX_TEXT_BYTES, parseDiff, parseDelete, parsePage, parsePair, parseShow, utf8Length, validateLabel, validateText, type Comparison, type DiffPage, type PageView, type TextPair } from './text-diff-model.ts';
import './style.css';
import './text-diff.css';
import { TextPatchPanel } from './TextPatchPanel.tsx';
import { ComparisonView } from './ComparisonView.tsx';

const bridge = new App({ name: 'Text comparison', version: '0.1.0' }, {});
const blank = (): TextPair => ({ before: '', after: '', before_label: 'Before', after_label: 'After' });
const message = (error: unknown, fallback: string) => error instanceof DiffResponseError ? error.message : fallback;
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
  const initialHandle = useRef<string | null>(null);
  // A cancelled RPC may still create a server handle. Keep Clear blocked until
  // its response is consumed and any late handle is deleted or retained for retry.
  const creation = useRef<{ cancelled: boolean } | null>(null);
  const pendingDeletion = useRef(new Map<string, Comparison>());
  const pendingPatches = useRef(new Set<string>());
  const expired = comparison !== null && now >= comparison.expires_at * 1000;
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
      if (current.current) pendingDeletion.current.set(current.current.comparison_id, current.current);
      serial.current++; fileSerial.current.before++; fileSerial.current.after++;
      initialHandle.current = null;
      if (!creation.current) setBusy(null); setPageBusy(false); setPage(null); setComparison(null); setError('');
      try {
        const parsed = parsePair(input.arguments ?? {});
        suppliedPair.current = parsed !== null;
        const handle = input.arguments?.comparison_id;
        if (!parsed && typeof handle === 'string' && /^[0-9a-f]{64}$/.test(handle)) initialHandle.current = handle;
        setPair(parsed ? { ...blank(), ...parsed } : (Object.hasOwn(input.arguments ?? {}, 'comparison_id') ? null : blank()));
        setDirty(false);
      } catch (error) { setError(error instanceof Error ? error.message : 'The supplied texts could not be opened.'); }
    };
    bridge.ontoolresult = result => {
      if (current.current) pendingDeletion.current.set(current.current.comparison_id, current.current);
      serial.current++;
      if (!creation.current) setBusy(null); setPageBusy(false); setPage(null); setView('changes'); setPageIndex(0); setError('');
      try {
        const summary = parseShow(result);
        if (summary?.origin && initialHandle.current !== summary.comparison_id) {
          pendingDeletion.current.set(summary.comparison_id, summary);
          throw new DiffResponseError('The supplied-text response contained unexpected historical metadata. Use Clear to delete its retained result.');
        }
        if (summary) pendingDeletion.current.delete(summary.comparison_id);
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
    void bridge.callServerTool({ name: 'text.diff.page', arguments: expected }, { timeout: 15000 }).then(result => {
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
    parseDelete(await bridge.callServerTool({ name: 'text.diff.delete', arguments: { comparison_id: summary.comparison_id } }, { timeout: 15000 }));
    // Record server cleanup even when a newer host input invalidated the display.
    pendingDeletion.current.delete(summary.comparison_id);
    if (!live.current || request !== serial.current) return false;
    setComparison(previous => previous?.comparison_id === summary.comparison_id ? null : previous);
    if (current.current?.comparison_id === summary.comparison_id) setPage(null);
    return true;
  }
  async function removePatch(id: string) {
    parseDelete(await bridge.callServerTool({ name: 'text.attachment.delete', arguments: { attachment_id: id } }, { timeout: 15000 }));
    pendingPatches.current.delete(id);
  }
  async function cleanupCreated() {
    // Try every capability; a failed deletion must not prevent independent cleanup.
    let failed = false;
    for (const id of [...pendingPatches.current]) { try { await removePatch(id); } catch { failed = true; } }
    for (const summary of [...pendingDeletion.current.values()]) {
      try {
        parseDelete(await bridge.callServerTool({ name: 'text.diff.delete', arguments: { comparison_id: summary.comparison_id } }, { timeout: 15000 }));
        pendingDeletion.current.delete(summary.comparison_id);
        if (live.current) setComparison(previous => previous?.comparison_id === summary.comparison_id ? null : previous);
      } catch { failed = true; }
    }
    return !failed;
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
    let received = false;
    try {
      if (!(await cleanupCreated())) throw new DiffResponseError('Temporary text could not be deleted. Retry Clear before comparing again.');
      if (!live.current || operation.cancelled || request !== serial.current) return;
      if (current.current) {
        const previous = current.current;
        pendingDeletion.current.set(previous.comparison_id, previous);
        if (!(await remove(previous, request))) return;
      }
      if (operation.cancelled || request !== serial.current) return;
      const result = await bridge.callServerTool({ name: 'text.diff', arguments: { ...input } }, { timeout: 30000 });
      received = true;
      const { comparison: summary, patch } = parseDiff(result, {
        comparison: value => pendingDeletion.current.set(value.comparison_id, value),
        patch: value => pendingPatches.current.add(value.attachment_id),
      });
      // A newly supplied pair cannot acquire a server-history association.
      if (summary.origin) throw new DiffResponseError('The supplied-text response contained unexpected historical metadata.');
      if (!live.current || operation.cancelled || request !== serial.current) {
        const cleaned = await cleanupCreated();
        if (live.current && operation.cancelled) setError(cleaned
          ? 'The request was cancelled. Its retained comparison was deleted.'
          : 'The cancelled comparison could not be deleted. Retry Clear to delete the retained text.');
        return;
      }
      // This comparison becomes visible; its unused patch is deleted promptly.
      pendingDeletion.current.delete(summary.comparison_id);
      setComparison(summary); setView('changes'); setPageIndex(0); setDirty(false); setNow(Date.now());
      try { await removePatch(patch.attachment_id); }
      catch { if (live.current && request === serial.current) setError('The unused patch could not be deleted. Retry Clear before comparing again.'); }
      // Cancellation or a new host input may arrive while patch deletion is pending.
      if (!live.current || operation.cancelled || request !== serial.current) {
        pendingDeletion.current.set(summary.comparison_id, summary);
        const cleaned = await cleanupCreated();
        if (live.current && operation.cancelled) {
          if (cleaned) setComparison(previous => previous?.comparison_id === summary.comparison_id ? null : previous);
          setError(cleaned ? 'The request was cancelled. Its retained comparison was deleted.' : 'The cancelled comparison could not be deleted. Retry Clear to delete the retained text.');
        }
      }
    } catch (error) {
      // Malformed responses can still contain valid, independently owned handles.
      const cleaned = received ? await cleanupCreated() : pendingDeletion.current.size === 0 && pendingPatches.current.size === 0;
      if (live.current && (operation.cancelled || request === serial.current)) {
        const detail = message(error, operation.cancelled ? 'The request was cancelled. If the host lost its response, any retained text expires within 10 minutes.' : 'The comparison could not be completed. Try again.');
        setError(cleaned || detail.includes('Retry Clear') ? detail : `${detail} Retry Clear to delete retained text.`);
      }
    }
    finally {
      if (creation.current === operation) { creation.current = null; if (live.current) setBusy(null); }
    }
  }
  async function clear() {
    if (busyRef.current || creation.current) return;
    if ((current.current || pendingDeletion.current.size || pendingPatches.current.size) && !ready) { setError('Reconnect the host to delete the retained text, or wait for its expiry.'); return; }
    const request = ++serial.current;
    fileSerial.current.before++; fileSerial.current.after++;
    setBusy('delete'); setPageBusy(false); setError('');
    if (current.current) pendingDeletion.current.set(current.current.comparison_id, current.current);
    try {
      if (!(await cleanupCreated())) throw new DiffResponseError('The retained text could not be deleted.');
      if (!live.current || request !== serial.current) return;
      setComparison(null); setPair(blank()); setPage(null); setView('changes'); setPageIndex(0); setDirty(false);
    } catch (error) { if (live.current && request === serial.current) setError(`${message(error, 'The retained text could not be deleted.')} Clear has not completed; retry Clear.`); }
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
          const result = await bridge.callServerTool({ name: 'text.diff.page', arguments: expected }, { timeout: 15000 });
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
    {comparison && <ComparisonView comparison={comparison} page={page} expired={expired} busy={!!busy} pageBusy={pageBusy} dirty={dirty} view={view} layout={layout} wide={wide} dark={dark} error={error} onView={changeView} onLayout={setLayout} onPage={setPageIndex} onRetry={() => { setError(''); setComparison({ ...comparison }); }} />}
    <TextPatchPanel ready={ready} bridge={bridge} />
    <SourceOffer ready={ready} bridge={bridge} />
  </main>;
}
const root = document.getElementById('root');
if (root) createRoot(root).render(<TextComparison />);
