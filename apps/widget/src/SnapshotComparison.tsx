import React, { useEffect, useRef, useState } from 'react';
import type { App } from '@modelcontextprotocol/ext-apps';
import { ComparisonView } from './ComparisonView.tsx';
import { parseCompare, parseDelete, parsePage, type Comparison, type DiffPage, type PageView } from './text-diff-model.ts';
import type { HistoryQuery } from './history-model.ts';

/** The handle stays owned here across history navigation; never put it in URLs/storage. */
export function SnapshotComparison({ bridge, ready, query, before, after, cancelVersion }: { bridge: App; ready: boolean; query: HistoryQuery | null; before: string; after: string; cancelVersion: number }) {
  const [comparison, setComparison] = useState<Comparison | null>(null);
  const [page, setPage] = useState<DiffPage | null>(null);
  const [view, setView] = useState<PageView>('changes');
  const [pageIndex, setPageIndex] = useState(0);
  const [busy, setBusy] = useState(false), [pageBusy, setPageBusy] = useState(false), [error, setError] = useState('');
  const [now, setNow] = useState(Date.now()), [layout, setLayout] = useState('auto');
  const [wide, setWide] = useState(matchMedia('(min-width: 900px)').matches), [dark, setDark] = useState(matchMedia('(prefers-color-scheme: dark)').matches);
  const current = useRef(comparison); current.current = comparison;
  const operation = useRef<{ cancelled: boolean } | null>(null);
  const pending = useRef(new Map<string, Comparison>());
  const live = useRef(true), pageSerial = useRef(0), locked = useRef(false);
  const expired = comparison !== null && now >= comparison.expires_at * 1000;
  useEffect(() => {
    live.current = true;
    const width = matchMedia('(min-width: 900px)'), theme = matchMedia('(prefers-color-scheme: dark)');
    const resize = () => setWide(width.matches), recolor = () => setDark(theme.matches);
    width.addEventListener('change', resize); theme.addEventListener('change', recolor);
    const timer = setInterval(() => setNow(Date.now()), 1000);
    return () => { live.current = false; pageSerial.current++; if (operation.current) operation.current.cancelled = true; clearInterval(timer); width.removeEventListener('change', resize); theme.removeEventListener('change', recolor); };
  }, []);
  const queryKey = JSON.stringify(query);
  useEffect(() => { if (operation.current) { operation.current.cancelled = true; setError('The selection changed or request was cancelled. Waiting for the server response and cleanup.'); } }, [queryKey, before, after, cancelVersion, ready]);
  useEffect(() => {
    if (!comparison || !ready || expired || busy || (view === 'changes' && comparison.change_pages === 0)) { setPage(null); setPageBusy(false); return; }
    const request = ++pageSerial.current;
    const expected = { comparison_id: comparison.comparison_id, view, page: pageIndex };
    setPage(null); setPageBusy(true);
    void bridge.callServerTool({ name: 'get_text_diff_page', arguments: expected }, { timeout: 15000 }).then(result => {
      if (!live.current || pageSerial.current !== request) return;
      const parsed = parsePage(result, expected);
      if (view === 'changes' && parsed.total_pages !== comparison.change_pages) throw new Error('Inconsistent pages');
      setPage(parsed);
    }).catch(() => { if (live.current && pageSerial.current === request) setError('The comparison page could not be loaded. Try again.'); }).finally(() => { if (live.current && pageSerial.current === request) setPageBusy(false); });
    return () => { if (request === pageSerial.current) pageSerial.current++; };
  }, [bridge, comparison, ready, expired, busy, view, pageIndex]);
  async function remove(summary: Comparison) {
    parseDelete(await bridge.callServerTool({ name: 'delete_text_diff', arguments: { comparison_id: summary.comparison_id } }, { timeout: 15000 }));
    pending.current.delete(summary.comparison_id);
    if (current.current?.comparison_id === summary.comparison_id) { current.current = null; if (live.current) { setComparison(null); setPage(null); } }
  }
  async function deleteOwned() {
    const owned = new Map(pending.current);
    if (current.current) owned.set(current.current.comparison_id, current.current);
    for (const summary of owned.values()) await remove(summary);
  }
  async function compare() {
    if (!ready || locked.current || query?.operation !== 'get' || !before || !after) return;
    const expected = { source: query.source, id: query.id, before_snapshot_id: before, after_snapshot_id: after };
    const task = { cancelled: false }; operation.current = task; locked.current = true;
    setBusy(true); setPageBusy(false); setError(''); pageSerial.current++;
    try {
      await deleteOwned();
      if (task.cancelled) return;
      const summary = parseCompare(await bridge.callServerTool({ name: 'demo_compare_record_snapshots', arguments: expected }, { timeout: 35000 }));
      pending.current.set(summary.comparison_id, summary);
      if (task.cancelled || !live.current) {
        await remove(summary);
        if (live.current) setError('The cancelled comparison was deleted.');
        return;
      }
      const origin = summary.origin;
      if (!origin || origin.source !== expected.source || origin.record_id !== expected.id || origin.before.snapshot_id !== before || origin.after.snapshot_id !== after) throw new Error('Snapshot identity mismatch');
      pending.current.delete(summary.comparison_id);
      current.current = summary; setComparison(summary); setView('changes'); setPageIndex(0); setNow(Date.now());
    } catch {
      if (live.current) setError(pending.current.size ? 'The retained comparison could not be opened or deleted. Retry Clear to delete it.' : 'The comparison could not be completed. Retry, or use Clear. Any inaccessible result expires within 10 minutes.');
    } finally { if (operation.current === task) operation.current = null; locked.current = false; if (live.current) setBusy(false); }
  }
  async function clear() {
    if (!ready || locked.current) return;
    locked.current = true; setBusy(true); setPageBusy(false); setError(''); pageSerial.current++;
    try { await deleteOwned(); if (live.current) { setPage(null); setView('changes'); setPageIndex(0); } }
    catch { if (live.current) setError('Clear has not completed. Retry Clear to delete the retained comparison.'); }
    finally { locked.current = false; if (live.current) setBusy(false); }
  }
  return <section aria-label="Snapshot comparison">
    {query?.operation === 'get' && <><p>Compare retained titles and bodies exactly. Differences do not establish legal equivalence.</p><button disabled={!ready || busy || !before || !after} onClick={() => void compare()}>Compare snapshots</button></>}
    {(comparison || pending.current.size > 0 || busy) && <><p className="metadata">Comparison results expire after 10 minutes. Anyone with a comparison handle can read or delete that result. Clear deletes the comparison, not the historical snapshots.</p><button disabled={!ready || busy} onClick={() => void clear()}>Clear comparison</button></>}
    {busy && <p role="status">Processing comparison…</p>}
    {pageBusy && <p role="status">Loading comparison page…</p>}
    {error && <p role="alert" className="error">{error}</p>}
    {comparison && <ComparisonView comparison={comparison} page={page} expired={expired} busy={busy} pageBusy={pageBusy} view={view} layout={layout} wide={wide} dark={dark} error={error} onView={value => { pageSerial.current++; setPage(null); setError(''); setView(value); setPageIndex(0); }} onLayout={setLayout} onPage={setPageIndex} onRetry={() => { setError(''); setComparison({ ...comparison }); }} />}
  </section>;
}
