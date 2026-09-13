/** Shared bounded read-only rendering; all differences are computed by Rust. */
import React, { Component, useMemo, type ReactNode } from 'react';
import { fragmentRows, scalarSegments, splitRows, metadata, sourceRange, type DiffRow, type Fragment, type Comparison, type DiffPage, type PageView } from './text-diff-model.ts';
import { snapshotLabel } from './history-model.ts';
import './text-diff.css';

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

export function ComparisonOrigin({ comparison }: { comparison: Comparison }) {
  const origin = comparison.origin;
  if (!origin) return null;
  return <aside aria-label="Retained snapshot comparison" className="snapshot-origin">
    <p>Historical observations of synthetic record {origin.source} / {origin.record_id}. These are local observations, not legal revisions.</p>
    <p>Compared text: title, two LF characters, then body. No normalization.</p>
    <dl>{(['before', 'after'] as const).map(side => <React.Fragment key={side}>
      <dt>{side === 'before' ? 'Before snapshot' : 'After snapshot'}</dt><dd>{origin[side].snapshot_id}</dd>
      <dt>{side === 'before' ? 'Before observation' : 'After observation'}</dt><dd>{snapshotLabel(origin[side])} · schema {origin[side].schema_version}</dd>
      <dt>{side === 'before' ? 'Before payload SHA-256' : 'After payload SHA-256'}</dt><dd>{origin[side].payload_sha256}</dd>
    </React.Fragment>)}</dl>
  </aside>;
}
export interface ComparisonViewProps {
  comparison: Comparison; page: DiffPage | null; expired: boolean; busy: boolean; pageBusy: boolean;
  dirty?: boolean; view: PageView; layout: string; wide: boolean; dark: boolean; error: string;
  onView: (view: PageView) => void; onLayout: (layout: string) => void; onPage: (page: number) => void; onRetry: () => void;
}
export function ComparisonView({ comparison, page, expired, busy, pageBusy, dirty, view, layout, wide, dark, error, onView, onLayout, onPage, onRetry }: ComparisonViewProps) {
  const mode = layout === 'split' || (layout === 'auto' && wide) ? 'split' : 'unified';
  return <section className="comparison-result" aria-label="Comparison result" aria-busy={busy || pageBusy}>
    <h2>{dirty ? 'Previous comparison' : 'Comparison result'}</h2>
    {dirty && <p>Inputs have changed. Compare again to update these results.</p>}
    <ComparisonOrigin comparison={comparison} />
    <p>{comparison.equal ? 'The supplied texts are identical.' : `${comparison.additions} added lines · ${comparison.deletions} deleted lines`}</p>
    <p className="metadata">{expired ? 'This comparison has expired. Compare again to create a new result.' : `Expires at ${new Date(comparison.expires_at * 1000).toISOString()}. Clear deletes the retained result.`}</p>
    <dl className="text-metadata"><dt>{comparison.before.label}</dt><dd>{metadata(comparison.before)}</dd><dt>{comparison.after.label}</dt><dd>{metadata(comparison.after)}</dd></dl>
    {!expired && <>
      <div className="diff-actions"><label>View<select aria-label="View" disabled={busy || pageBusy} value={view} onChange={event => onView(event.target.value as PageView)}><option value="changes">Changes</option><option value="before">Before text</option><option value="after">After text</option></select></label><label>Layout<select aria-label="Layout" value={layout} onChange={event => onLayout(event.target.value)}><option value="auto">Automatic</option><option value="split">Split</option><option value="unified">Unified</option></select></label></div>
      {page && view === 'changes' && <RenderBoundary key={`${comparison.comparison_id}:${page.page}`}>{page.fragments.map((fragment, index) => <FragmentView key={index} fragment={fragment} mode={mode} dark={dark} />)}</RenderBoundary>}
      {page && view !== 'changes' && <><p className="metadata">Exact source chunk {page.page + 1} of {page.total_pages}. Chunks may divide a line.</p><pre className="source-chunk">{page.text}</pre></>}
      {page && <nav aria-label="Comparison pages"><button disabled={busy || pageBusy || page.page === 0} onClick={() => onPage(page.page - 1)}>Previous page</button><span>Page {page.page + 1} of {page.total_pages}</span><button disabled={busy || pageBusy || page.page + 1 >= page.total_pages} onClick={() => onPage(page.page + 1)}>Next page</button></nav>}
      {!page && !pageBusy && error && <button disabled={busy} onClick={onRetry}>Retry page</button>}
    </>}
  </section>;
}
