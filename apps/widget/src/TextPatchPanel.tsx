import React, { useEffect, useRef, useState } from 'react';
import type { App } from '@modelcontextprotocol/ext-apps';
import { attachmentChunks, decodeFile, decodePatchFile, editAsLf, parseAttachmentChunk, parseAttachmentUpload, parseDelete, parsePatchResult, utf8Length, validatePatch, validateText, type Attachment } from './text-diff-model.ts';

export function TextPatchPanel({ bridge, ready }: { bridge: App; ready: boolean }) {
  const [target, setTarget] = useState('');
  const [patch, setPatch] = useState('');
  const [output, setOutput] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const working = useRef(false);
  const alive = useRef(true);
  const retained = useRef(new Set<string>());
  const versions = useRef({ target: 0, patch: 0 });
  useEffect(() => { alive.current = true; return () => { alive.current = false; }; }, []);
  async function remove(id: string) {
    parseDelete(await bridge.callServerTool({ name: 'text.attachment.delete', arguments: { attachment_id: id } }, { timeout: 15000 }));
    retained.current.delete(id);
  }
  async function upload(text: string, kind: 'text' | 'patch'): Promise<Attachment> {
    let previous: Attachment | null = null;
    const totalBytes = utf8Length(text);
    for (const part of attachmentChunks(text)) {
      const args = previous ? { attachment_id: previous.attachment_id, ...part } : { kind, total_bytes: totalBytes, ...part };
      const current = parseAttachmentUpload(await bridge.callServerTool({ name: 'text.attachment.upload', arguments: args }, { timeout: 15000 }));
      retained.current.add(current.attachment_id);
      if (current.kind !== kind || current.total_bytes !== totalBytes || current.committed_bytes !== part.offset + utf8Length(part.chunk) || current.sealed !== part.final || (previous && (current.attachment_id !== previous.attachment_id || current.expires_at !== previous.expires_at))) throw new Error('The server returned an inconsistent upload.');
      previous = current;
      if (!alive.current) throw new Error('The app closed during upload.');
    }
    if (!previous) throw new Error('The attachment could not be created.');
    return previous;
  }
  async function apply() {
    if (!ready || working.current) return;
    try { validateText(target); validatePatch(patch); }
    catch (e) { setError((e as Error).message); return; }
    working.current = true; setBusy(true); setError(''); setOutput(null);
    versions.current.target++; versions.current.patch++;
    try {
      for (const id of [...retained.current]) await remove(id);
      const before = await upload(target, 'text');
      const changes = await upload(patch, 'patch');
      const result = parsePatchResult(await bridge.callServerTool({ name: 'text.apply_patch', arguments: { target: { attachment_id: before.attachment_id }, patch: { attachment_id: changes.attachment_id } } }, { timeout: 30000 }));
      retained.current.add(result.attachment_id);
      let text = ''; let offset = 0;
      for (;;) {
        const page = parseAttachmentChunk(await bridge.callServerTool({ name: 'text.attachment.read', arguments: { attachment_id: result.attachment_id, offset } }, { timeout: 15000 }), result, offset);
        text += page.text; offset = page.next_offset;
        if (offset > result.total_bytes) throw new Error('The server returned excessive patch output.');
        if (page.complete) break;
        if (!alive.current) throw new Error('The app closed while loading the result.');
      }
      validateText(text);
      if (alive.current) setOutput(text);
    } catch (e) { if (alive.current) setError(e instanceof Error ? e.message : 'Patch application failed.'); }
    finally {
      let cleanupFailed = false;
      for (const id of [...retained.current]) { try { await remove(id); } catch { cleanupFailed = true; } }
      if (alive.current) { setBusy(false); if (cleanupFailed) setError('Some temporary attachments could not be deleted. Use Clear patch to retry; fixed expiry remains active.'); }
      working.current = false;
    }
  }
  async function clear() {
    if (working.current) return;
    working.current = true; setBusy(true); setError('');
    versions.current.target++; versions.current.patch++;
    try { for (const id of [...retained.current]) await remove(id); setTarget(''); setPatch(''); setOutput(null); }
    catch { setError('Temporary attachment deletion failed. Retry Clear patch.'); }
    finally { working.current = false; if (alive.current) setBusy(false); }
  }
  async function load(kind: 'target' | 'patch', file?: File) {
    if (!file) return;
    const version = ++versions.current[kind];
    try {
      if (file.size > (kind === 'patch' ? 8 : 1) * 1048576) throw new Error('The selected file exceeds its byte limit.');
      const buffer = await file.arrayBuffer();
      const text = kind === 'patch' ? decodePatchFile(buffer) : decodeFile(buffer);
      if (alive.current && !working.current && versions.current[kind] === version) (kind === 'patch' ? setPatch : setTarget)(text);
    } catch (e) { if (alive.current) setError((e as Error).message); }
  }
  return <section aria-label="Apply a text patch" className="text-patch">
    <h2>Apply a text patch</h2><p>Apply a single-file unified patch at exact positions. The result preserves UTF-8 and line endings. Files upload only when you choose Apply patch.</p>
    <div className="text-inputs">{(['target', 'patch'] as const).map(kind => {
      const value = kind === 'target' ? target : patch; const update = kind === 'target' ? setTarget : setPatch;
      return <div className="text-input" key={kind}><label>{kind === 'target' ? 'Patch target' : 'Unified patch'}<textarea aria-label={kind === 'target' ? 'Patch target' : 'Unified patch'} value={value} disabled={busy} readOnly={value.includes('\r')} spellCheck={false} onChange={e => { versions.current[kind]++; update(e.target.value); }} /></label>
      {value.includes('\r') && <button disabled={busy} onClick={() => update(editAsLf(value))}>Edit {kind} as LF</button>}
      <label>Load {kind} file<input type="file" disabled={busy} onChange={e => { void load(kind, e.currentTarget.files?.[0]); e.currentTarget.value = ''; }} /></label></div>;
    })}</div>
    <div className="diff-actions"><button disabled={!ready || busy} onClick={() => void apply()}>Apply patch</button><button disabled={busy} onClick={() => void clear()}>Clear patch</button></div>
    {busy && <p role="status">Applying patch and cleaning up temporary attachments…</p>}{error && <p role="alert">{error}</p>}
    {output !== null && <div><h3>Patched text</h3><pre aria-label="Patched text" className="source-chunk">{output}</pre><p>{utf8Length(output)} UTF-8 bytes. This local preview remains after server attachments are deleted.</p></div>}
  </section>;
}
