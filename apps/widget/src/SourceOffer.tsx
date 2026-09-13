import React, { useState } from 'react';
import type { App } from '@modelcontextprotocol/ext-apps';
import { readSourceUrl } from './source-offer.ts';

const sourceUrl = readSourceUrl(document);
export function SourceOffer({ ready, bridge }: { ready: boolean; bridge: App }) {
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
