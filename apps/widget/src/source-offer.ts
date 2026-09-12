/** Revalidate the operator's inert metadata before displaying or opening it. */
export function validateSourceUrl(value: string | null): string | null {
  if (!value || new TextEncoder().encode(value).byteLength > 2048 || /[\u0000-\u0020\u007f-\u009f]/u.test(value)) return null;
  try {
    const url = new URL(value);
    const authority = /^https:\/\/([^/?#]+)/i.exec(value)?.[1];
    if (!authority || authority.includes('@') || value.includes('\\') || url.protocol !== 'https:' || !url.hostname || url.username || url.password) return null;
    return value;
  } catch { return null; }
}

export function readSourceUrl(document: Document): string | null {
  const entries = document.querySelectorAll('meta[name="openlegal-source-url"]');
  return entries.length === 1 ? validateSourceUrl(entries[0].getAttribute('content')) : null;
}
