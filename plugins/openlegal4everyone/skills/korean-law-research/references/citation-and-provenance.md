# Citation and provenance

Every quoted passage carries a citation grounded in returned source evidence.
For ChatGPT-compatible research, use `search` followed by `fetch` and retain the
returned non-empty `url`, `id`, `title` and compact `metadata`. Place the host's
source citation beside the supported claim when citation metadata is available.
Never invent citation tokens or claim native rendering from a Markdown link.
If the host does not supply citation metadata, link the returned `url` and
state that it is a source link.

Native legal tools may return `references` with exact capture IDs, public URLs
and standard MCP resource links. Use those values verbatim. Fetch a returned
body item ID when available; a metadata-only reference proves provenance, not
legal body text. Use `resources/read` on the returned URI when the client supports
it. Do not construct IDs or substitute HEAD for a historical capture.

For detailed provenance, read the `metadata` object from `database.get` or
`database.get_metadata` for that same capture (times are Unix seconds; show them
as dates with a timezone):

```
<title> <article or section>
Source: <object.provider> (<object.dataset>), ID <object.id>, revision <revision_id>
Capture: <capture_id>, <source_url>
Dates: publication <publication_date>, effective <effective_date>   (omit an absent date)
Retrieved <retrieved_at>, validated <validated_at>
```

Rules:

- Use the official Korean title as returned. Put any translation or alias next
  to it and label it as a translation.
- Keep identifiers verbatim. Do not convert them to numbers or merge records that
  merely look alike.
- Keep promulgation, effective, retrieval and validation dates distinct. Never
  fill a missing date with today or a neighboring record's date.
- When `metadata.metadata.attachment_status` is `incomplete`, say that some attachments
  were unavailable.
- If two sources disagree, report both with their provenance instead of choosing
  one silently.
- SHA-256 digests link stored bytes; they do not prove legal authority.
- Cite the returned capture permalink for retained text. A returned
  `official_browser_url` may be added as a labeled provider link; it can change
  after capture. `source_reference` / `source_url` records upstream provenance
  and is not necessarily a browser-accessible historical page.
- `fetch` metadata is compact and string-valued; it does not contain all native
  freshness fields. Omit absent values or read exact-capture native metadata
  when the answer needs them. A passage is a bounded text unit, not the whole law.

OpenAI documents `search`/`fetch` results with non-empty canonical URLs as eligible
for [citation metadata](https://developers.openai.com/api/docs/mcp#citation-behavior).
Actual rendering remains a host observation; MCP resource links alone do not
guarantee ChatGPT's citation UI.

Upstream data has its own reuse conditions, separate from this plugin's
software license.
