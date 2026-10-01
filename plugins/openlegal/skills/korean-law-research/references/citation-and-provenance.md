# Citation and provenance

Every quoted passage carries a citation built only from the `metadata` object that
`database.get` or `database.get_metadata` returns (times are Unix seconds; show
them as dates with a timezone):

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

Upstream data has its own reuse conditions, separate from this plugin's
software license.
