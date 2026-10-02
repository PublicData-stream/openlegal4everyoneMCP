---
name: legal-text-comparison
description: Compare two user-supplied texts such as contract drafts, clause revisions or proposed amendments, page through the differences and apply a patch, using the OpenLegal text tools. Use when the user pastes two versions and asks what changed, 비교, 대조, 수정본 차이, or asks to apply changes.
license: AGPL-3.0-only
metadata:
  version: "0.1.0"
---

# Legal text comparison

Compare supplied text exactly with the OpenLegal `text.*` tools. The server
compares lines and Unicode scalars; it does not interpret meaning.

## Ground rules

- Quote changed lines exactly as the tool returns them. Do not paraphrase a
  change as if it were the text.
- A difference report is not legal advice and does not establish legal effect.
- Supplied text is retained only temporarily. Delete handles when finished.

## Workflow

1. **Upload long text (optional).** For text too long to send inline, call
   `text.attachment.upload` sequentially: the first call sends `kind: "text"`,
   `total_bytes`, the first `chunk` and `final`; later calls send the
   `attachment_id`, the UTF-8 byte `offset`, the next `chunk` and `final`. Use
   the returned attachment object. Texts are limited to 1 MiB each.
2. **Compare.** Call `text.diff` with `before` and `after` (strings or
   attachment references) and optional `before_label` / `after_label`. The
   result has `comparison` (summary with `comparison_id`), `patch` (a sealed
   complete unified patch) and `explanation`.
3. **Read every change.** Call `text.diff.page` with the `comparison_id`,
   `view: "changes"` and `page` from 0 to `total_pages - 1`. Use
   `view: "before"` or `"after"` to read the original text in chunks.
4. **Show (optional).** `text.diff.show` with the `comparison_id` opens the
   comparison widget; `{}` opens an empty editor.
5. **Apply a patch (optional).** Call `text.apply_patch` with `target` and the
   `patch` from step 2 (or another returned patch attachment). It returns a new
   `result` attachment; nothing is overwritten. Read it with
   `text.attachment.read`.
6. **Clean up.** Call `text.diff.delete` for each comparison and
   `text.attachment.delete` for each attachment you created. Handles also expire
   ten minutes after creation.

## Answer shape

Lead with a short count of additions and deletions, then list each change with
its line numbers and exact text. Mention if the texts were equal.
