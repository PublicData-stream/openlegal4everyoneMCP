# `database.query` syntax

| Input | Meaning |
| --- | --- |
| `민법 손해배상` | Both terms (implicit AND) |
| `A AND B`, `A OR B` | Boolean operators; only uppercase `AND`, `OR`, `NOT` are operators |
| `NOT A` or `-A` | Exclude |
| `(A OR B) C` | Grouping |
| `"불법행위로 인한"` | Exact, case-sensitive source substring |
| `'A B'` | Both terms, any order or distance |
| `in:title:민법` | Scope to the title |
| `in:body:(A OR B)` | Scope a group to the body |
| `in:case_number:2020다12345` | Scope to the case number |
| `손해*` | Prefix |

- Bare `title:`, `body:` and `case_number:` are invalid; always use `in:`.
- Korean words are analyzed by two morphological engines (Lindera and
  MeCab-Ko). `A AND B` requires one engine to match both; `NOT B` excludes a
  match by either engine.
- `literal: true` searches the whole query as a source substring without DSL
  parsing; `ignore_case` works only with `literal: true`.
- `context_lines` is accepted only by `database.rg`. `database.rg` uses Rust
  regex syntax, line oriented and case sensitive by default.
- Pages default to 20 results, at most 100. Continue with `cursor`; cursors
  expire after ten minutes.
- A `query` may contain at most 4,096 bytes. `context_lines` accepts at most 3,
  and `sections` at most 32 names. Larger values are rejected as invalid input.

## Filters

`filters` accepts `datasets` (at most three), `authority`, `object_id`,
`document_type`, `date_kind` (`publication`, `effective`, `judgment`), `date_from`
and `date_to` (`YYYYMMDD`). `date_from` or `date_to` requires `date_kind`, and
`date_from` must not be later than `date_to`. Datasets include
`national_statute`, `administrative_rule`, `ordinance`, `treaty`, `precedent`,
`constitutional_decision`, `legal_interpretation` and `administrative_appeal`;
which ones are populated depends on the deployment.

`include_history: true` also searches retained historical revisions.
`include_ocr: true` adds OCR text, which is excluded by default.
