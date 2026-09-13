# Supplied-text comparison

This opt-in feature compares two supplied UTF-8 texts with Git and renders bounded
pages in a React MCP App using `@git-diff-view/react`. It does not retrieve legal
records, browse repositories, determine legal equivalence, or interpret amendments.

## Run locally

Install Git and the pinned Rust/Node/pnpm toolchains. Build both widget assets:

```sh
pnpm --dir apps/widget install --frozen-lockfile
pnpm --dir apps/widget build
```

Prepare the disposable TLS files described in the [demo setup](demo.md#local-setup),
without starting its mock upstream. Replace the corresponding-source placeholder
in `deploy/text-diff/server.toml`, then run:

```sh
cargo run --locked -p openlegal-server -- deploy/text-diff/server.toml
```

The additional configuration is:

```toml
[limits]
max_message_bytes = 16777216
max_buffer_bytes = 268435456

[text_diff]
git_path = "/usr/bin/git"
widget_html = "apps/widget/dist/text-diff.html"
```

The feature requires these message/buffer allowances because two 1 MiB inputs can
expand to approximately 12 MiB when JSON-escaped, and HTTP reserves four message
allowances per active request. This profile does not change ordinary-server
defaults. The 256 MiB transport reservation budget and 128 MiB comparison budget
are separate accounting bounds, not a process RSS guarantee. Allow additional
runtime overhead; the integration container uses 1 GiB. Both data transports and
the private health listener remain required.

The edge must admit the same request size: the pinned OxiBelt fixture sets
`routes.limits.max_request_body_bytes = 16777216` only for `/mcp`. Its inherited
10 MiB default is insufficient for the largest JSON-escaped input pair.

Git is an absolute operator-selected executable, probed before listeners bind.
The comparison HTML must fit 3 MiB after source-offer substitution and its
serialized resource must fit both 6 MiB and half the configured message allowance.
The synthetic browser retains its 1 MiB raw allowance. Both assets bundle script,
styles and license locally, with no external origins allowed by their MCP CSP.

## MCP contract

All four tools share one application service across HTTP and native WebTransport.
Object outputs carry `schema_version: 1`.

| Tool | Input and output |
| --- | --- |
| `compare_texts` | Required `before` and `after` strings; optional `before_label` and `after_label`. Returns a summary with `comparison_id`, expiry, byte/line/newline information, additions/deletions, equality and change-page count. |
| `show_text_diff` | Either `{}`, a before/after pair with optional labels, or `comparison_id` alone. Returns `{schema_version, comparison}`; null comparison opens an empty editor. Only this tool advertises the UI resource. |
| `get_text_diff_page` | `comparison_id`, zero-based `page`, and `view` (`changes`, `before`, `after`). Returns numbered changes or original text chunks, with `total_pages`. |
| `delete_text_diff` | `comparison_id`. Returns `{schema_version: 1, deleted: true}` even when the well-formed handle is already absent. |

The resource URI is `ui://openlegal/text-diff-v1.html`, MIME
`text/html;profile=mcp-app`. Missing/expired handles have the same sanitized read
error. Invalid arguments, unavailable execution, saturation and resource limits
remain distinct existing MCP errors. Git stderr, private paths and supplied text
are not diagnostics. A summary is not a complete patch: retrieve every changes
page when completeness matters.

## Exactness and paging

Each text permits at most 1 MiB UTF-8, 100,000 LF-delimited lines and 16 KiB per
line. NUL is rejected. Display labels are bounded to 128 UTF-8 bytes and never
become file paths. Empty strings are valid. Whitespace, BOMs, CRLF/LF, lone CR and
final-newline differences are preserved; there is no Unicode normalization or
ignore-whitespace mode.

Git runs a fixed no-index Myers comparison with three context lines. The adapter
disables inherited configuration, external diff commands, text conversion, paging
and color. It reads at most 8 MiB stdout and 8 KiB stderr. Exceeding a bound fails
the whole comparison rather than returning a truncated success.

Change pages contain at most 400 diff-content lines and 256 KiB of serialized
output including metadata. Large hunks are split between lines; each fragment
retains original before/after starts and counts, and final-newline markers stay
attached to their content line. The widget rebases fragments for bounded renderer
work, displays local gutter numbers, and labels their original source ranges.
It does not regenerate differences or expand unchanged source context. Original
text chunks contain at most 32 KiB, split at UTF-8 boundaries; concatenate pages
in order to reconstruct the exact source.

Local UTF-8 files are read with strict decoding and BOM preservation. They are sent
only on Compare. Raw inputs remain separate from textarea display values. Editing
CR-containing input requires an explicit LF-edit transition, so the conversion
becomes an intentional input change. An existing handle can be opened without
recomputation; original text pages are loaded only when editing is requested.

## Retention, deletion and lifecycle

A handle contains 256 random bits and is a bearer capability: anyone receiving it
can read or delete the result. There is no account isolation or list-results API.
Do not place handles in logs, URLs or persistent browser storage. Sharing a handle
also shares deletion authority.

Results expire ten minutes after publication; reads never extend the deadline.
At most 32 comparisons and 128 MiB of accounted originals, patches, indexes and
reservations are admitted. Unexpired entries are not evicted to admit new work.
At most two Git jobs run at once, with a ten-second deadline shortened by the
caller deadline. Capacity is reserved before temporary inputs or Git execution.

Supervised work survives a dropped requesting future only long enough to stop and
reap the child and clean its private temporary inputs. Failed/cancelled work does
not publish. Temporary files exist during computation; results are memory-only.
Prefer an operator-managed temporary filesystem for sensitive inputs. Ordinary
cleanup is not a secure-erasure guarantee after a host crash or against host access.

Clear is disabled while creation is unresolved. It invalidates page requests and
deletes the current result before reporting success; deletion failure leaves a
retry action. Recomparison deletes the previous widget result before creating its
replacement. Deletion prevents subsequent reads but cannot retract content already
returned to another client. Closing a widget is not a reliable deletion signal;
fixed expiry remains the fallback.

`delete_text_diff` is the sole built-in mutation exception. Its annotations declare
read-only false, destructive true, idempotent true and open-world false. Generic
extension registration still requires read-only behavior. This exception does not
authorize repository changes, provider writes, or general administration.

## Validation and limitations

The [contributor checks](../CONTRIBUTING.md#testing-and-ci) apply. Run the complete
edge gate with both built assets:

```sh
DEMO_WIDGET_HTML=apps/widget/dist/index.html \
TEXT_DIFF_WIDGET_HTML=apps/widget/dist/text-diff.html scripts/test-oxibelt.sh
```

Fixtures are synthetic or supplied text; routine tests never contact legal providers.
Browser tests use a local MCP Apps host. Passing these gates does not establish live
ChatGPT rendering or a particular host's support for maximum-size tool arguments.
No deployment or publication is included.
