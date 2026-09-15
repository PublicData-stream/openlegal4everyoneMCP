# Search-query syntax processor

## Status and API

The standalone Rust processor parses supplied search text into a typed expression
tree. It performs no matching, provider requests, database operations, or other I/O.
The existing synthetic MCP search still accepts literal text with its original
256-byte limit. Its query validation, cache keys, stored history, payload processors,
and widget behavior are unchanged.

`openlegal_domain::search_query` owns expressions, source spans, errors, and bounds.
`openlegal_normalization::search_query::SearchQueryProcessor` owns parsing:

```rust
use openlegal_normalization::search_query::SearchQueryProcessor;

let processor = SearchQueryProcessor::new(&["title", "body"])?;
let parsed = processor.parse("in:title:(license OR permit) -'properly installed'")?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

Construction takes `&[&str]` and returns `Result<SearchQueryProcessor,
QueryConfigError>`. Parsing takes `&str` and returns `Result<ParsedQuery,
ParseError>`. The processor owns a validated copy of the allowlist and can be reused.
There is no serialization contract or configuration change for the running server.

## Language

| Input | Intent |
| --- | --- |
| `license contract` | Both terms (implicit AND) |
| `license AND contract` | Both terms (explicit AND) |
| `license OR permit` | Either expression |
| `NOT license` or `-license` | Negate the following operand |
| `(license OR permit) contract` | Group the alternatives, then require contract |
| `"properly installed"` | Exact literal sequence, including case, punctuation, spacing, and order |
| `'properly installed'` | Both ordinary terms, without requiring adjacency or order |
| `license -'properly installed'` | License AND NOT (properly AND installed) |
| `in:title:license` | Scope license to title |
| `in:body:(license OR permit)` | Scope the whole group to body |
| `licens*` | Prefix-match intent, with prefix value `licens` |
| Empty or whitespace-only input | Explicit `MatchAll` |

Negating the single-quoted example excludes records containing **both** words;
either word alone does not satisfy that excluded group. Double quotes encode an
exact text sequence; ordinary terms and prefixes preserve input text without
choosing an analyzer. Future executors must explicitly define ordinary-term matching
and honor these distinct intents. This module does not claim any search engine or
legal provider implements the language.

### Precedence, grouping, and fields

Field operands and unary negation bind first, followed by AND, then OR. AND and OR
chains retain source order in associative nodes. Parentheses retain an explicit
group node; single quotes retain a distinct group-origin marker around their term
or AND expression. No simplification, deduplication, or semantic canonicalization
occurs, including double negation and negated `MatchAll` equivalents.

Only complete, unescaped uppercase `AND`, `OR`, and `NOT` tokens are operators;
lowercase spellings are terms. Parentheses delimit tokens, so `NOT(foo)` and
`foo AND(bar)` are valid. Implicit AND requires intervening whitespace: `foo"bar"`
and `(foo)(bar)` are errors. Unary `-` and `NOT` may have whitespace before their
operand. An operator without an operand is an error.

The spelling `in:field:operand` requires an operand to begin immediately after its
second colon. The operand can be a term, prefix, quoted form, parenthesized
expression, or unary negation. Thus `in:title:license contract` scopes only license,
and both `in:title:-license` and `-in:title:license` are accepted. A field scope
anywhere inside another field scope is an error; use separate scopes, such as
`in:title:license OR in:body:contract`.

Fields are case-sensitive, explicit caller choices. At most 64 unique names are
allowed, each 1–64 ASCII bytes matching `[a-z][a-z0-9_]*`. Duplicate/invalid names
and excess configuration entries return configuration errors. An empty allowlist
is valid and disables field scopes. Unknown query fields are errors. Only
token-initial unescaped `in:` is reserved; other colons, as in `article:5` or
`https://example.test`, remain literal. No URL is followed.

### Quotes, escapes, and punctuation

- Double quotes preserve their decoded contents exactly. Empty double quotes are
  an error; a nonempty whitespace-only phrase is valid.
- Single quotes split unescaped Unicode whitespace into ordinary terms, joined
  with AND. Keywords, parentheses, colons, double quotes, and stars inside them
  are literal. Empty or whitespace-only single-quoted groups are errors.
- ASCII apostrophes immediately between Unicode letters/numbers remain literal,
  including `don't` and `'don't stop'`. Elsewhere an apostrophe delimits a quote;
  escape it when needed. Hyphens inside bare words, such as `well-installed`, are
  literal; a leading unescaped hyphen is unary negation.
- Backslash makes the next Unicode scalar literal, including whitespace. It has
  the same rule inside both quote forms. There is no C/JSON escape expansion:
  `\n` decodes to `n`. A trailing backslash is an error. Escape a keyword character
  to make a literal word, for example `\AND`; escape `in:` as `\in:title:value`.
- Only one unescaped trailing star on a nonempty bare word denotes a prefix.
  Standalone `*`, `*word`, `wo*rd`, and `word**` are errors. Quoted or escaped stars
  are literal. `AND*` is a prefix, not a Boolean operator.
- Unicode whitespace separates expressions and single-quoted words, but all
  `char::is_control` characters (including tabs/newlines) are rejected globally,
  even inside quotes or following a backslash.

Wildcards other than trailing `*`, fuzzy matching, proximity, date filters, and
additional search-engine operators have no special meaning in this version.

## Output, diagnostics, and bounds

`ParsedQuery` contains the original source string and an `Expr`. Expressions are
`MatchAll`, `Term`, `Exact`, `Prefix`, `Not`, `And`, `Or`, `Field`, or `Group`.
Decoded literal values omit delimiters and escaping backslashes; the original
source remains available. There is no stemming, case folding, or Unicode
normalization. The original source is not a canonical cache key for any new search
semantics.

Every expression has a half-open UTF-8 byte span, on character boundaries, referring
to the original source. Quote and parenthesis groups include their delimiters;
single-quoted child terms reference their original word slices. Whitespace outside
an expression is excluded, except `MatchAll`, whose span covers the entire input.
Offsets are bytes, not Unicode scalar or displayed-character positions.

| Bound | Accounting |
| --- | --- |
| Input | 4096 UTF-8 bytes, checked before copying/token allocation |
| Expressions | 256 nodes, including operator and explicit group wrappers |
| Syntax nesting | 32 nested parentheses, single-quote groups, field scopes, or unary operators |

Each AND/OR chain allocates one operator node when it has at least two children;
single-child chains return that child. Group wrappers always count. Nesting is
checked before recursive descent; redundant parentheses and repeated negations
count even when their meaning could be simplified. AND/OR chains are processed
iteratively. Temporary decoded strings and token storage are bounded by input
size; field lookup is bounded by the validated allowlist. No regular expressions
or new dependencies are used.

Input length and forbidden controls are checked first. Parsing then stops at the
first detected lexical, grammatical, or resource error, without repair or partial
success. Typed errors cover malformed operands/groups/quotes, separators, field
syntax, unknown/nested fields, invalid wildcards, trailing escapes, and limits.
End-of-input diagnostics may use the zero-width span `len..len`. Error formatting
contains only category and byte span, never raw query text. Applications should
avoid routine logging of the returned source or expression contents.

## Validation and integration boundary

Contract tests cover expression meanings, source preservation, Unicode byte spans,
escaping and malformed input, field configuration, and exact resource boundaries.
Deterministic generated cases check successful trees and error spans without
panics; these are bounded regression tests, not exhaustive fuzzing. See the
[implementation review](search-query-review.md) for checks and independent review.

Future execution must specify analyzer behavior, admission before expensive work,
field capabilities, result completeness/pagination, and search-semantics versioning.
Do not add this syntax validation to the existing `Query::validate` without a
compatibility design: existing retained query identities can contain literal text
that this grammar would reject. No persistence migration or change to existing
search results is part of this implementation.
