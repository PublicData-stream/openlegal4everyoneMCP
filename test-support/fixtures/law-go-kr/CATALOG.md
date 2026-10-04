# LAW source catalog evidence

`catalog-guide-fields.json` contains endpoint, request-field and response-field
facts extracted on 2026-10-04 from the unauthenticated public LAW API guides.
`catalog-guide-fields.provenance.json` describes the extraction and evidence limits.
These are documentation fixtures, not authenticated API responses or legal records.
Sample authentication values and full sample request URLs are omitted.

Response fields include every response table in each guide, in first-appearance
order. In particular, the two FTC decision forms and the referenced/delegated
three-column comparison forms remain distinct documented views whose field names
are combined in the registry; the final table must not discard earlier forms.

`observed-list-field-aliases.provenance.json` separately records the approved
tag-only dev observation of the audit consultation list's serial-field alias.
It does not modify the public documentation fixture or establish a corresponding
detail-response identity contract. Tests use fictional identifiers and titles.

The rendered index labels its total as 191. Its fetched HTML contains 195 unique
`openApiGuide` links; the fixture preserves all 195 instead of discarding four
without an established explanation. The adapter registry covers 69 original
families, separating institutional targets and ministry/commission namespaces.
Mobile, customized-subset and provision views do not establish new legal objects.

`FiniteMetadata` entries have a finite inventory or status response.
`IdentifiedSupplement` entries require parent IDs or provision locators obtained
from original records. Knowledge-base row IDs are documented as result ordinals,
so the registry does not invent stable legal-object IDs for those lists.
Intelligent query APIs have no finite query-space traversal and remain `QueryOnly`.

Important unresolved contracts stay explicit:

- The deletion guide's request table says `datDel`; its samples say `delHst`.
- English-law revision selection by `MST` is documented, but its detail response
  fields and an explicit history inventory selector are not documented there.
- Finance and tax interpretations have list guides but no detail guides in this
  index. Their list responses can be archived; a detail target is not invented.
- Date-filtered change inventories (`lsChgListGuide`, `lsDayJoRvsListGuide`)
  document `regDt` or `fromRegDt`/`toRegDt`, but no earliest supported change date
  or complete date enumeration. They remain `NeedsVerification`; requests are
  disabled rather than inventing dates or claiming supplemental completeness.
  The latter also documents next-day availability, so querying today cannot prove
  that today's change inventory is complete.
- The legal-term list documents a string `법령용어ID`, while detail lookup uses
  a term name and returns numeric definition serials. Approved bounded dev
  diagnostics observed non-decimal list IDs, including values longer than the
  canonical object-ID bound. The guide does not establish `trmSeqs` or a group-ID
  decomposition rule. Those rows remain explicitly rejected/incomplete; they
  are not split, truncated or hashed into invented legal-object identities.

The [official service guidance](https://open.law.go.kr/LSO/information/guide.do)
supports free legal-information reuse, including commercial use, with attribution
and approved-access requirements. It does not establish third-party dictionary,
institutional-document, linked-content or attachment rights. Those items remain
metadata-only until applicable reuse conditions are verified.
