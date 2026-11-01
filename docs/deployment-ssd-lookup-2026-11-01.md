# Corpus Blob SSD lookup cutover, 2026-11-01

Recorded on **2026-11-01**, against signed runtime source
`eef8c514d065d062ad6aaedebbfd1596f135597f` and documentation base
`035024c652ea80b729e411ebda74154d638c4da2`. The recording commit is the signed
commit introducing this file, discoverable with
`git log --diff-filter=A --format=%H -- docs/deployment-ssd-lookup-2026-11-01.md`.
Current behavior belongs to the [operator runbook](deployment-kubernetes.md) and
[persistence contract](persistence.md).

This authorized operation retains signed source
`eef8c514d065d062ad6aaedebbfd1596f135597f`, version
`0.1.0-build.eef8c514`, and server image index
`sha256:2eee53981cc8a8a043dec9ad53b4ecac59535f9299b75f93afc779bf50b065da`.
It follows the [diagnostic deployment](deployment-diagnostic-2026-11-01.md).
Current policy remains in the [operator runbook](deployment-kubernetes.md) and
[persistence contract](persistence.md); this operation changes neither contract.

## Storage and preservation

Only the serving corpus Blob claim moved to a new SSD ext4 copy through the
separate Retain PV/PVC `openlegal-corpus-blobs-ssd-lookup`. The container path,
EEF image/configuration and PostgreSQL, cache, index and dictionary stores
remain unchanged. The SSD is a single-device lookup copy, not a replacement
mirror. Original HDD data, PV/PVC and all seven coordinated source snapshots
were retained. The separately excluded Docker database was untouched.

Serving stop completed at **11:44:05 UTC**. Seven coordinated snapshots and
an isolated restore completed with actual OS-zero outcomes, owned-process
closure and disposable-resource cleanup. Their original nine successful phase
records remain preserved.

Copy verification covered **50,103 permanent files, 949,396,332 bytes**, plus
one separately validated 12-byte health-stage artifact. It required durable
writes, full destination readback, unchanged source-before/after witnesses and
permanent-reference correspondence. The health artifact is nonpermanent;
after startup it belongs to a separately validated volatile namespace, not a
claim that the entire tree remains byte-identical.

Copy manifest SHA-256:
`a2c9eb593c51f056c15bd20e52140c2321862e75a5052c2656990f5307181914`.
Permanent-reference correspondence SHA-256:
`b86a42944776c2614430c831479c1347da82842f648e86b23e66c85a03ed8822`.

## Preserved failure and continuation

The original storage-creation phase exited one after server dry-run returned
bytes to a text-only response parser. It transmitted no creation effect and
created no PV/PVC. Its failed journal and OS/mirror evidence remain failed.

A separately source-reviewed continuation authenticated the original nine
successful phases and the failed creation evidence, retained their original
authority and shared lock, and executed only the remaining stages. It did not
relabel the failed phase, repeat snapshot/restore/copy, or synthesize a new
preservation baseline. Actual creation, claim switch and serving start then
completed with OS-zero and sealed closed-mirror evidence.

## Runtime and external checkpoint

New SSD serving start completed at **12:55:48 UTC**. Its new Pod had zero
restarts, internal health returned 200, one ready endpoint existed, and actual
AMD64 image/configuration and PVC/PV/host-mount identity matched the verified
SSD destination. These are startup checks, not two-hour stability.

A real external event began at **13:10:00.275 UTC**. A fresh matrix passed all
16 combinations of two hosts, two protocol revisions, IPv4/IPv6 and
HTTP/native WebTransport, with four subprocess exits zero and the fixed-capture
body checks. Packager and archive assembler exited zero. The 3,012-byte archive
was delivered to an installer whose owned wait returned zero at 13:11:30,
within the unchanged 210-second boundary. Raw event/result bytes were preserved;
no earlier matrix or expired timestamps were reused.

Archive SHA-256:
`87e8c4370f41d363ab6b2997b97bb7e8b94d4ec49952923ab476770292c5e945`.
External raw manifest SHA-256:
`c813881d6b4877ff38d017ba8bd241c38110a9fd3373e68ed309dfd2a412b52d`.

The complete lookup phase finished at **13:12:19 UTC**. Its actual worker and
supervisor exited zero, owned processes closed, and the sealed journal mirror
matched. The fresh120 assessment selected `preserve_serving`, with collection
quarantined and provider review required. The guard remained valid through
final checks. All 28 retained-table fingerprints, provider/quarantine state,
SQL-work closure, actual native image and SSD mount binding matched; the same
Pod still had zero restarts.

Accepted lookup completion SHA-256:
`0bdf9122f085edc7a4e54de1cf9a89be82efba059bfe00cbab0a3c6c97d581fe`.
Closed mirror SHA-256:
`f2a8a03c800a2df603644160cc69a7c4e5bff5585fa58928f2235c199a29c5cb`.

## Actual two-hour observation

The monotonic observation loop started at **13:16:10 UTC** and completed
**189 bounded checks**, with the final identity sample at **15:16:18 UTC**.
The same Pod retained zero server and init-container restarts, one ready
endpoint, and internal live/ready status 200 throughout the checks. The loop
required at least 7,200 seconds; it was not shortened or simulated.

Final retained/provider/quarantine and original-storage comparisons, SQL-work
closure, SSD permanent-file readback and mount binding then passed. Phase
completion was **15:18:47 UTC**. The reported `stable_seconds` was
**7,356.104 seconds**, including those final checks; it is not the interval
between the first and last identity samples. The actual worker and supervisor
both exited zero, their owned processes closed, and the sealed mirror matched.

Observation completion SHA-256:
`b3c61ac4aa33ba93e344fc263ad81e1a77af6ec989e9aa89d145615b2cef163b`.
Closed mirror SHA-256:
`0ff8d92d82557aaaf8cfa9784e98adf20ffe6d688880050415c084d7e3b91555`.

## Boundaries and remaining work

Collection remained off and its original Blob mapping was retained. No automatic
SSD/HDD synchronization or failover was introduced. Resumption requires a
separately reviewed consistent storage/write arrangement and provider decision.
No new role, credential, Secret or collection Job was created. Product APIs,
durability/acknowledgement rules and normal SQL/blob allowances were unchanged. Existing runtime health and ordinary serving SQL
writes continue; this is not a zero-write deployment.

Historical failed observation and strict 167-reference comparison remain failed,
with exact column attribution unresolved. SSD placement does not establish the
cause of the earlier health deadline or a durable repair. Native ChatGPT UI,
reboot persistence, full provider cloning/reuse rights and load/capacity
acceptance remain separate outstanding gates.

## Review and checks

Reviewer identity: **Codex, independent review agent**, separate from the
implementer. The frozen operator source graph was
`f850bddb9c887e227690e86bd63648591127e103695dd676fc24eda92cbfb15f`.
Prior bounded tooling reviews concluded CLEAR in these categories:

- **Security Review**: authority, source/hash, parser/path, process and shared-lock
  boundaries of the continuation, source installer and local runner.
- **MCP/API Boundary Review**: source/event identity, unchanged external arrival
  bounds and actual 16-case acceptance boundaries in the continuation, external
  recipe and local runner.

Their immutable review receipt SHA-256 values were:

- Continuation: `f52643d29b90e3f5eed83ff6d8c49e563e306246c19d8d59db0dc3c2dfa3add7`.
- Source installer: `c5cdef8924d657d33aa651e723a2b4cc9fa234d5a34224839a15a2edc47887b8`.
- External recipe: `a3964adcff3d182c3345b3ee93ea6bd3ecd4a48cf14a5be8d09e817b72144e86`.
- Local runner: `a57645280c880a70d555ad5453aebb57fe91e8c58bde94de332d0931b1e5d26e`.

Final documentation/evidence consistency review concluded **CLEAR** for this
record, its index row and the earlier record's header link, based on the
documentation revision above. This is separate from the prior tooling categories. The parent
verified actual OS status and every closed remote mirror through the frozen
verifier. Review inputs contain byte-faithful closed metadata plus an explicitly
identified observation-result projection; the reviewer does not claim a separate
traversal of the full remote mirror or a new security scan.

Documentation validation passed `git diff --check`; `git status --short` showed
only the three intended Markdown files. A Python standard-library check resolved
all 63 relative file/anchor references across them. Full new-file content,
Markdown structure and the preserved historical body were inspected. No Rust
checks or image builds were rerun for this documentation-only patch. The deployed runtime and earlier exact-source
CI remain the EEF release recorded in the diagnostic deployment.
