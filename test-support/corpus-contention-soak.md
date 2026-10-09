# Disposable corpus contention soak

Run only on the selected SSH-MCP `dev` host; Docker networks on the local
workspace conflict with SSH routing. No legal provider is contacted. Use the same
validated dictionary as the PostgreSQL gate, or let the gate explicitly provision
its pinned dictionary source.

```sh
scripts/test-postgres.sh --contention-soak-seconds 5
scripts/test-postgres.sh --contention-soak-seconds 7200
```

The first command validates startup and completion; it does not establish a
two-hour observation. Seconds must be 1–86400, and the option cannot be combined
with test arguments or prebuilt execution. Ordinary PostgreSQL tests retain their
existing invocation and duration. Redirect stdout and stderr to new operator-owned
paths to preserve the JSONL evidence and build diagnostics separately.

The disposable fixture seeds 20,000 completed jobs, 20,000 current and 20,000
historical required-body members, 128 actual immutable captures with 64 HEAD/history
pairs and larger multiline bodies, and eight pending jobs. The missing member bodies
deliberately prevent a false completeness claim. Workload member keys are synthetic
database load; they are not fabricated retained captures or source identity proof.

Two workers continually enqueue, claim and publish deterministic synthetic records
while maintenance, snapshot status and real Tantivy replay run concurrently. The
full provisioned Korean analyzer is used. Each real index commit precedes its
database acknowledgement; no counter-only acknowledgement is substituted. Every
60 seconds, both publication and index acknowledgement must advance. Unknown
publication or index outcomes terminate the run without replaying the operation.

Seeding and initial indexing happen before the monotonic observation starts. JSONL
samples show elapsed time, progress, status, and whole-storage-operation latency;
these timings include pool waiting and are not isolated SQL execution benchmarks.
Completion requires the requested elapsed duration, an index watermark match,
unchanged initial archive hashes, exact retained HEAD/history identities and no
failed/running jobs. Reports contain synthetic aggregate evidence and no database
URL or credentials.

The PostgreSQL gate limits each fixture database container to 512 MiB and two CPUs;
the soak uses the same 1-second pool/lock, 2-second statement and 5-second transaction
budgets as serving. The corpus raw archive has a 512 MiB admission cap. The Rust
process also needs dictionary/index memory and temporary index/PostgreSQL space;
reserve at least 2 GiB process headroom and 4 GiB free disk for the two-hour run,
and monitor them on `dev`. These are preparation allowances, not measured peaks.
The gate removes the database containers and Rust fixture directory on completion.
