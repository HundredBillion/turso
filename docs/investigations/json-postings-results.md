# Transactional JSON postings experiment

This approved throwaway experiment uses ordinary tables and triggers, not a new
index method, public extension ABI, or planner feature. The fixture is
`benches/fixtures/json_postings.sql`; `json_posting_probe` is its runnable scan
reference, lifecycle checks, and measurement driver.

A lookup requires tenant, mode, canonical `json_tree` path, JSON kind and scalar.
Missing paths have no row; JSON null does. Booleans, strings and numbers have
separate kinds. Numbers retain INTEGER/REAL storage and engine numeric equality,
including integers beyond 2^53. Array membership returns distinct document IDs;
repeated members remain in storage. Container containment and path alias
normalization are unsupported. Duplicate object keys, including escaped aliases,
are rejected before mutation. Every document replacement deletes and rebuilds
all its postings in the same transaction. Applications must not write the posting
table directly. Predictable paths should first use ordinary expression indexes.

## Verification

Six focused checks cover typed matches, missing/null, array duplicates, tenant
isolation, failed duplicate-key writes, updates including changed IDs and tenants,
deletes, outer rollback, savepoints, two contending connections, reader snapshots,
clean reopen and simulated power loss. The existing `UnreliableIo` models loss of
unsynced writes: after a FULL acknowledged commit and an uncommitted delete,
materializing only durable DB/WAL bytes recovers the acknowledged documents and
exactly matching postings. This is one power-loss boundary, not exhaustive
commit/checkpoint fault exploration. Removing the update trigger deliberately
made the scan-oracle lifecycle test fail; restoring it passed.

The oracle compares every posting, including scalar storage class and duplicate
array members, against `json_tree` over documents. Each timed read must reproduce
its reference result. Runs also verify `integrity_check` and reopen contents.

## Reproduction and interpretation

```
cargo test -p turso_benches --example json_posting_probe
CARGO_PROFILE_LIB_RELEASE_PANIC=unwind cargo build -p turso_benches --example json_posting_probe --profile lib-release
POSTING_ROWS=1000 POSTING_SAMPLES=1000 POSTING_MODE=scan target/lib-release/examples/json_posting_probe
POSTING_ROWS=1000 POSTING_SAMPLES=1000 POSTING_MODE=expression target/lib-release/examples/json_posting_probe
POSTING_ROWS=1000 POSTING_SAMPLES=1000 POSTING_MODE=postings target/lib-release/examples/json_posting_probe
```

The expression mode indexes the two known scalar paths with the exact type and
value expressions used by the query. It substitutes literal paths so the planner
can recognize those indexes, and uses `INDEXED BY` to select them explicitly.
Without that hint the observed planner chose the tenant-only index. The recorded
plan uses tenant and kind expressions, with scalar equality remaining residual. Array membership still scans in that mode; ordinary
scalar expression indexes do not index all array members. This baseline checks
whether selected paths already offer the read benefit with less maintenance.

Run modes in separate processes to capture peak RSS, alternate order for repeated
measurements, and build before timing. Rows are capped at 100,000 and samples at
10,000. All modes use WAL, synchronous FULL, a 64 MiB page-cache limit, ten tenants,
and identical generated documents containing sixteen custom scalar fields,
duplicate tags and optional nulls. Reads use prepared statements with 100 warmups.
Updates replace the complete JSON document while changing one custom field;
every sample changes its value. JSON construction is outside the update timer.
The timer includes binding, stepping and result collection, but excludes prepare.

Stdout records p50/p95/p99 in microseconds. Stderr records plans, seed time,
pre-checkpoint DB/WAL sizes, truncating-checkpoint time, checkpointed database bytes
and open/configuration time. File size is not bytes written: automatic checkpoints
and WAL reuse prevent treating it as physical write amplification. System-level
write accounting remains required for that claim.

The first local exploratory run (1,000 rows, 100 samples, shared macOS host)
showed scalar lookup p50 122.7 to 2.75 us, membership 122.3 to 5.33 us, and null
lookup 132.6 to 6.21 us. Durable update p50 rose from 40.8 to 191.1 us; bulk seed
rose from 4.94 to 94.3 ms. These illustrate a read/write tradeoff, not hosted
performance acceptance or a recommendation to index every scalar. The initial
run predates checkpoint-size instrumentation; retain its raw output separately.
Hosted alternating runs on the final source are needed before drawing a speed
conclusion. Comparing this application schema with a scan on one engine revision
does not prove the engine beats upstream main, nor does it establish that Redis
can be removed.

At 1,000 rows the checkpointed file sizes were 1,048,576 bytes for scan,
1,089,536 for two expression indexes and 4,882,432 for all scalar/member postings.
A separate local expression-index run produced scalar p50 8.46 us and null
p50 4.92 us, illustrating that selected indexes can match or beat general postings
for known paths. These separate-run timings remain exploratory.

The membership scan explicitly requires an array at the requested path;
`json_each` alone also emits a row for scalar values. A non-array null membership
regression failed without this guard and passed after restoration. Earlier local
raw runs predate this additional guard, although their queried `tags` fields were
all arrays. Hosted results must use the guarded revision.

Conflict replacement is included explicitly: `INSERT OR REPLACE` can implicitly
delete an old row without firing its delete trigger when recursive triggers are
disabled. The insert trigger therefore clears `NEW.id` postings before rebuilding;
the update trigger clears both old and new IDs for `UPDATE OR REPLACE` collisions.
The explicit insert-replacement case failed before cleanup and passed afterward,
as did a collision between two document IDs. This followup adds insertion lookup
work; prior local timings are not measurements of this final fixture.
