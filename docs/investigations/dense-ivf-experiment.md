# Internal dense IVF experiment

This opt-in prototype is not a supported SQL interface, stable file format, or
extension ABI. It starts at fork main `78a77aac1`; the approved requirements are
in the companion retrieval-prototypes design. No separate PRD was supplied.

Build with `--features turso_core/experimental_dense_ivf`. Nothing registers in
ordinary builds. Existing database formats and `ExtensionApi` are unchanged.

```sql
CREATE TABLE v(id INTEGER PRIMARY KEY, e BLOB, tenant INTEGER);
CREATE INDEX vi ON v USING experimental_dense_ivf(e, tenant)
WITH (version=1, metric='cosine', dimensions=2,
      centroids='[1,0];[0,1];[-1,0];[0,-1]', nprobe=4);
SELECT id, vector_distance_cos(e, vector32('[1,0.2]')) AS distance
FROM v WHERE tenant=7 ORDER BY distance, rowid LIMIT 10;
```

Train centroids outside a write transaction, then supply them explicitly. Version,
metric, dimensions, centroids and fixed nprobe persist in the transactional
`sqlite_schema` index definition. Each posting in the core backing store is keyed
by `(integer tenant, cluster, rowid, vector32 blob)`. The first three fields
identify a posting; the trailing vector provides exact reranking. The owned
backing table declares all four fields, matching the backing index layout.
The index does not maintain an external file, global mutable search cache or
transaction-private training state. Row changes use existing index callbacks.

The tenant prefix is sought before reading candidates. Selected clusters are
ordered by cosine distance with cluster ID as tiebreaker; candidates use the
same cosine implementation as the SQL function, with rowid as final tiebreaker.
All-cluster search must match an exact filtered scan. nprobe is fixed per
attachment: the benchmark rebuilds the index for each sweep point. There is no
new scalar API to control it. Tenant equality is the only authorization boundary
modeled. Extra predicates, joins, aggregation and other SQL shapes are outside
the benchmark and must not be described as supported approximate search.

The prototype enables two narrow internal pattern matcher cases (equality and
rowid) behind the same feature. Existing matching lacks both, so an EQP assertion
is required: an apparently successful query otherwise silently exercises a scan.
The exact oracle's EQP must conversely contain no index method.

## Execution record

| Task | Status | Evidence |
| --- | --- | --- |
| Internal attachment and transactional postings | Verified by focused tests | core/index_method/experimental_dense_ivf.rs |
| Exactness, mutation and tenant tests | Passed in WAL and MVCC | tests/integration/index_method/dense_ivf.rs |
| Concurrent writers and injected failures | Focused scenarios passed | Existing QueuedIo and UnreliableIo harnesses |
| Synthetic 100k recall and latency sweep | Local debug correctness passed; isolated optimized run pending | evidence/dense-ivf-100k-local-debug.txt |
| Independent branch review | Pending | Parent agent review |

Initial tests correctly failed: the planner took a scan before equality and
rowid matching existed; reading a borrowed record blob caught vector alignment
requirements; deleting records caught B-tree seeks that require an advance; and
MVCC reopen lost postings when the declared backing schema omitted record fields.
The last issue is avoided by the explicit owned four-column layout, without a
recovery-engine change. A failed multirow insert must undo its earlier valid row.
NaN, infinity, zero, wrong dimensions and NULL are rejected.

## Reproduction and current evidence

Rust 1.88, macOS ARM64, two build jobs, no incremental compilation. The shared
host has unrelated CPU-heavy jobs; its timings are not performance acceptance.

```
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 \
CARGO_PROFILE_TEST_DEBUG=0 DENSE_IVF_ROWS=2000 DENSE_IVF_QUERIES=10 \
cargo test -p core_tester --no-default-features \
  --features experimental_dense_ivf,test_helper --test integration_tests \
  dense_ivf -- --include-ignored --nocapture --test-threads=1
```

Result: **8 passed**, 0 failed, including the entire six-point smoke sweep.
Formatting and whitespace checks passed. Core Clippy passed with warnings
denied after allowing `unfulfilled_lint_expectations`: strict Clippy first failed
on an existing expectation at `core/json/cache.rs:107`, outside this change.
All-cluster rows and distances matched the exact oracle bit for bit. EQP checks
confirm the indexed query uses an index method and the oracle does not. The
fault test deliberately produces I/O-error log entries; the test checks rollback.

For the isolated optimized 100k run, build once, then measure the test binary
separately so peak RSS and elapsed time exclude compilation:

```
export CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0
export CARGO_PROFILE_TEST_OPT_LEVEL=3 CARGO_PROFILE_TEST_DEBUG=0
export CARGO_PROFILE_TEST_CODEGEN_UNITS=16
cargo test -p core_tester --no-default-features \
  --features experimental_dense_ivf,test_helper --test integration_tests \
  --no-run --message-format=json
# Use the executable from the compiler-artifact message:
DENSE_IVF_ROWS=100000 DENSE_IVF_QUERIES=100 /usr/bin/time -v "$TEST_BINARY" \
  dense_ivf_recall_probe --ignored --nocapture --test-threads=1
```

The probe records deterministic seed 19937, 32 dimensions, 32 centroids, six
training passes over the first 2,048 vectors, and separate held-out queries.
One fifth of data vectors and all queries mix neighboring latent centers to
exercise cluster boundaries. Tenants hold 80%, 10%, 9.9%, 0.1%, and 0% of rows.
The 100-query default gives only 20 samples per tenant, so tail percentiles are
coarse order statistics, not a service-level latency estimate.

It reports full-scan and tenant-B-tree exact latency, IVF recall@10, p50/p95/p99,
actual candidate visits, build time, allocated/file bytes, 1,000-row replacement
cost and resulting WAL bytes, and reopen plus first-query time. Peak process RSS
comes from time(1). Candidate visits use a test-helper global counter, so run the
ignored probe alone. Each measured count is checked against independent cluster
assignment counts. The index is rebuilt for each fixed nprobe; the source rows,
centroids and held-out queries remain identical. Updates set `e=e` to measure
complete replacement without changing later sweep workloads. The recorded WAL
bytes are an observed update volume, not a device write-amplification estimate.

## Local 100k functional result

The committed probe at `5f3a98f0b` completed the 100,000-row / 100-query sweep
on 2026-11-06. All-cluster results matched exact rows and distances, and every
actual candidate count matched the independent assignment count. Full output:
[evidence/dense-ivf-100k-local-debug.txt](evidence/dense-ivf-100k-local-debug.txt).
This was an **unoptimized debug build on the shared Mac**, not a speedup claim.

| nprobe | Recall@10: 80,000-row tenant | Recall@10: 100-row tenant |
| --- | ---: | ---: |
| 1 | 0.725 | 0.100 |
| 2 | 0.955 | 0.200 |
| 4 | 0.995 | 0.295 |
| 8 | 1.000 | 0.535 |
| 16 | 1.000 | 0.840 |
| 32 | 1.000 | 1.000 |

Filtering before ranking prevents cross-tenant results, but it does not make
small-nprobe recall acceptable for highly selective tenants. The sparse tenant
needed every cluster to recover all exact neighbors in this generated workload.
No recall threshold is approved by these numbers.

Observed base allocation was 15,204,352 bytes including the tenant B-tree;
the dense index added 16,261,120 allocated bytes. Whole-process peak RSS was
103,038,976 bytes, including the in-memory generated corpus and benchmark.
The entire run took 61.3 seconds. Each index build took 5.59–6.94 seconds;
1,000 replacements took 46.3–56.0 ms and generated 733,392 WAL bytes.
Reopen plus first query ranged from 10.2 to 102.5 ms across probe counts.
Raw p50/p95/p99 and both exact baselines are retained in the evidence file.
These host-dependent figures only establish that resource usage was practical
for this scale; wait for isolated optimized results before comparing speed.

## Limits

The isolated optimized 100k measurement is pending. No
representative embeddings were supplied and no recall loss has been accepted.
The crash test covers torn checkpoint writeback under FULL synchronous mode;
it is not an exhaustive commit-phase, syscall-failure, or simulator seed sweep.
MVCC failure recovery beyond clean reopen is not covered by that WAL crash test.
The prototype materializes and sorts all selected candidates; its memory grows
with the eligible candidate count. Training quality, drift/rebuild policy,
numerical extremes beyond invalid-value rejection, cancellation, schema renames,
complex authorization, and a versioned plugin bridge remain outside validation.
No production-readiness claim follows from these tests or synthetic timings.
