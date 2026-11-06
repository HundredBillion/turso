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
| Synthetic 100k recall and latency sweep | Local debug and hosted optimized runs passed | evidence/dense-ivf-100k-hosted.txt |
| Synthetic 1m recall and latency sweep | Hosted optimized run passed | evidence/dense-ivf-1m-hosted.txt |
| Independent branch review | Parent reviewed query core; proposal reviewed separately | No production approval |

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

## Hosted optimized 100k result

[Run 42244303993](https://github.com/HundredBillion/turso/actions/runs/42244303993)
succeeded using source `5f3a98f0b` and validation revision `736f704d7` (workflow
changes only). Rust 1.88, Linux x86_64, four allocated logical CPUs on an AMD
EPYC 9V74 runner; test profile opt-level 3, 16 codegen units, debug info disabled.
Seven lifecycle tests passed and the separate ignored 100k sweep passed.

| Method | Large tenant p50 / p95 (ms) | Large tenant recall@10 | 100-row tenant p50 / p95 (ms) | 100-row tenant recall@10 |
| --- | ---: | ---: | ---: | ---: |
| Exact filtered full scan | 33.522 / 36.169 | 1.000 | 6.718 / 7.114 | 1.000 |
| Exact with tenant B-tree | 45.915 / 47.616 | 1.000 | 0.239 / 0.258 | 1.000 |
| IVF nprobe=1 | 0.764 / 0.926 | 0.725 | 0.042 / 0.052 | 0.100 |
| IVF nprobe=4 | 2.568 / 2.981 | 0.995 | 0.054 / 0.074 | 0.295 |
| IVF nprobe=8 | 5.003 / 5.466 | 1.000 | 0.067 / 0.078 | 0.535 |
| IVF nprobe=16 | 9.934 / 10.364 | 1.000 | 0.087 / 0.105 | 0.840 |
| IVF nprobe=32 | 20.010 / 21.302 | 1.000 | 0.126 / 0.139 | 1.000 |

Recall matched the local deterministic workload at every sweep point. Full
probing matched exact rows and distances. Observed perfect recall at fewer
clusters applies only to these held-out synthetic queries and is not a guarantee.

Training took 9.397 ms; base ingestion took 401.525 ms. Index builds took
1.070–1.191 seconds, with the same 16,261,120 extra allocated bytes as the local
run. Replacing 1,000 rows took 9.017–33.569 ms and generated 733,392 WAL bytes;
reopen plus first query took 1.495–25.974 ms. Peak whole-process RSS was
84,972 KiB; the sweep took 11.86 seconds excluding compilation. Results include
SQL preparation and the test-helper candidate counter. One run and twenty
queries per tenant establish an experimental comparison, not a reliable tail
latency estimate.

[Raw measurement](evidence/dense-ivf-100k-hosted.txt) retains every tenant and
probe count; [metadata](evidence/dense-ivf-100k-hosted-metadata.md) retains CPU,
compiler, binary hash, lifecycle results and resource counters. The resource
usage permits a separate 1m-row follow-up on the same implementation.

## Hosted optimized 1m result

[Run 42245689959](https://github.com/HundredBillion/turso/actions/runs/42245689959)
succeeded with the same implementation and optimized configuration. Validation
revision `32100d451` changes only the workflow relative to source `5f3a98f0b`.
All-cluster exactness and candidate-count assertions passed at one million rows.

| Method | 800,000-row tenant p50 / p95 (ms) | Recall@10 | 1,000-row tenant p50 / p95 (ms) | Recall@10 |
| --- | ---: | ---: | ---: | ---: |
| Exact filtered full scan | 450.401 / 457.335 | 1.000 | 92.317 / 93.082 | 1.000 |
| Exact with tenant B-tree | 566.640 / 572.322 | 1.000 | 2.838 / 2.942 | 1.000 |
| IVF nprobe=1 | 7.016 / 7.679 | 0.675 | 0.073 / 0.138 | 0.150 |
| IVF nprobe=4 | 30.361 / 30.917 | 0.995 | 0.132 / 0.212 | 0.380 |
| IVF nprobe=8 | 60.831 / 62.606 | 1.000 | 0.242 / 0.330 | 0.665 |
| IVF nprobe=16 | 122.259 / 123.863 | 1.000 | 0.383 / 0.476 | 0.885 |
| IVF nprobe=32 | 244.540 / 246.492 | 1.000 | 0.631 / 0.686 | 1.000 |

Training took 10.155 ms; base ingestion took 4.976 seconds. Builds took
13.651–14.398 seconds. The base database used 152,379,392 bytes and the index
added 159,449,088 allocated bytes. Replacing 1,000 rows took 12.006–12.653 ms
and generated 716,912 WAL bytes; reopen plus first query took 9.583–252.034 ms.
Whole-process peak RSS was 582,196 KiB; the complete sweep took 149.73 seconds,
excluding compilation. [Raw measurement](evidence/dense-ivf-1m-hosted.txt) and
[metadata](evidence/dense-ivf-1m-hosted-metadata.md) retain the full evidence.

Both scales use 32 dimensions and only 20 queries per tenant in one run. Their
p95/p99 estimates are sparse; synthetic recall does not establish RAG quality.
Queries are generated after the corpus from the same RNG, so the 1m and 100k
runs have different held-out queries. Each individual sweep compares every
method against identical queries, but the cross-scale numbers are not a paired
same-query scaling experiment. The corpus/training prefix is shared.

## Limits

The measured experiments are complete at 100k and 1m, within the scope above. No
representative embeddings were supplied and no recall loss has been accepted.
The crash test covers torn checkpoint writeback under FULL synchronous mode;
it is not an exhaustive commit-phase, syscall-failure, or simulator seed sweep.
MVCC failure recovery beyond clean reopen is not covered by that WAL crash test.
The prototype materializes and sorts all selected candidates; its memory grows
with the eligible candidate count. Training quality, drift/rebuild policy,
numerical extremes beyond invalid-value rejection, cancellation, schema renames,
complex authorization, and a versioned plugin bridge remain outside validation.
No production-readiness claim follows from these tests or synthetic timings.
