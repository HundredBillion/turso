# Cache-free workloads: initial measurement probe

This investigation starts from `78a77aac1`. It does not establish that Redis can
be removed. The probe is experimental measurement code, not a new database
feature. No SQL, JSONB encoding, file format, or extension contract changes.

## Reproduce the embedded JSON baseline

```sh
git rev-parse HEAD
rustup run 1.88 rustc -Vv
uname -a
CARGO_PROFILE_LIB_RELEASE_PANIC=unwind rustup run 1.88 cargo build --profile lib-release -p turso_benches --example json_workload_probe
PROBE_ROWS=1000 PROBE_SAMPLES=1000 target/lib-release/examples/json_workload_probe > results.csv 2> configuration-and-plans.log
```

Record hardware, memory, free space, filesystem and power mode with the results.
Run without other compilation or benchmarks. Repeat at least three times before
comparing changes. Preserve raw CSV and logs with the engine revision and build
command. A debug build is useful for correctness only. The measured benchmark profile is
`lib-release` (optimized, 16 codegen units, no LTO); benchmark binaries unwind on
panic. The probe command matches that setting to share build dependencies.

The probe constructs exact 1,024 / 10,240 / 102,400-byte JSON text documents with
category and price first, padding in the middle, and a trailing scalar. Each
representation has the same table and seeded values; the typed-column case
retains its JSON document so it has comparable row width. JSONB encodes the
same document and its byte length may differ. There are ten tenants and seven
categories. IDs, tenant assignment, prices and pseudorandom query sequence are
deterministic. Each case gets its own temporary file, deleted afterward.

Each file uses WAL, FULL synchronous mode and a 64 MiB page cache; effective
pragmas and query plans are logged. These settings do not imply crash-recovery
validation or a measured number of filesystem synchronization operations.
The defaults keep each live dataset near 100 MiB before WAL/index overhead.
Rows are restricted to 100–10,000 and samples to 1–100,000 to bound accidental
resource use. Larger experiments require a reviewed change to these caps.

The cases are prepared point reads, tenant/category filtering before and after
an expression/composite index, and autocommit single-row price increments with
the index present. The typed-column update changes the typed price only; it
models moving a frequently updated field out of JSON, not maintaining duplicate
copies of that field. Each case executes 100 warmups and then measured samples.
Warmup updates really commit and are included in final-value verification.
The probe checks result cardinalities and verifies the final aggregate price
increased by exactly the committed operation count.

Timing includes parameter binding, stepping, I/O and statement reset. It excludes
setup, SQL preparation, result serialization, correctness checks, and caller
loop work. Percentiles use the nearest-rank definition. The final CSV column is
the inverse mean statement duration, **not sustained application throughput**.
There is one client and no offered-load generator, so this experiment cannot
measure overload behavior or avoid coordinated omission. The warmups warm the
engine/OS caches but do not prove every page is resident.

## Source observations to test

`core/json/jsonb.rs::operate_on_path` clones the full `ValueBlob` before every
operation for rollback. `SearchOperation` does not mutate the input but still
passes through this path. `core/json/mod.rs` allocates search result capacity
using the input document length (or half the length), even for scalar values.
`core/json/cache.rs` clones cached JSONB values. These are separate allocation
sources; removing only one is not a borrowed extraction implementation.

Do not skip the snapshot based on `ReplaceExisting`: search, replacement and
deletion can share this mode. A prototype should introduce an explicitly
read-only traversal or search entry point and preserve existing mutation
rollback. Test missing paths, null, negative array indexes, quoted keys,
duplicate keys, numeric/string distinctions, invalid paths and malformed JSONB
against the existing engine/SQLite oracle. Measure allocations and bytes copied
for small scalar results and large subdocuments before retaining the prototype.

Selected-path expression indexes already exist. The probe records whether the
planner chooses them; no generic containment index is implied. Wider objects,
late keys, nesting, arrays, cache hit/miss behavior and mutation amplification
need separate probes. The current padded document deliberately isolates size
from key count and is not a representative ecommerce document corpus.

## Staged design decisions

1. **Workload contract.** Select product lookup/facets, inventory plus order
   transaction, feed pagination, tenant custom fields and permission-filtered
   retrieval mixes. Specify data skew, hot rows, dataset growth, staleness
   allowance, read-after-write behavior and durability. Choose embedded API
   and shared-service protocol/topology. Freeze hardware and numeric targets
   before interpreting results as acceptance evidence.
2. **Application baseline.** Implement complete requests on database-only and
   database-plus-Redis paths with the same consistency contract. Run at fixed
   offered load, 1/8/32/128 clients, datasets inside/near/above RAM, warm/cold
   reads and sustained checkpoint activity. Include network and serialization
   for the service. Report p50/p95/p99, throughput, retries, CPU, RSS, allocations,
   database/WAL size, bytes written and fsync/checkpoint events. Redis expiry,
   pub/sub and queues are separate requirements.
3. **JSON prototype.** Prefer the compatible immutable search change after
   profiling confirms allocation cost. Compare selected-path indexes with
   typed columns before considering a transactional posting index. Only add
   posting indexes if arbitrary containment or array membership is required;
   define missing/null/number/string/array semantics before implementation.
4. **Dense retrieval prototype.** Use the existing internal `IndexMethod`
   interface; do not promise a public ABI first. Compare exact filtered top-k
   with a dense IVF or HNSW prototype on fixed embeddings, at 100k then 1m rows.
   Agree dimension/model, metric and tenant selectivity. Filters must apply
   during candidate search, not only after global top-k. Report recall@k,
   latency, index/build memory, updates/deletes and restart cost. A flat exact
   vector scan is a reference, not an approximate index.
5. **Correctness and extensibility.** Prove insert/delete/rollback/savepoints,
   concurrent writes, reopen and injected crash behavior through existing
   integration/simulator harnesses. Then design a versioned loadable bridge
   with ownership, error/cancellation, transaction lifecycle, resource budgets,
   durable-format versions and ABI negotiation. Existing native plugins are
   trusted process code. The current loadable API lacks custom-index registration.
6. **Decision.** Evaluate each workload/deployment separately. Keep compatibility
   unless measured limits justify changing JSONB or the database format. Any
   incompatible change needs migration, crash/recovery and rollback procedures.
   A cache-removal go/no-go requires all application acceptance evidence, not
   scalar microbenchmark speedups.

The alternatives are a compatible JSON optimization first (recommended), a
retrieval-first internal index prototype if RAG is the priority, or a storage
rewrite. The last option currently has no measurement support and the broadest
migration/correctness cost.

## Accepted comparison target

The user requests matching the benchmarks in the main Turso repository, with
improvement over current main as the fallback. On inspection, upstream main was
`2709799dad2ebb0f1749cfa62fa28514f568da28`; its `benches/json_benchmark.rs`
blob `ee9a525d3f2dd4376e08a07fc7afa23fd2a8621b` exactly matches the fork baseline.
The existing JSONB Size, Sequential JSONB Calls and JSON Patch groups therefore
supply the first comparison workloads. They measure conversion/cache and patch
behavior, not scalar extraction or complete application requests. Preserve
these groups and add targeted extraction cases if an extraction optimization
is pursued. Use the same benchmark source, hardware, toolchain, build flags and
runtime configuration for baseline and candidate. Record both engine SHAs.
Compare Criterion estimates/confidence intervals and repeated raw results;
do not claim improvement from a single noisy timing. No absolute application
latency or recall requirement has been supplied.

The new file-backed probe supplements that suite with query plans and indexed
application-shaped operations. It does not replace upstream benchmarks or
establish application-level cache-removal acceptance.

## Remaining completion evidence

This probe addresses only a small part of the embedded baseline. Redis/service
comparison, profiler evidence, immutable extraction prototype, general JSON
index evaluation, dense ANN, lifecycle/crash validation and loadable ABI remain
open. There is no evidence yet supporting cache removal, a representation
change, a file-format change, or production-ready approximate vector search.
