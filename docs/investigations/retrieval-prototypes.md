# Approved retrieval experiments

The user approved both experiments on 2026-11-06. They are experiments, not approved production features or interfaces.
See `json-postings-results.md` for the implemented SQL-backed probe. Compare against the fork's current main using identical
workloads and configurations. Preserve exact results as the correctness baseline;
an approximate method reports its recall/latency curve without an assumed
acceptable recall loss. No extension ABI or file-format commitment follows from
a successful microbenchmark.

## Transactional JSON postings

Question: when queries use many unpredictable scalar paths, can transactional
postings outperform JSON scans after including update cost? First compare
selected-path expression indexes and ordinary columns using the existing probe.
They remain the simpler solution for predictable paths.

Use a throwaway SQL-backed prototype before introducing planner contracts. A
document table owns `(id, tenant, payload)`. A posting table stores
`(tenant, path, kind, scalar, document_id)` and an index in that order. Store
numeric scalars as their original INTEGER or REAL values; never cast all numbers
to floating point. A second form for scalar array membership includes the array's
path and deduplicates results by document ID. The reference implementation scans
documents using the same explicit type predicate and extraction operation.

Define the first probe's semantics narrowly:

| Case | Probe rule |
|---|---|
| Missing path | No posting; distinct from JSON null |
| JSON null | `kind = null`, matched with `IS NULL` |
| String / number / boolean | Distinct kinds; numeric INTEGER/REAL equality follows engine comparison without lossy conversion |
| Arrays | Scalar membership only; repeated equal members do not duplicate document results |
| Objects | Scalar leaves at explicit canonical paths; no recursive containment claim |
| Duplicate object keys | Rejected by prototype triggers; production behavior must be specified separately |
| Tenant | Required leading index predicate and checked against the returned document |

One transaction replaces the document and all its postings. Measure the cost of
that full replacement before considering incremental maintenance. Test failed
updates, rollback, savepoints, deletes, two writers, reopen and injected crashes
against a scan oracle. Reopen must never expose a document/posting disagreement.
Use existing simulator failure points rather than inventing durability claims
from orderly close/reopen. Report index bytes and write amplification along with
latency. Only then decide whether an internal index method and planner integration
earn their maintenance cost.

## Dense retrieval through the internal index interface

Question: can a tenant-aware dense index beat exact filtered scans, and what
accuracy is lost when it searches fewer candidates? Compare alternatives:

1. Exact filtered scan, using existing `vector32` and cosine distance: reference
   results, minimal implementation, increasing work with eligible rows.
2. Dense IVF probe: deterministic training, persisted centroids and posting lists,
   adjustable number of searched clusters. Its candidate count is easy to measure
   and its persisted state can use existing transactional backing storage.
3. HNSW integration: a useful later comparison, with more complex transactional
   graph mutation, memory and recovery requirements. Avoid choosing it before the
   simpler probe answers the filtering and lifecycle questions.

Recommend IVF for the first internal throwaway prototype. Its attachment declares
the vector and tenant columns and validates dimensions/finite nonzero vectors.
Use existing `IndexMethod`, `IndexMethodAttachment` and `BackingStore` mechanisms,
with `TransactionalBackingStore` only after the implementation actually stores
all durable state there. Do not modify the public native-extension table.

Persist a format/version record, dimensions/metric, centroids, and entries keyed
by `(tenant, cluster, document_id)`. Keep vectors in backing storage for exact
candidate reranking. Training occurs outside write transactions; installing the
complete index and its version is transactional. Inserts/deletes use normal
transactional index callbacks. Rebuild is the first response to drift; background
retraining and concurrent index generation swaps are separate designs.

At query time, rank centroids for the query vector, scan only the requested
tenant's selected clusters, and compute exact cosine distances for those
candidates. Permission filtering must happen before accepting a candidate. The
first probe can use a tenant boundary only and must explicitly exclude more
complex authorization claims. Search all clusters as an exactness check, then
sweep the probe count for the recall/latency curve. Break equal scores by document
ID in both reference and index results.

Use deterministic synthetic data first to check mechanics, then supplied or
appropriately licensed representative embeddings for performance conclusions.
Run 100k rows before 1m, with recorded dimensions and tenant skew. Include empty
tenants, highly selective tenants, queries near cluster boundaries, duplicate
vectors, updates and deletes. Reject invalid dimensions, NaN/infinity and zero
norm with explicit errors. Compare recall@10, candidate count, p50/p95/p99,
build time, memory, durable/index size, update throughput and restart time.

Do not call this ready until rollback/savepoints/concurrency/delete/reopen/crash
tests match the exact filtered reference. FTS rank fusion remains an application
experiment after lexical and dense candidate generation work independently.

## Loadable interface after the prototype

The current `extensions/core/src/lib.rs::ExtensionApi` offers scalar, aggregate,
virtual-table and optional VFS registration. Custom index lifecycle contracts
live separately in `core/index_method/mod.rs`; adding a function pointer is not
a complete bridge.

After the internal experiment identifies actual requirements, write a separate
versioned interface proposal covering struct size/version negotiation, opaque
handles and ownership, allocator boundaries, asynchronous I/O and cancellation,
error propagation, transaction/snapshot identities, statement/savepoint/rollback
events, durable format versions, budgets, and safe rejection of unsupported
capabilities. Native extensions remain trusted in-process code. Validate an
out-of-tree extension and compatibility failures before declaring stability.
