# Bounded mixed-write followup protocol

Status: detailed protocol approved by the coordinator after review on 2026-11-06.
Implementation follows immutable screening; no mutable-workload result is claimed. This is a test protocol for one controlled application process,
not a general cache-consistency protocol for multiple applications or independent
writers. All database mutation must pass through its writer gate.

## Admission, commit and cache visibility

All application workers share one read/write gate and a monotonically increasing
cache generation. A read holds a shared guard across Redis GET, any database miss
and Redis SET. Its key includes a random process-start namespace, generation,
tenant and complete canonical request. Writers acquire the exclusive guard,
execute one database transaction with WAL/FULL, and wait for a successful COMMIT
response. Before releasing the guard, a successful writer increments generation
and publishes the corresponding reference-model state. Every subsequent read
uses the new generation; readers that began earlier finish before the writer
commits. Thus an old cache miss cannot repopulate a visible new-generation key.
Facet queries and negative results are invalidated along with point results.

DB-only controls use the same gate and reference-model work. This measures the
protocol's admission/serialization cost fairly, while exposing the extra miss
and population cost imposed by Redis after updates. Generations are not deleted
synchronously; the dedicated Redis instance's bounded memory/eviction policy
limits obsolete entries. Record live/peak memory and evictions. A fresh random
namespace on application restart prevents generation reuse from exposing old data.

A failed or uncertain write outcome never returns a success acknowledgment or
allows later reads through the gate. The trial fails closed, closes clients and
recovers the database before further traffic. In particular, a lost response can
mean the transaction committed; do not infer rollback from a network error.
Recovery determines the durable outcome before a fresh namespace permits reads.
The protocol cannot guarantee coherence for writes made outside its gate.

## Reference state and timing

Each document has an explicit version and deterministic update content. Include
an inventory/order transaction that decrements inventory and inserts an order
carrying the same commit/version ID. The reference invariant is initial inventory
minus current inventory equals committed order quantity for each tenant/product;
there must be neither an order without its decrement nor a decrement without its
order. Use sufficient initial stock or explicitly model insufficient-stock
rejection. Change a document facet value in the same transaction so generation
invalidation is tested for both point and facet responses. The
reference model records successful commit order and expected document versions,
including tenant and facet changes. Each read captures the model's visible
commit generation while holding the same shared guard; its response is checked
against that immutable generation after timing. Database results must not be used
to manufacture their own expected values. Reference-model preparation, allocation
and validation overhead must be reported and either placed outside request timing
without delaying other workers, or included equally in both variants. Do not hide
model work inside a gate after stopping a writer's timer while it blocks readers.

Request latency starts at scheduled arrival and includes gate wait, database or
cache service, FULL commit and generation publication. Record actual-start
lateness, gate-wait time, service time, successful reads, acknowledged writes,
errors and retries separately. The initial mix should be selected from immutable
screening capacity, then use fixed request traces with both low and moderate
write fractions. Use the same offered load, trace, gate and durability settings
across DB-only/Redis and upstream/current-fork/candidate controls.

## Checkpoint and recovery evidence

Run explicit PASSIVE checkpoints from a separate database connection at a fixed
request or elapsed-time cadence, recording attempts, busy results, elapsed time
and completed work. Keep FULL commit durability unchanged. Report database/WAL
size and system write counters before/after; distinguish full-run counters from
per-operation device writes. A clean restart check compares all document versions
and query results with the committed reference before allowing cache traffic. For actual process
restarts, a surviving independent controller or external acknowledgment ledger
records attempted transaction IDs and observed success acknowledgments. It must
survive the application process; the database under test cannot be its own oracle.
Classify successful commit with lost acknowledgment explicitly from this ledger.

At deterministic failure points, use the existing UnreliableIo harness to retain
only synced DB/WAL bytes, then reopen with fresh PlatformIO. A crash before commit
acknowledgment may recover either a completed transaction or its predecessor;
all document changes must belong to a consistent committed prefix. Every
acknowledged FULL commit must survive. No recovered document may combine fields
from different versions. Include failure before COMMIT, after successful COMMIT
but before generation publication, after generation publication, and during
checkpoint backfill/sync. The lost-ack case must be identified explicitly rather
than counted as a missing acknowledged write.

Restart with a fresh cache namespace and validate point, typed-facet and JSON-
facet responses against the recovered prefix. Retain the old Redis contents to
prove namespace isolation. Only after these controls and sustained comparisons
pass can this bounded single-application protocol support a mutable-workload
comparison. It still does not establish multi-host or independent-writer cache
removal safety.

This harness exercises ordinary WAL transactions and WAL checkpoints. It does
not validate the separate, unfinished multiprocess MVCC protocol tracked in #7,
and its results must not be used as evidence that those lifecycle gaps are fixed.
