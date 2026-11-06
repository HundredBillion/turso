# Independent PHP worker concurrency

Status: prerequisite repairs are integrated and have passed focused Linux/Windows and local PHP/C/JavaScript verification (see the upstream reuse record). The full cross-process MVCC protocol is not implemented. Fork issue #7 stays open and its implementation PR stays draft until that separate acceptance milestone is complete.

## Problem Statement

Independent PHP-FPM workers need to share a local database while reading and changing content. The existing PHP compatibility work provides correct binding behavior and same-process MVCC operation, but MVCC transaction state, commit ordering and checkpoint ownership live inside one engine process. Experimental multiprocess WAL shares physical frames and serializes writes; it does not provide the requested overlapping MVCC transactions across workers.

## Solution

Provide opt-in embedded cross-process MVCC, reachable through PHP's C compatibility interface, with parallel transaction execution, consistent snapshots and globally ordered conflict validation and durable commit publication. Preserve ordinary opens and existing database files. A mandatory database daemon is outside this design.

## User Stories

1. As a site owner, I want independent PHP workers to share one local database so ordinary deployment does not require a database service.
2. As an editor, I want independent changes to proceed concurrently so another editor's transaction does not block every operation.
3. As a reader, I want a stable snapshot while other workers publish changes so a page does not combine incompatible states.
4. As an application developer, I want fresh transactions to see peer commits so workers do not retain stale content.
5. As an application developer, I want conflicting updates and unique keys to return defined errors so retries cannot silently lose data.
6. As an application developer, I want generated IDs and sequences to remain unique across workers.
7. As an application developer, I want explicit transactions retried only as complete units under application control.
8. As an operator, I want acknowledged transactions preserved through process death and rolling restarts.
9. As an operator, I want checkpoints and log reclamation to complete without invalidating another worker's reader.
10. As a plugin developer, I want memory, TEMP, attached and read-only databases to preserve their documented permissions and isolation.
11. As a maintainer, I want existing upstream fixes reused with their authorship and known limitations visible.
12. As a site owner, I want performance evaluated using actual PHP workers and equal durability before changing the chosen database.

## Implementation Decisions

- Repair and review upstream startup, ATTACH, registry-option and shared-index changes independently. Their presence alone does not complete the cross-process MVCC requirement.
- Keep per-process MVCC stores and investigate live replay from a globally coordinated committed logical-log prefix. A shared authority owns epochs, transaction/snapshot registration, commit ordering and process liveness. Rust heap pointers and synchronization primitives must not be interpreted as process-shared data structures.
- Transaction begin imports an agreed committed prefix and registers its snapshot with retention coordination. Commit catches up under global append authority, validates rows and index uniqueness against intervening commits, makes the append durable as requested, and then publishes success. Executing transactions may overlap; final commit publication follows one recoverable order.
- Coordinate rowid and sequence allocation across workers. No duplicate generated key is accepted as a successful independent create.
- Recover abandoned append ownership before another writer acknowledges a commit. Define torn-tail handling and the relationship among durable bytes, published committed position and acknowledgements.
- Coordinate checkpoint election, schema/root publication, active snapshots and replay positions. Reclamation must retain everything required by active transactions and lagging workers. Permanently disabling checkpoints or allowing unbounded retained history is not a completed implementation.
- Use an explicit validated opt-in reachable through the C/PHP interface. Existing opens retain their defaults. Keep the current MVCC/multiprocess safety guards until the new protocol is implemented and verified; unsupported platforms, custom storage and incompatible live protocol versions fail clearly.
- Start verification on supported 64-bit Linux and local filesystems. Do not advertise unverified Windows, IOCP or network-filesystem operation. Existing supported configurations remain covered by regression checks.
- Keep this work on its own fork branch. The PHP compatibility PR receives only fixes to its own regressions; new concurrency imports and protocol work are reviewed separately.

### Approved initial protocol policies (2026-11-06)

- Expose the opt-in through a validated URI parameter usable by the C/PHP interface.
- The initial shared-mode PHP release supports PDO URI opens only (user approved 2026-11-06). Ordinary SQLite3-class behavior remains unchanged. Do not introduce a vendor open flag or filename rewriting for the SQLite3 class in this release. Shared-mode acceptance must attest the capability through PDO; ordinary SQLite3 remains part of default-mode regression coverage.
- Start with globally quiescent checkpoints. Pause new transaction admission while waiting within a bounded busy deadline for active transactions to finish. Preserve readers rather than silently aborting them.
- Enforce a configurable retained-log limit. If active snapshots prevent reclamation, additional commits return bounded `BUSY`; history must not grow without bound. Reject a transaction larger than the limit before appending any of it.
- Read-only participants in this explicit mode may write coordination metadata while database, WAL and logical-log contents remain read-only. Reject the opt-in when coordination metadata cannot be written. Existing ordinary read-only opens retain their current guarantees.
- Use a versioned on-disk mode marker for the explicit opt-in so older binaries reject the new mode, including read-only opens. Continue reading all existing database formats. An opted-in database requires explicit safe downgrade before older binaries can reopen it; upgrade and downgrade must preserve acknowledged commits across crashes.
- Existing databases require an explicit offline upgrade with all older workers stopped, including unlocked read-only workers. The user approved this prerequisite on 2026-11-06. A normal shared-mode URI open must not silently upgrade an existing old-format database. Newly created databases may initialize directly in the new mode.

The source-level protocol proposal and delivery boundaries are in
`docs/design/php-worker-mvcc-protocol.md`. These policies do not imply that the
runtime protocol is implemented or verified.

## Testing Decisions

Use public C/PHP operations and real independent child processes as the main acceptance boundary. Existing native subprocess, yielded-I/O and deterministic scheduling facilities support narrowly minimized storage regressions. Thread-only success is insufficient for the cross-process claim.

| Acceptance | Required observation |
| --- | --- |
| Overlap | Barriers hold two independent write transactions open while an independent reader executes. |
| CRUD | Creates, own reads, updates and deletes produce exact expected committed rows. |
| Snapshot isolation | An active reader retains its old view; a fresh transaction sees peer commits. |
| Conflicts | Same-row update/delete and unique-index conflicts return explicit bounded outcomes. |
| IDs | Concurrent automatic IDs and sequences remain distinct and survive reopen. |
| Rollback | One worker's rollback cannot expose or erase another worker's commit. |
| Atomicity | Acknowledged multi-row transactions appear completely after fresh-process recovery. |
| Durability | An external acknowledgement ledger agrees with recovered data after worker churn and forced crashes. |
| Lifecycle | Concurrent creation, creator death, rolling restart and orphaned ownership recover safely. |
| Checkpoint | Long readers and lagging workers survive checkpoint publication and log reclamation. |
| Failure injection | Allocation failures, short writes and death during append, sync and checkpoint never acknowledge partial work. |
| Permissions | Read-only main/attachments reject persistent writes while allowed private TEMP operations work. |
| Compatibility | Memory/VFS behavior, existing files and incompatible live coordination versions have defined outcomes. |
| Performance | Equal-durability PHP-worker workloads report latency, throughput and memory after correctness passes. |

Treat the acknowledged-commit loss and corruption reported in upstream issue #11383, including its two-process rolling-restart scenario, as required investigation and release gates. No local reproduction or fix is claimed by this document. Keep the unrelated autocommit retry proposal optional: it does not make explicit transactions or processes safe automatically.

## Out of Scope

A mandatory daemon, a change to Mosaic's SQLite default, distributed replication, cloud services and lock-free commits are outside this work. Serialized multiprocess WAL is an independently useful milestone, not the final overlapping-MVCC-writer deliverable.

## Further Notes

The implementation PR remains draft until the full acceptance contract is demonstrated. It may contain independently reviewed prerequisite repairs first; those imports cannot close issue #7 by themselves. Upstream contributor credit and exact imported revisions belong in the dependency record and commit history.
