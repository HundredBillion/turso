# Shared generation reload

Implements the approved quiescent checkpoint reload requirement in the shared checkpoint TSP. The coordinator retains global admission pause and authority; no active local transactions or global registrations may survive installation. Preserve the existing MVCC store, its monotone clock and transaction identifiers.

## Root metadata preparation

The existing root map cannot be replaced without allocating new skiplist nodes after clearing old state. Store that map in an ArcSwap so a complete replacement can be built fallibly with the store allocator before installation. Readers retain a load guard while holding entries or iterators. Mechanical changes are limited to root-map consumers; ordinary root lookup and update semantics remain unchanged.

- [x] Convert the root-map field and its readers; compile and run existing MVCC root/checkpoint tests.
- [x] Add a private prepared reload object in `core/mvcc/database/shared_generation_reload.rs`. Validate the materialized header, target timestamp and positive physical roots; allocate the complete canonical root map before clearing existing metadata.
- [ ] Verify allocation failure preserves existing root bindings and transaction identity. Install only under retained quiescence, clear obsolete versions/indexes/row allocators/sequences/dirty and finalized state, update header/base timestamps and schema generation without rewinding the clock.
- [x] Add a private pager generation-reset operation that rejects active/dirty pager state and discards cache and pending-read entries. A delayed callback may complete its old page but must not make that page visible to a subsequent generation read.
- [ ] Coordinate the final proof type and replay-context installation with the checkpoint owner. The reload helper must not fabricate physical materialization or completion proof from caller-supplied timestamps.
- [ ] Run focused failure and delayed-read tests, native combined runtime validation, and independent review before integrating.

The coordinator still owns parsing a fresh physical schema, publishing the completed generation, finalizing ID state, updating replay context, and releasing admission last. This component must not activate PDO or claim complete checkpoint recovery on its own.

## Continuation verification

The root-map preparation and allocator failure checks pass (3 tests); the MVCC
database subset passes 560 tests with 6 ignored. Pager generation reset passes
7 tests, including delayed reads and physical header reload. Strict core library
Clippy with `bench,conn_raw_api` and formatting checks pass on Rust 1.88.

The original missing installer was confirmed on Linux in
[run 42300733437](https://github.com/HundredBillion/turso/actions/runs/42300733437)
against `dfb36d871`. The installer now exists, but its two genuine completed-proof
native tests still require Linux verification. The positive test also resets its
pager before installation, as required of the coordinator. This does not establish
a separate stale-header defect or activate the public mode.

Production coordinator integration remains incomplete: finalize ID state, parse
and install the physical schema/base context, replace replay state for the new
generation, reclaim retained history and release admission last. Existing replay
refresh rejects a changed log generation until that integration exists.
