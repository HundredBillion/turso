# Shared generation reload

Implements the approved quiescent checkpoint reload requirement in the shared checkpoint TSP. The coordinator retains global admission pause and authority; no active local transactions or global registrations may survive installation. Preserve the existing MVCC store, its monotone clock and transaction identifiers.

## Root metadata preparation

The existing root map cannot be replaced without allocating new skiplist nodes after clearing old state. Store that map in an ArcSwap so a complete replacement can be built fallibly with the store allocator before installation. Readers retain a load guard while holding entries or iterators. Mechanical changes are limited to root-map consumers; ordinary root lookup and update semantics remain unchanged.

- [ ] Convert the root-map field and its readers; compile and run existing MVCC root/checkpoint tests.
- [ ] Add a private prepared reload object in `core/mvcc/database/shared_generation_reload.rs`. Validate the materialized header, target timestamp and positive physical roots; allocate the complete canonical root map before clearing existing metadata.
- [ ] Verify allocation failure preserves existing root bindings and transaction identity. Install only under retained quiescence, clear obsolete versions/indexes/row allocators/sequences/dirty and finalized state, update header/base timestamps and schema generation without rewinding the clock.
- [ ] Add a private pager generation-reset operation that rejects active/dirty pager state and discards cache and pending-read entries. A delayed callback may complete its old page but must not make that page visible to a subsequent generation read.
- [ ] Coordinate the final proof type and replay-context installation with the checkpoint owner. The reload helper must not fabricate physical materialization or completion proof from caller-supplied timestamps.
- [ ] Run focused failure and delayed-read tests, native combined runtime validation, and independent review before integrating.

The coordinator still owns parsing a fresh physical schema, publishing the completed generation, finalizing ID state, updating replay context, and releasing admission last. This component must not activate PDO or claim complete checkpoint recovery on its own.
