# Shared checkpoint runtime

Implements the approved quiescent checkpoint policy in the shared runtime. The existing checkpoint gate and ID journal compactor are prerequisites; neither is a completed database checkpoint protocol.

## Admission and ownership

Pause new admissions before waiting for an empty global registration set. Existing writers must continue acquiring publication authority while the coordinator waits. Use a bounded deadline and return BUSY without evicting readers. Once no registrations remain, retain publication authority continuously through the checkpoint. Check local transactions as well, including abandoned local commit state, before modifying mappings or physical pages.

A new durable checkpoint intent is necessary. Main-file materialization can finish before the final generation publication, so an unchanged normal generation cannot alone prove that a cached local schema/root context remains valid after an interrupted checkpoint.

## Persistent order

1. Recover the selected manifest and complete logical prefix under authority. Load ID metadata against that exact generation and checkpoint identity. Compute the target generation with checked arithmetic; its checkpoint timestamp equals the selected committed timestamp.
2. Publish a checkpoint-intent manifest retaining the old acknowledged logical prefix, the old checkpoint-base timestamp, and the exact target generation/timestamp. FULL-sync matching intent records in both manifest slots before any physical materialization. Losing one slot must not make started materialization look like an ordinary old generation.
3. Run the existing blocking checkpoint materializer with a distinct private retain-log disposition. This disposition must preserve the old logical bytes and acknowledged offset while materializing and synchronizing main/WAL state. Ordinary checkpoints retain their existing truncate assertions. Shared mode forces durable synchronization regardless of connection synchronous settings.
4. Validate the physical checkpoint metadata and root/schema image against the intended timestamp. Prepare the target ID floor-only snapshot using the minimum of the old permanent floor and the negative bound covering the actual materialized physical roots, with checked conversion. There are no live claims or per-domain counters to migrate. Synchronize the target bank and mirror the active-plus-prepared ID selector; retain the old active bank. Preparing after materialization avoids guessing future root allocations or rewriting a referenced prepared snapshot.
5. Publish the completed target generation with an empty acknowledged logical prefix and zero salt, then FULL-sync matching completed records in both manifest slots. Only this operation may construct the coordinator's opaque `CompletedCheckpointProof`.
6. Finalize the prepared ID generation using that proof, mirror its selected witness, then reclaim the obsolete ID bank. Reclaim the old logical bytes only after completed-manifest mirroring. Keep authority pins until physical completion retirement.
7. Rebuild the local replay/base context and physical-root mapping at quiescence. Preflight allocations before replacing visible metadata. Invalidate prepared-schema generations and the relevant pager caches/pending reads. Idle workers must recognize the new generation and reload before registering a snapshot. Finish authority and release the admission pause last.

## Crash recovery

- Before durable intent mirroring: no physical materialization may have started. A raw caller-supplied generation is insufficient proof for discarding prepared state.
- After intent mirroring and before completed mirroring: new admissions remain blocked. Recover physical WAL/main state and complete materialization against the retained acknowledged logical prefix. Never infer that an old cached root map is reusable merely because the old log generation is still selected.
- After completed mirroring: the materialized base is authoritative for the old prefix. An old physical logical tail is unacknowledged and must never be replayed. Resume ID finalization and reclamation before enabling commits.
- During ID or log reclamation: the completed mirrored manifest and ID selector determine the only permitted generation. A corrupted selected prefix fails closed; do not fall back to an older bank or generation.

## Concrete seams and tests

The coordinator owns opaque completion/abort proofs. ID helpers may prepare a target, but cannot manufacture proof that the database was materialized. The existing manifest publication API consumes its authority guard while ID operations retain bindings; add a reviewed ownership-transfer seam that preserves the same authority and physical-retirement guarantees rather than dropping/reacquiring it between stages.

The existing `reset_after_vacuum` shows the metadata that needs rebuilding, but its assertions and infallible mutation sequence are not sufficient for shared interrupted-checkpoint recovery. Implement a fallible preflight variant under global quiescence, including row/index buckets, root bindings, row allocators, sequence state, schema/header counters and replay context. Test a cancelled delayed physical read against cache/pending-read invalidation so a late callback cannot make an old-generation page visible to a new transaction.

Required tests cover long-reader timeout without eviction, continued existing-writer progress while admissions pause, same-process and independent-process idle reload, every persistent transition with injected failure/cancellation, loss of either manifest/selector copy, retained-prefix recovery after main materialization, and repeat checkpoints under a fixed physical byte budget. The final PDO-worker suite must exercise checkpoint pressure and recovery with an external synchronized acknowledgement ledger.
