# Live peer-frame replay technical spec

> **For agentic workers:** REQUIRED SUB-SKILL: Use dmi-superpowers:executing-plans to implement this TSP task-by-task.

**Goal:** Import a verified peer commit into an existing local MVCC store without changing active snapshots, losing local transaction versions, or leaving partial visible state after a decoding/allocation failure.

**Architecture:** Prepare complete decoded operations and schema transitions before publication. Resolve affected canonical version chains, exclude concurrent changes while staging their replacement contents, then publish through an infallible application step. Keep the existing chain Arcs and local TxID versions. New-mode transaction admission uses the shared timestamp allocator; existing modes keep their existing clock and recovery behavior.

**Source requirements:** `docs/PRDs/11-06-2026-independent-php-worker-concurrency.md` and `docs/design/php-worker-mvcc-protocol.md`. The user-approved quiescent-checkpoint policy permits physical root changes only after all shared snapshots drain.

## Constraints and audited hazards

- `maybe_recover_logical_log` is startup recovery: it resets the local clock and durable boundary. It is not a live-import API.
- `recover_process_frame` currently takes context maps before fallible decoding and applies row/root/header mutations incrementally. Calling it on a live store does not provide atomic import.
- Snapshot timestamps must come from shared `register_snapshot`, and commit timestamps from the same shared authority. Local clock values cannot be mixed into that order.
- Do not take the transaction-lifetime checkpoint write gate to import: long readers must keep running while peer commits become available to later snapshots.
- Resolve canonical index keys using their existing index comparators. Deduplicate affected chain Arcs, then lock by a single process-local allocation-address order. Audit every nested lock path before adopting this ordering; never acquire clock/admission after acquiring chain guards if another path takes them in the opposite order.
- After acquiring all affected chain guards, verify each chain remains the map's canonical entry. GC can unlink an empty entry before its guard is obtained. Retry before visible mutation if identity changed.
- Prepare replacement contents while chain mutation is excluded. Preserve local TxID versions and the existing chain Arcs referenced by transaction write sets.
- Missing logical root IDs may be pre-created with no physical root. Existing physical bindings remain unchanged until quiescent checkpoint. Schema drops retain backing/root mappings needed by old snapshots.
- Complete all decoding, schema validation, payload/index-info allocation, chain cloning/capacity allocation, and bookkeeping preparation before modifying visible version contents. Empty pre-created entries are the only permitted preflight residue.
- Publication has no I/O, fallible allocation or schema parsing. Update local imported boundary only after all chain contents/header/schema state are installed.
- Shared authority remains owned throughout import and actual transaction insertion/commit validation. Do not expose the public opt-in until the entire protocol is connected and acceptance passes.

## Task 1: Prepare the complete frame

**Blocked by:** Strict prefix reader; manifest recovery selection.

**Files:** `core/mvcc/database/mod.rs`, a focused replay module if extraction improves clarity, and database unit tests.

- [ ] Build a non-mutating frame preparation seam from existing before/after schema and index decoding logic.
- [ ] Reject inconsistent timestamps, malformed records, missing index metadata, incompatible logical root changes and invalid schema transitions before touching visible store state.
- [ ] Cover a valid first operation followed by a malformed late operation: no row/header/schema/imported-boundary change and no lost recovery context.
- [ ] Exercise CREATE/DROP/ALTER and index-before/schema-after ordering already supported by startup recovery; retain normal recovery behavior.

## Task 2: Atomic installation into existing chains

**Blocked by:** Task 1; completed lock-order audit.

- [ ] Add the smallest local admission/chain-lock mechanism required by the audit, with no process-wide transaction serialization.
- [ ] Pre-create missing empty entries, resolve/deduplicate canonical chains, acquire guards in one order and revalidate map identity.
- [ ] Stage all touched chain contents and metadata before publication, preserving active local versions and old-snapshot history.
- [ ] Apply prepared state without failure points, then publish the imported watermark/schema generation.
- [ ] Inject allocation failures at each preparation stage; retry the same frame and prove no duplicate rows, ended versions, skipped operations or lost local writes.
- [ ] Race local insert/delete/rollback/GC with import; prove old snapshots stay stable and later snapshots see the complete peer frame.

## Task 3: Yielding admission and commit integration

**Blocked by:** Task 2; durable manifest loader/publisher and shared authority.

**Files:** `core/vdbe/execute.rs`, `core/mvcc/database/mod.rs`, transaction/state ownership definitions and logical storage integration.

- [ ] Add yielding opt-in admission before synchronous MVCC transaction insertion; retain authority and snapshot registration until insertion succeeds or fails.
- [ ] Use globally issued snapshot/commit timestamps and retain registrations for the full transaction lifetime, including cursor, savepoint and error paths.
- [ ] On commit, import the current agreed prefix before validating local row/unique conflicts; serialize append and manifest publication with shared authority.
- [ ] Establish logical-log sync before manifest write and typed durable-manifest proof before mapped publication/acknowledgement.
- [ ] Preserve local transaction execution overlap, bounded BUSY behavior and existing-mode defaults.
- [ ] Run independent-process conflicting/disjoint writers and long-reader acceptance. These tests are required in addition to private component tests.

## Remaining delivery boundaries

This work does not replace mode-marker upgrade/downgrade, global row/table/sequence reservations, quiescent checkpoint/reclamation, retained-log-cap admission, validated C/PHP URI plumbing or full PHP-worker acceptance. All remain required before issue completion.
