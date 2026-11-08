# Private shared ID opcode integration

Status: source audit; the owner accepted the yielding boundary below. Runtime implementation and sequence-domain details are pending. Public mode activation remains disabled.

The VDBE must not publish a generated ID or advance its program counter until the shared reservation is durable and the runtime has retired backend ownership. Existing transactions reserve through publication authority, without entering the new-transaction admission gate: a checkpoint can pause new admissions while waiting for these writers to finish.

## Source seams

- `new_rowid_inner` in `core/vdbe/execute.rs` currently uses `MvCursor::start_new_rowid` and a process-local allocator. Select the actual cursor database via `ProgramState::cursor_databases`, and obtain the cursor's logical table and transaction identity. Main-store presence alone is insufficient for attached/TEMP/ephemeral cursors. Preserve the ordinary cursor path.
- `op_create_btree` currently allocates a negative logical ID and records it in the transaction. Shared reservation must finish first; record the returned ID once, then write the register and advance.
- MVCC AUTOINCREMENT does not execute `NewRowid`: `emit_rowid_generation` delegates to the sequence backing-table RMW. Wrapped inner transactions register the outer claim in `SequenceRegisterAllocation` before committing the new boundary. The skipped-inner/autocommit paths are handled by `SequenceTrackAllocation`; both need an explicit shared contract.
- Explicit rowids bypass generation. `ClaimExplicitRow` therefore needs a write-path seam that also covers UPDATE changing a key and replacement/conflict handling. Do not infer coverage from tests of generated rowids.
- Named savepoints, internal statement savepoints, rollback, reset and drop currently include synchronous local cleanup. Durable claim release cannot be implemented by yielding inside Drop. The runtime must retain transaction-owned claims and unfinished release state independently of ProgramState, or conservatively retain claims until safe owner cleanup/recovery.

## Proposed yielding boundary

A private `SharedIdState` on ProgramState captures the request and retains runtime polling state. Its reset may drop unfinished local state only with backend lease pins and recovery fencing intact. Finished claim ownership belongs to the transaction, not this opcode state. A typed runtime request/result avoids exposing IDs journal internals to the VDBE. Each resume must use the captured database/transaction/domain/request, and must reject accidental reuse for a different opcode operation.

Agreed API names are `SharedIdState`, `SharedIdRequest`, and `SharedIdResult` (subject to the owner export). The request surface is `ReserveRow { table_id }`, `ClaimExplicitRow { table_id, value }`, `ReserveLogicalTable`, and `ClaimSequence { table_id, value, autonomous }`; the result is `Value(i64)` or `Claimed`. `shared_id_nonblock` receives connection, pager, transaction ID, database ID, request and mutable state. Runtime captures these on first entry and derives the owner/generation from the retained transaction registration.

Policy `claim()` currently assigns a new ordinal for every call. The opcode must therefore remember completion across a later yielding insert. Register/Track duplicate sequence claims and generated-row/Insert backstop claims also need an explicit idempotence contract; allocating extra ordinals changes capacity and savepoint semantics. The optional `NewRowid.prev_largest_reg` output is emitted only on the non-MVCC AUTOINCREMENT branch today; the shared implementation must not fabricate a previous maximum from a random-at-MAX result.

The owner will supply the actual API and latest-committed oracle. The opcode layer must never temporarily swap the caller's transaction to perform oracle reads. Cursor logical-table identity needs a narrow accessor; schema names or physical root-page numbers cannot substitute across checkpoint generations.

## Required evidence

1. Ordinary WAL/MVCC generated IDs, explicit IDs, TEMP and attached paths retain their behavior.
2. Two independent shared instances reserve distinct rowids and negative logical IDs; AUTOINCREMENT and sequence publication preserve outer/inner transaction ownership.
3. Delayed ID writes/syncs resume the same opcode without duplicate claims or early register/PC publication; reset/drop retains required physical ownership and recovery fencing.
4. Explicit high keys, INSERT OR REPLACE, UPDATE key changes, aborted statements, named/nested savepoints and transaction rollback receive distinct tests.
5. A writer already admitted can finish an ID reservation while a checkpoint pauses new admissions; no admission-gate wait cycle.
6. Capacity rejection before physical I/O is distinguishable from an uncertain durable append; only the proven safe case may be retried as such.

No claim of full allocation acceptance is made until the runtime API, opcode routing and independent-worker tests run together.

## Initial native evidence and remaining boundaries

The test-only source `1e6fbaa2b` ran on native Linux in [run42302197993](https://github.com/HundredBillion/turso/actions/runs/42302197993). All three tests failed at their intended behavioral assertions: two independent stores allocated rowid2, two pending schema transactions allocated logical table−4, and generated allocation ignored a peer's explicit live key100. The workflow requires those assertion messages, so compilation failures do not satisfy this RED evidence. These tests are not yet passing with the new runtime.

The owner confirmed the initial runtime API rejects a shared database ID other than main before I/O, because the checked oracle currently uses the main connection/pager. Ordinary nonshared attached/TEMP behavior remains on its original path; shared ATTACH support is a remaining integration gate.

The owner also confirmed sequence claims use the backing table's logical ID (including implicit AUTOINCREMENT sequences). Wrapped inner preclaims use the saved outer transaction and `autonomous=true`. The authoritative policy will make repeated owner/domain/value claims idempotent at their earliest ordinal. Opcode polling still must retain the explicit claim→insert phase boundary, so a later cursor yield does not restart a completed claim.

Publishing shared sequence claims is not sufficient to certify `sequence_watermark_experimental()`: its reader currently consults process-local allocations. A shared lowest-live-claim read or explicit unsupported-mode guard is still required. Transaction/savepoint cleanup, cancellation, capacity and actual independent-worker acceptance remain outstanding until the runtime and VDBE run together.
