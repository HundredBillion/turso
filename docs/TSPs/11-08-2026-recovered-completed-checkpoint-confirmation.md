# Recovered Completed Checkpoint Confirmation Technical Spec

> **For agentic workers:** REQUIRED SUB-SKILL: Use dmi-superpowers:executing-plans to implement this TSP task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Confirm an already materialized, normal empty-prefix checkpoint after losing the original process and completion proof, while retaining unrecovered authority and paused admission.

**Architecture:** Consume the sealed physical-and-ID inspection result from `e60715199`. Suspend its opaque IDs, freshly reselect the identical completed manifest under the same lease, and mirror that actual record through the existing manifest write/FULL-sync state machine. A private manifest child creates completion evidence only after both slots and all physical completions retire; the runtime coordinator retains the physical facts, IDs, maximum owner epoch, guard and pause.

**Tech Stack:** Rust 1.88; existing ManifestLoad/ManifestPublication; retained native files; existing publication I/O pins; Linux OFD snapshot/admission coordination.

## Global Constraints

- Existing worktree `/Users/davidlee/Projects/turso-physical-connection`; new branch `codex/shared-completed-checkpoint-confirmation` preserves `codex/shared-checkpoint-recovery-entry` at `e60715199572d8fc168d40e2c08e179404248341`.
- Existing user approvals cover this completed-checkpoint protocol. Parent reviews this exact seam before production or stub implementation.
- This slice accepts only a normal record with zero prefix offset, zero CRC and zero salt; actual physical persistent timestamp equals its committed timestamp. Intent, downgrade and nonempty-WAL recovery remain rejected.
- No mapped recover, ID finalization, logical reclaim, Database bootstrap, replay publication, authority finish or admission release is added.
- Source identity is absent. Source database comes only from the inspected selected ID state, or is absent for an already finalized selector. Never derive a predecessor generation or source from a timestamp.
- Keep `SuspendedRecoveryIds` opaque and non-rejoinable. Retain the inspected maximum owner epoch separately across suspension.
- Do not relax ordinary `LoadedManifest::publish` checks or increment manifest sequence: this operation remirrors the actual selected record.
- Existing read-only inspection evidence: native run 42433975156 covers exact e607; no new baseline failure is known. Reuse existing coordinated targets; no fresh large target directory.
- Public mode and PDO activation stay disabled. Validation workflow commits remain outside product branches.

## Files and interfaces

- Add `core/mvcc/database/shared_checkpoint_recovery_confirmation.rs`, declared as a child of `shared_checkpoint_recovery_ids`. It may consume that parent's sealed `InspectedRecoveryIds` fields; raw constructors remain private.
- Add `core/mvcc/commit_manifest/checkpoint_recovery_confirmation.rs`, declared beneath `checkpoint_publication`, so it can create its parent's private `CompletedCheckpointProof` without broadening proof field visibility.
- Add narrow MVCC-only evidence type reexports through `shared_runtime.rs` and `database/mod.rs`. The evidence has private fields and no public constructor or mutable accessors.
- Add native tests beside the runtime confirmation child, reusing `shared_completed_checkpoint_fixture` and existing manifest fault/callback controls where possible.
- Preserve `shared_ids/checkpoint_recovery.rs`'s opaque suspension barrier; no generic rejoin method is added.

Planned runtime entry and result:

```rust
impl InspectedRecoveryIds {
    pub(in crate::mvcc) fn begin_confirmation(self)
        -> Result<RecoveryCheckpointConfirmationState>;
}
impl RecoveryCheckpointConfirmationState {
    pub(in crate::mvcc) fn poll(&mut self)
        -> IOResultOr<RecoveredCheckpointConfirmation>;
}
```

`RecoveredCheckpointConfirmation` has private fields holding the same runtime, PhysicalBaseParts, SuspendedRecoveryIds, max_owner_epoch, guard, pause and CompletedCheckpointProof. Read-only proof/guard/maximum-epoch access is sufficient for this slice's tests; no consuming mutation bridge is exposed yet.

`RecoveryCompletionEvidence` has private fields binding the exact inspected ManifestRecord, target RecoveryIdentity, optional source DatabaseGeneration, actual physical floor, native main identity, the retained actual manifest File Arc and its native identity, and Weak publication lease. Its only constructor consumes validated fields inside the runtime confirmation child. Read-only methods let the manifest child compare the freshly loaded record and held lease; arbitrary timestamps and raw guards cannot construct this evidence.

Planned manifest seam:

```rust
impl LoadedManifest {
    pub(in crate::mvcc) fn mirror_recovered_completion(
        self,
        evidence: RecoveryCompletionEvidence,
    ) -> Result<RecoveryCompletionMirror>;
}
impl RecoveryCompletionMirror {
    pub(in crate::mvcc) fn poll(&mut self)
        -> Result<IOResult<MirroredRecoveryCompletion>>;
}
```

`MirroredRecoveryCompletion` is sealed and yields only the actual proof and original guard to the runtime coordinator. It cannot initialize mapped state or release admission.

## Task 1: Real completed fixture and missing confirmation regression

**Blocked by:** Parent approval of this seam.

**Files:** Runtime confirmation child; declaration; focused native fixture/test.

**Interfaces:** Consumes `InspectedRecoveryIds`; introduces the exact confirmation entry above with an explicit unimplemented error and no proof construction.

- [x] Create a genuine completed fixture; save target/source/floor expectations from the original real proof only for test assertions. Drop original IDs, guard, proof, pause, connection and Database before recovery starts. Retain only runtime/files/configuration and the directory's lifetime.
- [x] Acquire fresh paused recovery selection, inspect actual retained main, then inspect selected IDs through existing APIs.
- [x] Call `begin_confirmation` and drive `poll`; the first test requires real confirmation and therefore fails with `recovered checkpoint confirmation is not implemented` before implementation.
- [x] Pin the exact source in the owned native workflow and verify explicit missing implementation (zero passed, one failed), after the existing recovery entry/physical/ID prerequisites pass. Do not treat compilation failure or timeout as behavior RED.

Test's real inspection sequence:

```rust
let mut opening = runtime.begin_checkpoint_recovery().unwrap();
let selected = io.block(|| opening.poll()).unwrap();
let mut physical = selected.begin_physical_read(
    crate::OpenOptions::new(Arc::new(crate::EngineDialect)),
).unwrap();
let physical = io.block(|| physical.poll()).unwrap();
let mut loading = physical.begin_id_inspection().unwrap();
let inspected = io.block(|| loading.poll()).unwrap();
let mut confirming = inspected.begin_confirmation().unwrap();
let confirmed = io.block(|| confirming.poll()).unwrap();
assert!(confirmed.guard().requires_recovery());
assert!(confirmed.guard().published().is_err());
assert_eq!(confirmed.proof().source_identity(), None);
assert!(confirmed.proof().matches_lease(confirmed.guard()));
```

## Task 2: Durable same-record mirror and sealed recovered proof

**Blocked by:** Task 1's actual native RED.

**Files:** Runtime confirmation child; private manifest confirmation child; narrow type reexports.

**Interfaces:** Implements the state and sealed result above. Ordinary manifest publication remains unchanged.

- [x] Before suspending IDs, capture their identity, selected source database and maximum owner epoch. Check runtime ownership, exact record identity, normal empty target, physical native identity/size/page bound, physical persistent timestamp and empty WAL. Validate quiescence without reading mapped publication timestamps.
- [x] Capture actual physical floor from file bytes/page size (checked negative conversion), preserve the conservative inspected ID floor and source selection, and capture the active Weak lease. Suspend opaque IDs and transfer the same guard into a new ManifestLoad.
- [x] After fresh ManifestLoad, require `loaded.record() == inspected_record`; wait for its physical reads to retire. A changed record, runtime owner, target, floor or lease returns an error without writes. The manifest child must also require Arc identity with the retained actual manifest file and matching native file identity before constructing a publication; identical record bytes in a substitute MemoryFile do not qualify.
- [x] Construct private ManifestPublication directly for `1 - loaded.slot` with the same record and guard; do not call ordinary `publish`, fabricate a successor or change sequence.
- [x] Drive its write and FULL-sync to completion, then construct the second private publication for the other slot using the returned guard and identical record. Hold suspended IDs and pause throughout.
- [x] After the second FULL-sync callback, wait `guard.io_retired()` before producing the proof. Set exactly `source: None`, the inspected optional source database, actual target, actual physical floor and original Weak lease.
- [x] Return a sealed runtime result retaining all capabilities and maximum owner epoch. Assert mapped authority remains unrecovered, admission remains BUSY, and neither IDs nor main/WAL/log bytes changed. Decode both manifest slots and require the same selected record.

## Task 3: Mirror failure, cancellation and retirement boundaries

**Blocked by:** Task 2's first GREEN.

**Files:** Confirmation tests; reuse existing native delayed write/sync infrastructure.

**Interfaces:** No new product interface; tests observe retained capabilities and durable slot images.

- [x] Inject submission failure and callback error at each of two slot writes and two FULL-syncs. No result/proof may escape; dropping state leaves fresh recovery possible and mapped readiness absent. The previously selected normal slot remains valid until the inactive copy has FULL-synced.
- [x] Cancel before callback and after callback while the backend retains Completion, for both write and sync phases. Competing publication authority remains BUSY until actual backend retirement; a dropped stage never publishes readiness.
- [x] Hold the final completed FULL-sync's physical Completion: poll must yield retirement rather than return confirmation. Retire it and require the same continuous proof lease.
- [x] Load a substitute MemoryFile containing identical valid manifest bytes under the actual held guard and feed it with the genuine sealed evidence to the private mirror constructor. Reject the wrong manifest file before writes or proof construction.
- [x] Replace the selected manifest between inspection and the fresh confirmation load under the held test authority; reject it before writes. Test target-only finalized selector separately and require absent source database.
- [x] Deliberately remove the final retirement wait; the callback-held test must fail. Restore and rerun focused confirmation plus recovery/manifest/ID tests, strict library Clippy and formatting.
- [x] Commit only verified source, report exact native source/run/counts and remaining recovery gates. Parent reviews before integration; no public activation or readiness claim.

## Review questions resolved by this design

1. **Does a normal selected manifest alone become proof?** No. Only sealed actual physical and ID inspection feeds the constructor; the selected record is reloaded, matched and durably mirrored twice before proof creation.
2. **Does an old process's proof survive?** No. The genuine fixture discards it before recovery. The new proof carries a fresh continuous lease and no source prefix identity.
3. **Why write the inactive slot first?** It preserves the selected actual normal slot until an independent replacement is FULL-synced, then removes any stale intent in the former selected slot.
4. **Does confirmation mean ready?** No. IDs, maximum owner epoch, pause and unrecovered guard remain retained. Finalization, incarnation reconciliation, metadata reload and admission are later tasks.
5. **Can timestamps imply source generation?** No. Optional source database is copied from validated selector state; finalized target-only state has no predecessor claim.


Parent seam review (2026-11-08): approved the bounded witness slice with an additional mandatory binding between the sealed evidence and the actual retained manifest File Arc/native identity. A same-byte fake file under the same guard must not authorize recovery evidence. This is included in Task 2 and its explicit negative control in Task 3. No production or stub code preceded this approval.


## Verified witness milestone (2026-11-08)

The completed-normal confirmation slice is implemented on the isolated branch. It consumes actual sealed inspection, retains maximum owner epoch and opaque suspended IDs, freshly matches the selected record under the original lease, and checks the actual retained manifest File Arc/native identity. The private manifest child FULL-syncs the identical normal record into the inactive slot and then the formerly selected slot. Only physical retirement of both mirrors permits construction of a new source-identity-free CompletedCheckpointProof. The result retains the unrecovered guard, pause, physical facts, runtime, IDs and maximum epoch; it exposes no mutation/rejoin/readiness bridge.

The genuine missing-confirmation stub failed explicitly in [run 42481181555](https://github.com/HundredBillion/turso/actions/runs/42481181555), exact `a2638923b40721fcb97e251a406c8a1301c3ae29`, after entry3/physical1/ID1 prerequisites passed. First confirmation2 acceptance passed in [run 42482075956](https://github.com/HundredBillion/turso/actions/runs/42482075956), exact `80237ceebd7a9348eae3a4e5d7bf196b94e8bb90`. An earlier candidate run stopped at a Linux-only fixture error-type coercion; the production path was unchanged by its test adapter correction.

[Run 42482751913](https://github.com/HundredBillion/turso/actions/runs/42482751913) verified exact `03689b841e8092c65f64fce527b05d8c32e8e372`: seven confirmation cases and their restored run; eight submission/callback failures across two writes/two FULL-syncs with durable-image retry; eight cancellation-before/after-callback scenarios retaining physical authority; final sync retirement and inactive-first write order; changed-record rejection before mutation; finalized target selector with no inferred source; actual-file substitution rejection; completion3/recoveryIO2; IDs72 (3 ignored), manifests31 (4 ignored); 842 MVCC tests (45 ignored); strict library Clippy, format and source integrity. Removing actual-file binding or the final physical-retirement wait each failed its intended assertion, then restored verification passed.

The final test extension seeds 32 genuine logical-table ID reservations in a real transaction, rolls it back, and then uses the ordinary genuine checkpoint fixture. Before confirmation it requires a retained owner epoch Some and a permanent ID floor strictly below actual physical extent. [Run 42483728633](https://github.com/HundredBillion/turso/actions/runs/42483728633) verified exact `f2cdee212349c84f3a22b6a465163c58a583760f`: all eight confirmation cases and restored run, entry3/physical1/ID1, strict library Clippy, format and integrity passed. Dropping the captured epoch failed the nonempty-epoch assertion; substituting the permanent ID floor for physical extent failed the physical-evidence assertion. Production code is unchanged from the broader 036 run; the final delta strengthens test evidence only.

Parent independently reviewed the actual production runtime and proof-construction children CLEAR. Final branch/test review uses the execution ledger and review package. This milestone does not recover an intent/nonempty WAL, finalize recovered IDs, reconcile incarnation, bootstrap/reload a Database, reclaim history, publish replay readiness or activate public participants.
