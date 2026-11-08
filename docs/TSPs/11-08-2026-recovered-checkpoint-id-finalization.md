# Recovered Checkpoint ID Finalization Technical Spec

> **For agentic workers:** REQUIRED SUB-SKILL: Use dmi-superpowers:executing-plans to implement this TSP task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Use a genuinely recovered completion proof to restore the exact target oracle, preserve retained owner epochs, and finalize recovered IDs without admitting a Database.

**Architecture:** Consume the sealed RecoveredCheckpointConfirmation under its continuous guard/pause and withdraw local replay before mutations. A proof-gated authority method restores only the exact empty target; an opaque-ID method internally rejoins its saved state, durably mirrors the current ID witness for a fresh incarnation, reconciles epoch before forgetting old references, and delegates to the existing ID checkpoint finalizer. The result retains runtime, physical facts, FinalizedIds and pause; it never finishes authority or publishes Database readiness.

**Tech Stack:** Rust 1.88; CompletedCheckpointProof; AuthorityGuard; existing IdWitnessMirror and IdCheckpointFinalize; actual retained native files and publication I/O pins.

## Global Constraints

- Isolated branch `codex/shared-recovered-id-finalization` in the existing physical-connection worktree, starting at `6680b79c6acde11c952ff1888d176fc7e53e785f`. Preserve the completed-confirmation branch and its verified product/test source `f2cdee212349c84f3a22b6a465163c58a583760f`.
- Parent-approved existing #7 protocol covers this bounded slice; parent reviews this written seam before production. No new user design permission is needed.
- Accept only a sealed recovered source-identity-free completion proof for the exact native owner/lease and actual completed normal empty target. Never derive source generation from target minus one or timestamp.
- A recovered guard already publishing another prefix must reject rather than rewind. Restoration of an unrecovered map uses the existing monotonic next-timestamp rule.
- Capture/validate the maximum owner epoch before any selected reference or old bank is erased. Fresh-incarnation reconciliation follows FULL-synced/retired current ID witness mirroring and precedes existing finalizer mutations.
- Keep opaque SuspendedRecoveryIds non-rejoinable to callers; no generic LoadedIds escape is added. Its new consuming operation requires the real proof.
- Incarnation readiness is coordination metadata. It does not publish Database/replay readiness: local runtime replay stays None, pause and authority stay held.
- No Database bootstrap, physical reconciliation, logical-log reclaim, authority.finish, admission release, public opt-in or new PR is added.
- Existing canonical loader adapter in new helpers: `io.block(|| Ok(driver.poll()?))` for bare or boxed poll errors.
- Reuse existing targets and coordinate build slots; do not create a large fresh target directory or merge validation workflows.

## Files and private interfaces

- Add runtime child `core/mvcc/database/shared_checkpoint_recovery_finalize.rs` beneath the confirmation child. Descendant access consumes its parent's sealed fields without exposing raw constructors.
- Add a matching host_shared_wal+unix proof-gated method in `core/mvcc/shared_authority.rs`:

```rust
pub(super) fn recover_completed_checkpoint(
    &mut self,
    proof: &CompletedCheckpointProof,
) -> Result<()>;
```

It validates source_identity None, original active lease, exact empty target/base timestamp, no native snapshot locks and retired I/O. If unrecovered it calls existing recover(target.prefix); otherwise it requires published()==target.prefix and does nothing. It does not reconcile incarnation or finish authority.

- Extend `core/mvcc/shared_ids/checkpoint_recovery.rs` only with a proof-gated consuming operation and yielding state:

```rust
impl SuspendedRecoveryIds {
    pub(in crate::mvcc) fn begin_finalize(
        self,
        guard: AuthorityGuard,
        proof: CompletedCheckpointProof,
        captured_maximum: Option<u64>,
    ) -> Result<RecoveredIdFinalization>;
}
impl RecoveredIdFinalization {
    pub(in crate::mvcc) fn poll(&mut self)
        -> Result<IOResult<FinalizedIds>>;
}
```

It requires proof source None, target==saved identity, source_database==saved selected source, same original lease and exact target map. The internally rejoined LoadedIds maximum must equal captured_maximum before any selector writes. Already-ready authority delegates directly to existing finalize_recovered_checkpoint. Fresh authority starts existing mirror_current_witness, retaining both active/prepared references.

- Add only a narrow pub(super), proof-gated consuming MirroredIds helper in `shared_ids/witness_mirror.rs`. It validates exact proof/identity/source/lease and maximum, reconciles the fresh incarnation using actual maximum.unwrap_or(0), then returns existing IdCheckpointFinalize. It never returns LoadedIds.
- Add cfg(test) read-only coordination observation on SharedMvccAuthority for epoch/ready/next timestamp. This is required for assertions inside the actual old-bank truncate hook and supplies no mutation or proof capability.

Runtime entry and sealed result:

```rust
impl RecoveredCheckpointConfirmation {
    pub(in crate::mvcc) fn begin_id_finalization(self)
        -> Result<RecoveryIdFinalizationState>;
}
impl RecoveryIdFinalizationState {
    pub(in crate::mvcc) fn poll(&mut self)
        -> IOResultOr<FinalizedRecoveredCheckpoint>;
}
```

FinalizedRecoveredCheckpoint has private runtime, PhysicalBaseParts, FinalizedIds and CheckpointPause fields. Tests receive read-only proof/guard access; no consuming readiness bridge is provided.

## Task 1: Genuine fresh-incarnation counterexample and missing finalization

**Blocked by:** Parent review of this seam.

**Files:** Runtime child/declaration and focused native tests.

**Interfaces:** Introduce the exact runtime entry/result above with explicit error `recovered checkpoint ID finalization is not implemented`; no recovered-map mutation or ID write is implemented in the stub.

- [ ] Capture the actual initialized coordination-file image before seeding real ID-owner history. Seed logical-table reservations through existing shared_id_nonblock in a real transaction and roll it back, as in the verified nonempty-epoch witness test.
- [ ] Produce a genuine completed checkpoint and save only expected target/maximum plus path/config/directory. Drop every old Database, Store, Runtime, guard, proof, ID object, driver, file and I/O holder before replacing coordination bytes.
- [ ] Restore the actual earlier coordination image without replacing its inode/static identity/configuration, FULL-sync the restore, and leave durable main/manifest/IDs unchanged. This models volatile coordination loss; it does not synthesize a proof or source generation.
- [ ] Strictly open a new native main handle and SharedMvccRuntime::open_existing from path/config; do not keep an old raw main Arc. Require fresh incarnation_ready false and observed epoch<=retained genuine old maximum.
- [ ] Run actual fresh selection, physical inspection, ID inspection and completed-normal confirmation to obtain a new lease-bound sourceNone proof. Require maximum Some and capture it before passing the sealed result to begin_id_finalization.
- [ ] Drive the stub and verify exact missing error (zero passed/one failed) on a full-SHA-pinned native workflow after completed-confirmation prerequisites pass. Compile failure/timeout is not RED.

## Task 2: Proof-gated exact target restoration and opaque ID finalization

**Blocked by:** Task1 actual native RED.

**Files:** Runtime child, authority helper, opaque recovery IDs and witness-mirror helper.

**Interfaces:** Implements the entry, opaque yielding operation and sealed result above; reuses existing checkpoint finalizer unchanged.

- [ ] Validate the consumed runtime owns guard/proof, actual native main identity/extent and empty WAL still match physical facts, target/base timestamp/floor are exact, lease is active, native snapshots absent and all physical I/O retired.
- [ ] Withdraw runtime.replay to None before mapped/ID mutations. No later error restores the old replay context.
- [ ] Restore only the proof's completed target via recover_completed_checkpoint. Require an already recovered map to equal target exactly; preserve next timestamp monotonically and never accept a live sourceSome proof or foreign lease.
- [ ] Inside the opaque ID module, validate saved identity/source/lease and captured maximum, internally rejoin the existing suspended payload, and keep LoadedIds private. Ready incarnations use existing finalize_recovered_checkpoint.
- [ ] For a fresh incarnation, mirror the actual current ID witness through both FULL-syncs and backend retirement before reconciling captured maximum. The proof-gated MirroredIds helper verifies the same actual maximum, calls reconcile_incarnation, and then creates existing IdCheckpointFinalize.
- [ ] Existing finalization mirrors the target-only selector, FULL-syncs it, truncates/FULL-syncs the old bank and waits physical retirement. Observe epoch>genuine old maximum and ready before old-bank truncate; retain local replay None throughout.
- [ ] Return sealed runtime/physical facts/FinalizedIds/pause while authority and admission pause remain held. No participant transaction, schema bootstrap, logical truncation or finish call is added.

## Task 3: Failure, capability and epoch-before-erasure controls

**Blocked by:** Task2 initial GREEN.

**Files:** Runtime native tests and reused ID finalizer controls; no unrelated API changes.

- [ ] Test prepared target and already finalized target selector, with already-ready and genuinely fresh-runtime authority. Retained global logical floor remains conservative; physical proof floor still comes from actual extent.
- [ ] Reject a genuine sourceSome live proof, a different recovered proof/lease and foreign opaque IDs. Verify no ID writes escape and runtime stays unready after withdrawal.
- [ ] Force an already recovered different prefix under test authority; typed recovery must reject without changing that prefix or decreasing timestamp clock.
- [ ] Inject actual File submission/callback failures during current-witness mirror and existing selector/bank finalization. After errors or drop, no local replay readiness/finish occurs; physical completions continue excluding competing authority until retirement.
- [ ] Cancel before/after callback while backend retains physical Completion for selector writes/syncs and old-bank truncate/sync; no sealed final result escapes before final physical retirement.
- [ ] Counterfactually drop captured maximum: the genuine Some-epoch case must fail its maximum consistency/epoch assertion before old state disappears. Counterfactually skip reconcile_incarnation: the actual old-bank truncate hook must fail before performing the erase.
- [ ] Restore source, rerun focused native cases plus witness/ID protocol/authority tests, appropriate broader MVCC coverage, strict Clippy, format and source integrity. Record exact source/run/counts.
- [ ] Obtain independent whole-branch review and retain the private branch for parent integration. Database readiness and public activation remain separate work.

## Approved review decisions

Parent approved current-witness mirroring/retirement before fresh epoch reconciliation, a narrow proof-gated MirroredIds helper without LoadedIds escape, and the earlier-image crash fixture. The restore must occur only after all old authority/I/O owners are gone and must preserve inode/static configuration. Existing approvals settle the protocol; this plan does not introduce a user approval gate.
