# Recovered Checkpoint Log Reclamation Technical Spec

> **For agentic workers:** REQUIRED SUB-SKILL: Use dmi-superpowers:executing-plans to implement this TSP task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reclaim the obsolete logical log after genuine recovered completion and ID finalization, while retaining continuous authority, admission pause and exclusive permission to construct the runtime's first Store.

**Architecture:** Consume the sealed FinalizedRecoveredCheckpoint into a yielding reclamation owner. Reserve fresh runtime Store ownership before any logical mutation, freshly validate the actual completed manifest, truncate and FULL-sync the retained logical file, await physical retirement, and adopt the empty target append state. Return a sealed capability retaining all existing evidence and the fresh-Store reservation; no Database, Store, replay readiness or admission is published in this slice.

**Tech Stack:** Rust 1.88; existing ManifestLoad, GuardedFile checkpoint bindings, CompletedCheckpointProof, FinalizedIds and Storage append-state adoption; actual native file fixtures and publication I/O pins.

## Global Constraints

- Use existing isolated physical-connection worktree, branch `codex/shared-recovered-log-reclamation`, preserving parent `164f150e9486e57b2ee4b7d700531649e714d713`. Parent reviews this written seam before production.
- Existing approved cross-process checkpoint/recovery policy applies. No public PDO activation, new PR, intent/nonempty-WAL recovery, downgrade behavior, Database registration or authority.finish is added.
- Runtime replay None is necessary but does not prove absence of an existing Store. Runtime fresh-Store reservation must reject a previously attached Store before truncation or append-state mutation.
- The original source-identity-free completion proof, native main/manifest/log ownership, exact empty target, finalized ID state, same publication lease and admission pause remain authoritative. Never synthesize a proof from timestamps or target-minus-one.
- Drop/cancellation retains physical backend authority until retirement. A callback alone cannot authorize return, retry mutation or peer publication.
- Reuse the existing target directories and coordinate heavy builds. Linux workflow run steps explicitly use Bash with pipefail. Generic driver adapter is `io.block(|| Ok(driver.poll()?))`.

## Boundary and ownership decisions

This slice ends before private Database bootstrap. Its output is not a writable connection and cannot release admission. The later bootstrap must consume the sealed reservation and retain one actual Store across all I/O polls; it must not allocate a Store on each retry. Once a Store is attached, its runtime claim is permanent for that Runtime. If a cancelled construction drops that Store, a retry uses a genuinely reopened Runtime through fresh recovery, rather than constructing another Store under the old Runtime. Before any Store is attached, dropping a reservation releases only the uncommitted reservation; the publication I/O pins still prevent competing authority until actual retirement.

The reservation is runtime-private and cannot be asserted with a caller-provided boolean or fabricated enum. Ordinary `MvStore<C,A>::attach_shared_runtime` participates in the same reservation protocol. Its existing same-Store/same-Runtime idempotence remains; another Store cannot attach to the claimed Runtime, including after the first Store is dropped. Generic clock/allocator support remains unchanged.

## Files and private interfaces

- Create `core/mvcc/database/shared_store_claim.rs`, declared beneath shared_runtime. It owns the runtime-private reservation state and sealed linear reservation. Add one private state field to SharedMvccRuntime and initialize it in open_existing.
- Modify only attach_shared_runtime's actual attachment seam in `shared_runtime.rs`; keep existing storage/mode/transaction/identity validation and same-runtime idempotence before reserving.
- Create `core/mvcc/database/shared_checkpoint_recovery_reclaim.rs`, declared beneath recovered ID finalization so it can consume the parent's sealed fields without exposing them to callers.
- Add a sealed suspension bridge in `core/mvcc/shared_ids/checkpoint_finalize.rs`: `FinalizedIds::suspend(self) -> (SuspendedFinalizedIds, AuthorityGuard)` and `SuspendedFinalizedIds::rejoin(self, guard: AuthorityGuard) -> Result<FinalizedIds>`. The opaque token privately retains SuspendedIds and the genuine proof; rejoin requires the same original lease, exact published target, selected target IDs and no pending checkpoint. It yields only FinalizedIds, never generic LoadedIds. This permits fresh ManifestLoad to own the same guard without losing finalized evidence.
- Extend the existing native recovery finalization test harness for genuine fresh/ready fixtures and a separate logical-file transport decorator. Existing ID trace assertions remain unchanged.

```rust
impl FinalizedRecoveredCheckpoint {
    pub(in crate::mvcc) fn begin_log_reclamation(self)
        -> Result<RecoveryLogReclamation>;
}
impl RecoveryLogReclamation {
    pub(in crate::mvcc) fn poll(&mut self)
        -> IOResultOr<ReclaimedRecoveredCheckpoint>;
}
```

ReclaimedRecoveredCheckpoint has private runtime, PhysicalBaseParts, FinalizedIds, CheckpointPause, selected ManifestRecord and fresh Store reservation. It exposes only readonly guard/proof inspection. No raw parts, generic LoadedIds, runtime reservation constructor or writable connection escapes.

The runtime reservation implementation uses a private mutex with states Free, Reserved and Attached. A non-cloneable token owns the Runtime; only its private commit operation can transition Reserved to Attached. Drop resets Reserved to Free only for that token. A second reservation returns Busy; Attached rejects fresh construction. Ordinary attach reserves, sets the Store's existing OnceLock, and commits only after successful attachment. A failed set releases its uncommitted token. There is no reverse transition from Attached.

## Task 1: Genuine reclaim counterexample and first-Store ownership

**Blocked by:** Parent review of the written seam.

**Files:** shared_store_claim.rs, shared_runtime.rs, runtime child/declaration, existing recovery finalization tests.

**Interfaces:** Produce the runtime reservation and exact recovery entry/result above; the first recovery poll initially returns `InvalidArgument("recovered logical reclamation is not implemented")`.

- [x] Add native genuine fresh fixture regression: run actual confirmation and recovered IDs finalization; assert old logical bytes remain nonempty; call begin_log_reclamation and require eventual empty logical storage under the retained cap. The initial stub must fail at the exact missing reclamation diagnostic after finalization succeeds.
- [x] Add reservation tests: two Store attachments to one runtime reject the second; reattaching the same Store/runtime succeeds; an uncommitted reservation excludes attachment and becomes available after drop; a committed attachment remains claimed after its Store is dropped. Use actual runtime.storage for both Stores; a mismatched storage rejection must not consume the claim.
- [x] Run exact native stub RED and portable attachment tests. Record exact source, run, test count and expected failure; do not count compilation or fixture failures as the RED.
- [x] Implement the private reservation and ordinary attachment changes, then run ownership tests and existing normal open/begin coverage. Commit the independently reviewable ownership+stub milestone.

## Task 2: Proof-gated durable reclamation

**Blocked by:** Task 1.

**Files:** shared_checkpoint_recovery_reclaim.rs and existing recovery tests.

**Interfaces:** Consume FinalizedRecoveredCheckpoint and return the sealed ReclaimedRecoveredCheckpoint with the same proof, lease, finalized IDs, pause and uncommitted Store reservation.

- [x] Reserve the fresh Runtime before mutation. Require replay None, source_identity None, proof/guard same active lease, owned native authority, exact target map, finalized target IDs with no pending checkpoint, native snapshot quiescence, empty retained physical WAL, unchanged main file ID/extent/header bounds and physical timestamp/floor. Return Busy for unretired I/O; do not weaken ordinary publication checks.
- [x] Use the sealed finalized-ID suspension bridge to move the original guard through ManifestLoad of runtime.files.manifest. Wait for backend retirement, then require exact normal/empty/salt-zero target record and original proof lease. Rejoin the finalized IDs through the sealed proof/lease-gated bridge. Retain this selected record; do not mirror, advance sequence or manufacture another completion proof.
- [x] Bind the actual retained logical GuardedFile with bind_checkpoint. Drive these phases with one pending Completion and explicit callback-result checking before each transition:

```text
Validate -> FreshManifest -> ValidateAgain -> Truncate(0)
         -> FULLsync -> PhysicalRetirement -> AdoptEmpty -> SealedResult
```

- [x] Immediately before truncate, after final physical retirement, and before output, recheck proof/lease/native ownership/map/IDs/no snapshots/replay None and first-Store reservation ownership. Require actual logical size zero and concrete Storage file identity/no pending write before calling `adopt_shared_prefix(None, 0, 0, target.database.checkpoint_timestamp)`; afterwards verify offset0/CRC0/no header. Do not release authority or pause.
- [x] On error/drop, clear callback state/bindings and drop guard-bearing finalized evidence before releasing pause/reservation. Physical operation pins retain authority independently. No previous replay is restored. Retry performs real manifest/physical/ID recovery under a new proof; it never reuses an expired lease.
- [x] Run focused positive fresh prepared/target-only and already-attached rejection cases. Assert main/physical-WAL/manifest/IDs bytes are unchanged by reclaim, old logical bytes are removed, runtime replay remains None, admission and competing authority remain Busy, and no Store has been constructed. Commit after independent source review and GREEN.

## Task 3: Fault, cancellation and stale-evidence controls

**Blocked by:** Task 2.

**Files:** Existing recovery tests, TSP evidence; validation workflow remains on its separate branch.

**Interfaces:** Exercise the same genuine proof/IDs/runtime constructors and logical transport seam; add no test-only proof or mutable ownership escape.

- [ ] Inject submission errors and callback errors at actual truncate and FULLsync. Assert no sealed result, no replay readiness, no early peer publication, and fresh real recovery can retry both pre-truncate and already-truncated durable states.
- [ ] Hold truncate and FULLsync before callback and after callback-before-physical-retirement; drop the state at each boundary. Competing authority remains Busy until the actual backend retires, then fresh recovery can acquire and reconstruct a new proof. Live state cannot return output after callback alone.
- [ ] Reject wrong Runtime/native owner, foreign original lease/proof, nonempty physical WAL, changed actual main extent, changed target map or fresh normal record, and pending IDs before the first logical mutation. An already-attached runtime must reject even after runtime.replay is withdrawn.
- [ ] Run causal controls: bypass first-Store reservation to fail the existing-Store mutation guard; remove logical final-retirement wait to fail callback-before-retirement output guard; remove concrete append adoption to fail a seeded real previous append-state assertion. Restore exact source after each RED.
- [ ] With strict Bash pipefail, run restored focused tests, existing witness/finalization/native ownership tests, full MVCC suite, strict library Clippy, format and source-integrity checks. Inspect raw cargo summaries, not only workflow conclusion. Obtain independent whole-branch review, record exact SHA/run/counts, and retain the private branch for parent integration. Bootstrap/Ready remain separate work.
