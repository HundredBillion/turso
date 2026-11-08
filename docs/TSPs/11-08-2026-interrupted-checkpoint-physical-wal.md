# Interrupted checkpoint physical WAL recovery Technical Spec

> **For agentic workers:** REQUIRED SUB-SKILL: Use dmi-superpowers:executing-plans to implement this TSP task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restore an actual committed physical WAL into fully synced main storage after a killed shared checkpoint, while retaining the selected intent and continuous native exclusion.

**Architecture:** Consume the sealed mirrored intent from `shared_checkpoint_recovery_intent.rs`. Scan the retained WAL, inspect its committed image with an independent private reader, then use the existing pager FULL/TRUNCATE machinery under retained native physical exclusion. Return only inspected physical facts with the original intent/authority/pause; completion, ID finalization and admission remain separate.

**Tech Stack:** Rust1.88; existing native Linux64 OFD locks, SQLite WAL scanner, pager checkpoint state machine and backend completion ownership. No new dependencies or public API.

## Global Constraints

From `docs/PRDs/11-06-2026-independent-php-worker-concurrency.md`:

- “Start verification on supported 64-bit Linux and local filesystems. Do not advertise unverified Windows, IOCP or network-filesystem operation. Existing supported configurations remain covered by regression checks.”
- “Start with globally quiescent checkpoints. Pause new transaction admission while waiting within a bounded busy deadline for active transactions to finish. Preserve readers rather than silently aborting them.”
- “Keep the current MVCC/multiprocess safety guards until the new protocol is implemented and verified; unsupported platforms, custom storage and incompatible live protocol versions fail clearly.”
- “Recover abandoned append ownership before another writer acknowledges a commit. Define torn-tail handling and the relationship among durable bytes, published committed position and acknowledgements.”
- “The implementation PR remains draft until the full acceptance contract is demonstrated.”

This component does not change public admission. Do not edit the database marker/open-proof hunks in `core/database.rs` owned by the parent/performance work. Do not weaken normal `SharedRefresh`, completed checkpoint confirmation or recovered-ID checks. The existing intent remains an intent throughout this component. A timestamp within an interval is not physical consistency evidence.

---

## Audited source and actual invariants

- `checkpoint_state_machine.rs::maybe_stage_mvcc_metadata_write` places persistent timestamp metadata in the same physical pager transaction as materialized rows, indexes, schema and roots. Its committed image is the old source checkpoint or the acknowledged target, not an intermediate checkpoint timestamp.
- `sqlite3_ondisk.rs::BuildSharedWal` checks frame salts and rolling checksums, keeps the last valid commit boundary and drops later pending frames. Its initial `nbackfills` is zero. The recovery wrapper additionally requires magic `0x377f0682`/`0x377f0683`, format `3007000`, valid page size, actual frame extent and a positive committed boundary; the ordinary scanner does not independently reject every unsupported header field. It can silently return zero committed frames for a short/invalid header; that result alone does not prove main is consistent.
- `wal.rs::checkpoint_inner` syncs selected WAL frames before backfill. `pager.rs::checkpoint_inner` syncs main before truncating/syncing WAL. Reuse these state machines; do not write a second frame-copy loop.
- `ShmWalCoordination::seed_or_sync_authority` may adopt a mapped snapshot newer than a scan, including a positive tail when a scan sees no commits. It is unsuitable as the decision-maker for this exclusive recovery path.
- `MappedSharedWalCoordination::snapshot` and `with_snapshot_write` spin while `snapshot_seq` is odd. An interrupted mapped publisher can leave that value odd. Recovery must not enter either loop before it owns all native physical locks and repairs the derived mapped state.
- `try_acquire_writer` and `try_acquire_checkpoint` alone do not exclude readers. OFD locks owned by the same open description also do not identify sibling readers. Local counts must be checked under the same mutex used by reader registration before native reader-byte acquisition.
- `SharedPhysicalConnection` currently reads only retained main. It can be extended in its child file to attach a caller-supplied private scanned WAL after raw header initialization and before schema parsing; this does not require touching `core/database.rs`.
- Shared initial opens call `begin_read_pinned_open` and select a normal manifest before `_init`, WAL scan/build or schema reads. Connection admission uses `SharedOpenState`; physical oracles require refreshed identity/authority and pinned files. Checkpoint/recovery drivers retain authority. Ordinary opens reject the active mode marker. These are the actual admission boundaries, not a claim that every use of `WalFile` in the engine is MVCC guarded.

## Chosen ownership seam

Retain physical exclusion through actual backend retirement instead of relying only on the MVCC lease after physical locks are dropped.

Add a private storage child `core/storage/shared_wal_recovery.rs`, declared by `shared_wal_coordination.rs`, with a sealed `SharedWalRecoveryLocks` owner. It acquires the native CHECKPOINT byte, WRITER byte and every reader-slot byte while holding the mapping's local lock mutex. It rejects any local writer, checkpoint or reader count first. It holds an `Arc<MappedSharedWalCoordination>` and native lock offsets; it exposes no raw unlock method.

Add a local `recovery_held` flag and a poisoned state. The same-process writer/checkpoint/reader registration entry points reject acquisition while this owner is live or poisoned. Hold the local mutex across native-byte acquisition and publishing the local recovery flag, so same-description acquisition cannot slip between the checks. An acquisition failure releases only already acquired bytes in reverse order. An unlock failure poisons further acquisition rather than assuming ownership was released. Reader owner/frame values and snapshot timestamps are not liveness evidence. Do not call normal transient repair, which clears owner fields before this proof is established.

Extend `GuardedFile` with a recovery-only binding that stores an `Arc<SharedWalRecoveryLocks>` alongside the existing weak publication lease. The completion owner obtained by `pin` retains both the existing MVCC `IoPin` and this typed physical owner. Normal bind/read/checkpoint behavior is unchanged. Dropping a state or receiving a successful callback cannot release physical native locks while the backend retains a completion.

```rust
// Private APIs; visibility is crate-internal only where storage needs the sealed type.
impl GuardedFile {
    pub(super) fn bind_physical_recovery(
        self: &Arc<Self>,
        guard: &AuthorityGuard,
        physical: Arc<SharedWalRecoveryLocks>,
    ) -> Result<FileBinding>;
}

impl SharedWalRecoveryLocks {
    pub(crate) fn acquire(mapping: Arc<MappedSharedWalCoordination>) -> Result<Arc<Self>>;
    // No arbitrary caller-provided snapshot is accepted by these methods.
    pub(crate) fn install_empty(&self, evidence: &RecoveredPhysicalWalEvidence) -> Result<()>;
    pub(crate) fn begin_sync(
        self: &Arc<Self>,
        evidence: RecoveryWalSyncEvidence,
    ) -> Result<Completion>;
}
```

The runtime constructs the physical lock owner only from its actual retained coordination Arc, and validates that association before binding either file. The evidence types have private constructors in the runtime recovery child. They bind the actual runtime, retained main/WAL/manifest/native coordination identities, original full intent, original authority lease, exact scanner output Arc and observed file lengths. Storage accepts only the final actual synced-main/empty-WAL evidence for derived reconciliation. `RecoveryWalSyncEvidence` is an opaque crate-private owner constructed only by the runtime after those associations are checked; its fields retain physical exclusion before the MVCC pin in drop order. Storage captures the whole owner in the real backend sync completion. Neither `AuthorityGuard` nor `IoPin` needs broader visibility.

**Approved refinement:** No initial mapped reseed is performed. The private effective-image reader and copy pager use only an in-process WAL built from the actual committed disk scan. They must never call mapped snapshot, backfill, checksum or frame-index APIs, including through close, metadata or debug paths that could spin on an odd seqlock. Native physical ownership prevents other legitimate participants from consulting/replacing that state during copying.

After main is actually synced, WAL is actually empty and fresh main-only inspection matches the prevalidated committed image, `install_empty` clears obsolete derived index/backfill state and repairs an interrupted mapped seqlock under all native locks. The exact zero-frame header comes from the actual locally checkpointed WAL, not a guessed timestamp or invented normal record. Preserve/increment visibility counters only as invalidation metadata, never as durable-file proof. The final actual mapping sync and backend retirement remain required before returning; later complete recovery/new-peer acceptance must prove usability. This refinement removes an unnecessary intermediate derived-state write and does not relax future admission.

## State and output

Create `core/mvcc/database/shared_checkpoint_recovery_wal.rs` under `shared_runtime`:

```rust
impl RecoveredCheckpointIntent {
    pub(in crate::mvcc) fn begin_physical_wal_recovery(
        self,
        options: OpenOptions,
    ) -> Result<CheckpointPhysicalWalRecovery>;
}

impl CheckpointPhysicalWalRecovery {
    pub(in crate::mvcc) fn poll(&mut self) -> IOResultOr<RecoveredIntentPhysicalBase>;
}

// Private fields; no into_guard, generic rejoin, completed proof or Ready method.
struct RecoveredIntentPhysicalBase {
    intent: RecoveredCheckpointIntent,
    parts: PhysicalBaseParts,
}
```

The original intent owner stays nested throughout; its admission pause is released last. The driver progresses through native exclusion, guarded bindings, committed scan, effective-image inspection, fresh exact manifest reselection, existing physical checkpoint, fresh main-only inspection, final mapping reconciliation/sync and retirement. Store/replay/IDs/global published prefix remain untouched.

For effective-image inspection, build an **in-process** `WalFile` from the exact committed scan and attach it to a private read-only `SharedPhysicalConnection`. The actual native physical exclusion is owned externally, so the in-process checkpoint avoids trying to acquire the same native locks twice. Never construct ordinary `ShmWalCoordination` for this recovery operation. Clear the private page cache/schema header state after attaching WAL and before parsing. Reject custom storage, codecs and encryption as the existing physical reader does.

Require effective persistent timestamp equal to `intent.source_checkpoint_timestamp` or `record.prefix().committed_timestamp`, with actual marked main/WAL header page size/reserved bytes, bounded extent, root layout and schema validation. Reject any intermediate timestamp even if ordinary physical inspection's broader range check would accept it. Require the original actual manifest record after reselection and no pending backend I/O before the first mutation.

Fresh intent reselection must not consume or expose the original mirrored owner. Add a recovery-only `CheckpointIntentInspection` child behind `MirroredCheckpointIntent::inspect_exact_for_physical_recovery(&RecoveryWalEvidence)`. It reuses `ManifestLoad::start(None)` internally on the actual retained manifest's guarded read binding, which retains the original MVCC lease and physical owner. Return only `Done(())` after full record/file/native identity/lease checks and backend retirement; never return the guardless `LoadedManifest`. Capture and revalidate the exact owner across resumes, reject reuse after completion/error and keep normal manifest constructors owned-guard-only. The parent approved this bounded parser reuse; it introduces neither a second parser nor a generic guardless inspection/rejoin API.

Use a fresh private pager with the actual guarded main storage, scanned in-process WAL and independent cache/shared-pager state. Set page size, reserved bytes and IO context from the inspected actual image. Drive:

```rust
pager.checkpoint(
    CheckpointMode::Truncate { upper_bound_inclusive: None },
    SyncMode::Full,
    true,
)
```

After successful checkpoint completion, wait for actual backend retirement, verify actual WAL length zero and inspect main afresh without WAL. Require exact equality of its persistent timestamp/layout/schema identity to the prevalidated effective committed image. Only then publish/sync the derived empty physical coordination and return the unadmitted output. Neither a successful checkpoint callback nor an empty WAL alone is the output boundary.

## Task 1: Native exclusion and completion ownership

**Blocked by:** Parent review of this TSP.

**Files:** New storage recovery child; narrow mapping declaration/local registration gates in `shared_wal_coordination.rs`; recovery-only typed binding in `guarded_file.rs`; native storage ownership tests.

**Consumes:** Actual retained mapping and authority guard. **Produces:** `Arc<SharedWalRecoveryLocks>` retained by physical file completions.

- [x] Add native tests with same-process writer/checkpoint/WAL-reader/main-reader owners and independent child owners. Recovery acquisition is bounded `Busy`, preserves each old owner, and succeeds only after real release/death.
- [x] Add explicit native backend read/write/sync/truncate retention tests: drop state before callback and after callback but before backend Arc retirement; both new physical reader registration and actual shared open remain blocked until backend release.
- [x] Add the missing implementation and observe the ownership tests fail at explicit missing behavior, then implement the typed owner/binding and run them green.
- [x] Inject acquisition/unlock failures at every acquired prefix. Verify exact cleanup and poisoned reuse on unlock failure. Remove the same-process count gate and completion-retention capture separately; require intended failures, restore and rerun.
- [x] Commit this independently tested ownership seam before physical file mutation is implemented.

## Task 2: Genuine committed-WAL recovery

**Blocked by:** Task1.

**Files:** New runtime recovery child/tests, narrow type reexports; retained-WAL option in `core/database/shared_physical.rs`; existing crash fixture extension. Do not edit `core/database.rs`.

**Consumes:** Whole `RecoveredCheckpointIntent`, `SharedWalRecoveryLocks` and options. **Produces:** `RecoveredIntentPhysicalBase` only after actual main durability/empty WAL/fresh inspection/retirement.

- [ ] Extend the actual SIGKILL fixture to preserve three physical states: before first main write, after exactly one successful changed main-page write while later changed pages are held, and after successful main FULL sync but before actual WAL truncate. Use multiple changed pages and assert old/partial/new main byte relationships; run both one/two acknowledged commits.
- [ ] Add the explicit missing `begin_physical_wal_recovery` regression. Require all six genuine crash prerequisites pass first and the recovery test fail for the missing behavior rather than setup/timeout.
- [ ] Implement scan/effective-reader validation, exact record reselection and typed evidence without modifying main/WAL; test invalid salt/checksum/commit boundary, replaced files/lease/runtime, intermediate timestamp and foreign/live readers. Valid committed prefixes with torn uncommitted tails retain only the last committed image.
- [ ] Implement private in-process WAL copying from the exact scan under the retained owner, existing FULL/TRUNCATE checkpoint, fresh main inspection, final zero mapping/sync and backend retirement. Assert exact acknowledged rows/schema/indexes, unchanged intent/log/IDs, no replay/global recovery/Ready and owner-held admission exclusion.
- [ ] Test stale mapped tail/checksum/index/backfill and an interrupted odd mapped seqlock. Recovery must use the actual scan without consulting the mapping and finish within a bounded deadline. Remove the final derived mapping repair as a causal control; require the stale/odd-state postcondition regression fail without an unbounded snapshot call.
- [ ] Inject main write/sync and WAL truncate/sync failures; cancel before/after callbacks and before backend retirement. Reopen with fresh native selection and retry without losing the acknowledged logical prefix. A crash after durable main but before WAL truncate must be idempotent.
- [ ] Commit the verified recovery driver. Keep ordinary replay/completed confirmation guards unchanged.

## Task 3: Verification and explicit remaining crash cases

**Blocked by:** Task2.

- [ ] Run exact-source native ownership/recovery tests, three old/partial/new physical states with both slot parities, causal removals/restored tests, existing intent9/recovery24 and manifest/authority suites. Use Rust1.88/jobs2/debug0, Bash and pipefail; retain raw final parent summaries and source pins.
- [ ] Run full core and strict library Clippy/fmt/source integrity; Windows check proves new native code remains correctly gated. Obtain independent source review before integration.
- [ ] Record and hand off the unadmitted physical base. Actual acknowledged-prefix replay/materialization, real ID preparation/completed records/finalization and Ready remain later typed stages.
- [ ] Track a required subsequent recovery slice for genuine precommit/noncommitted/header-only/torn-initial-WAL crashes. The first committed-WAL slice fails closed for nonempty WAL with no valid committed frames; this is **not** final PR8 acceptance. Before permitting discard, prove actual source main and ordering from genuine coordinator crash fixtures. Never treat zero scanned frames alone as proof or leave a valid crash permanently unsupported at release.

## Stress-test answers for parent review

1. **Why no ordinary shared WAL constructor?** Its newer-mapped-state heuristic can override actual recovered disk bytes; this path has exclusive evidence and must rebuild derived state from disk.
2. **Why native reader locks in addition to writer/checkpoint locks?** Main/WAL readers can remain live without either write lock, and same-description OFD probing alone misses local readers.
3. **Why retain both native owners through callbacks?** Backend completion references can outlive the waiter and state. Both publication admission and physical participant exclusion remain closed until the backend retires.
4. **Why inspect before and after checkpoint?** The first read proves which committed physical image is being copied; the second proves actual main now contains that image after sync/truncate. Neither timestamp interval nor mapped backfill is a substitute.
5. **Can the output close issue7 or make PR8 ready?** No. It is an internal verified stage. All required crash classes, typed logical/ID completion and public PHP acceptance still apply.

The parent is the independent design hardening reviewer. Existing user protocol approvals authorize routine private recovery implementation after that review; no new user policy choice is introduced.


## Execution refinements accepted by the parent

- The recovery owner type and I/O transport compile on every `host_shared_wal` platform because `GuardedFile` also compiles there. Native acquisition rejects unsupported platforms before any lock; the runtime recovery path remains Linux64 only.
- Independent review caught a normal writer/checkpoint ownership race in liveness probing. Recheck current local ownership for the requested byte under the same mutex as the OFD probe; a previously sampled boolean is insufficient. The deterministic regression samples false, acquires the actual local owner, then invokes the probe and checks independent-fd exclusion.
- Distinct physical schema parses are compared by actual header/catalog root rows/custom-type definitions, not schema Arc identity. The existing `PhysicalBaseParts` already retains immutable catalog records suitable for a semantic comparison.
- Keep a fresh runtime unclaimed while opening only private nonshared physical readers. If a later materialization phase attaches a real shared Store, retain/reuse it under the one-Store-per-Runtime reservation; never reset that claim or construct a second Store for the handoff.


## Task1 verification record

Engine `1b4bf7bdb5e9539c8dec5fc255f2b88e0bb7c4eb` passed [final native and Windows checks](https://github.com/HundredBillion/turso/actions/runs/42507078556). Linux restored native8/recovery24 and the full-core final parent summary2900passed/152ignored all report zero failures. Strict library Clippy, formatting and source integrity passed. Local-reader count and physical completion-owner removals each failed the intended single test before exact restoration. Windows old-Unix-cfg restoration produced E0432, then the actual fixed source library check passed. Native acquisition remains restricted to supported Linux64 OFD storage.

The ownership matrix includes263 native lock failure positions, ten actual native I/O callback/cancellation combinations, real same-process/child participants with SIGKILL/retry, a stale ownership-sample regression, and actual same-inode/foreign-Arc/foreign-authority binding negatives. Earlier genuine missing-behavior and stale-probe/retention RED runs are preserved separately. Raw source pins, environment, control logs and parent summaries are retained in `.local/issue-resolution/checkpoint-physical-wal/` (final Linux artifact13627792711).

The parent reviewed the owner and lifetime implementation, identified the stale normal-count probe and Windows cfg mismatch, and those corrections have causal tests. The parent independently reviewed the matching final portability fix, read the raw final summaries and accepted this Task1 milestone. Whole-TSP final review remains a Task3 gate. No physical-WAL copy, mapping repair or Ready behavior is claimed by this Task1 record.

## Physical recovery verification milestone

Engine `041025a05417521b847948c8eeef74591449ceb3` passed [final Linux and Windows validation](https://github.com/HundredBillion/turso/actions/runs/42515975381). The native operation matrix covers eight genuine crash boundaries and both intent-slot parities: old main, partially copied main, fully synced main before WAL truncation, empty WAL, torn initial header, header without frames, torn first frame and complete uncommitted frames. It exercised332 I/O errors/short writes and628 cancellation/retirement positions, with fresh recovery and actual class-appropriate SQL rows after each.

The precommit fixtures prove exact original main bytes before recovery and preserve the acknowledged logical prefix. Actual backend traces require main fsync before WAL truncation, including zero valid frames and retries after truncation. A nonempty no-commit WAL paired with an ACK main is rejected unchanged. Removing that SOURCE guard fails its intended test. The committed-image oracle reads acknowledged user data; omitting the actual required data page only after the genuine crash reaches an unadmitted output and fails that row assertion. Removing final derived-state repair makes the bounded fresh-process stale/odd-mapping test fail. Restored committed5/precommit4/owner8/intent9 and I/O2 cohorts pass.

The final core parent summary is2900passed,164ignored,0failed. Rust1.88 strict library Clippy, formatting, source integrity and Windows library checks passed. Raw initial RED, invalid-control distinction, repaired causal controls, final source pins and full logs are retained under `.local/issue-resolution/checkpoint-physical-wal/`. The parent independently reviewed the committed driver and conservative precommit gate; final integration review covers the combined source.

This milestone returns only an unadmitted physical base while the real INTENT, lease and pause remain owned. It does not publish a completed record, finalize IDs, reclaim the acknowledged logical log or expose Ready. The approved successor [completion plan](11-08-2026-interrupted-checkpoint-completion.md) must consume this result, retain a SOURCE replay Store through real completion, and return a usable recovered Database before issue7/PR8 is complete.
