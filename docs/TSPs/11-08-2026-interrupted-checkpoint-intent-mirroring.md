# Interrupted Checkpoint Intent Mirroring Technical Spec

> **For agentic workers:** REQUIRED SUB-SKILL: Use dmi-superpowers:executing-plans to implement this TSP task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reconfirm the actual selected checkpoint intent after a process dies, retaining paused admission and unrecovered authority for subsequent physical recovery.

**Architecture:** Acquire the existing recovery selection, reselect the identical intent from the runtime's retained native manifest under the same guard, then rewrite both slots through the existing write/FULL-sync publication driver. Return a sealed owner containing that actual mirrored intent, the original runtime and pause; return no completed proof, recovered prefix or ready connection.

**Tech Stack:** Rust 1.88; existing ManifestLoad/ManifestPublication/CheckpointIntentPublication; retained Linux native files; OFD snapshot/admission coordination; existing completion ownership pins.

## Global Constraints

- Source base `07cff3dc54f351a60a57262b4144fbdad66e361e` combines the independently verified idle-generation reload and recovered completed-checkpoint confirmation. Branch `codex/shared-interrupted-checkpoint-recovery` is isolated in the existing pager-generation worktree; the completed milestone `codex/shared-idle-generation-reload` remains at `87f1362af`.
- The branch's additional `30ab781430c3d9b59b39ce42b287b0506e8143e4` changes only the installation test's evolved expectation; root owns its combined native verification. No production implementation in this TSP starts before parent review.
- The normal completed-target finalization work owned by the checkpoint agent remains separate: do not modify `shared_checkpoint_recovery_finalize.rs`, `shared_ids/checkpoint_recovery.rs`, the ID witness mirror or typed incarnation reconciliation in this first slice.
- Existing user approvals cover quiescent checkpoints and crash recovery. This slice adds no public API, URI behavior, PDO activation or disk format.
- Accept only an actual checkpoint intent. Reject a downgrade or normal record; preserve original sequence, log generation, salt, acknowledged offset/CRC/timestamp, source checkpoint timestamp and encoded target generation.
- The actual retained manifest Arc and native identity, runtime authority, main identity and physical-WAL identity must match. A same-byte memory manifest or a replacement lease is insufficient.
- Check native snapshot-slot quiescence without trusting mapped timestamps, clearing registrations or evicting readers. Retain the original pause and authority across all phases and physical completion retirement.
- This operation may rewrite only the two manifest slots. Main, physical WAL, logical log and ID files remain byte-identical. Nonempty physical WAL is permitted as input because no physical inspection or recovery is attempted here.
- Do not call guard.recover/finish, reconcile incarnation, inspect/finalize/prepare IDs, reclaim logical history, install a Store, create a replay context or publish readiness. Preserve mapped recovery state.
- Ordinary SharedRefresh, normal materializer authorization and completed-confirmation guards stay unchanged. Never fabricate a normal record to feed them.
- Use pinned Rust 1.88, two jobs, debug disabled and no incremental compilation. Reuse an explicitly reserved target or hosted validation; create no new large target directory.

## Source audit: the interrupted states

`CheckpointStateMachine::maybe_stage_mvcc_metadata_write` stages `persistent_tx_ts_max` in the same pager transaction as all selected rows, indexes, schema/root changes and sequence compaction. `CommitPagerTxn` commits that transaction before `CheckpointWal` begins. WAL recovery must therefore establish a committed physical image before the metadata timestamp is interpreted; an arbitrary timestamp between source and acknowledged target is not completion evidence.

Here FULL materialization means engine `SyncMode::Full`; on Linux, `Pager::get_sync_type` returns `FileSyncType::Fsync`, and both file-sync variants invoke regular fsync. Manifest publication explicitly uses `FileSyncType::FullFsync` at both barriers.

`WalFile::checkpoint_inner` fixes the committed WAL frame range under checkpoint/write locks and FULL-syncs it before any backfill. `CheckpointStateMachine::SyncDbFile` syncs main before WAL truncation. Shared materialization skips logical-log truncation; acknowledged source history remains until the later completed/finalized/install protocol authorizes reclamation.

A genuine crash can leave:

| Crash boundary | Main/WAL facts | Manifest/IDs |
| --- | --- | --- |
| Second intent mirror not submitted | Old main, empty WAL | One actual intent plus previous normal slot; source active IDs |
| Pager WAL commit durable, first main backfill not submitted | Old main, committed nonempty WAL | Both intent slots; source active IDs |
| Mid-backfill | Main may mix old/new pages; committed WAL remains durable | Both intent slots; source active IDs |
| Main FULL-sync before WAL truncation | New main, nonempty WAL | Both intent slots; source active IDs |
| Materialization retired before ID preparation | New main, empty WAL | Both intent slots; source active IDs |
| Prepared IDs before completed publication | New main, empty WAL | Intent plus actual prepared target IDs |
| First completed mirror durable | New main, empty WAL | Actual selected completed record; existing completed-confirmation path applies |

The existing `shared_prepared_checkpoint_fixture` always drives materialization to completion and prepares IDs before returning. It covers the sixth row, not the interrupted physical-WAL states. New crash fixtures must drive the actual coordinator and kill its process at real submitted-operation boundaries; do not obtain an existing completed fixture and relabel it as interrupted.

### Physical recovery is a separate audited dependency

`BuildSharedWal::begin/poll` already scans retained WAL bytes with header, salt, checksum and commit-frame validation, exposes only frames through the last valid commit, and ignores an uncommitted/torn tail. I/O errors remain errors through Completion's original-error result. Its successful result alone does not prove main was backfilled or durably synced.

`SharedPhysicalConnection` currently opens main-only retained read-only storage, and `CheckpointRecoverySelection::begin_physical_read` explicitly rejects nonempty physical WAL. Keep that guard.

`WalFile::new_with_shared_coordination` invokes `ShmWalCoordination::seed_or_sync_authority`; ordinary reconciliation can prefer persisted coordination state over a disk scan. A future recovery factory must establish the committed disk image and deliberately reconcile physical coordination under appropriate native exclusion. Attaching the ordinary constructor and assuming the scan replaces mapped state is insufficient, especially while idle processes retain handles.

`SharedRefresh::refresh` rejects the actual intent, and existing materializer authorization requires an already validated ready source replay identity. An eventual intent-replay/materializer slice needs distinct typed authorization for the original acknowledged prefix; it must neither manufacture a normal ManifestRecord nor weaken those normal guards.

The existing completed confirmation at `6680b79c6` accepts only a normal empty-prefix record, matching actual physical timestamp/floor and selected ID evidence. Keep it strict. Active-source-only IDs under an unfinished intent require real preparation; they cannot be passed to its completed-target inspector or guessed from `target_generation - 1`.

## First-slice files and interfaces

- Add `core/mvcc/database/shared_checkpoint_recovery_intent.rs`, a child of `shared_runtime`: sealed runtime evidence, fresh selection and owner retention. It may consume `CheckpointRecoverySelection` but cannot construct completed proofs.
- Modify `shared_runtime.rs` and `database/mod.rs` only for the Unix-gated child and narrow `RecoveryIntentEvidence` reexport used by the manifest module.
- Add one Unix-gated method inside `core/mvcc/commit_manifest/checkpoint_publication.rs`. Reuse `CheckpointIntentPublication` and its existing two-slot write/FULL-sync/retirement loop; do not add another generic publisher or another mirror state machine.
- Add `core/mvcc/database/shared_checkpoint_intent_fixture.rs` for test-only process/crash I/O controls, and `shared_checkpoint_recovery_intent_tests.rs` for functional, provenance, failure and retirement tests. Declare them only under Linux64 tests, excluding shuttle.

Runtime entry and result:

```rust
impl CheckpointRecoverySelection {
    pub(in crate::mvcc) fn begin_intent_mirror(self)
        -> Result<CheckpointIntentRecovery>;
}
impl CheckpointIntentRecovery {
    pub(in crate::mvcc) fn poll(&mut self)
        -> IOResultOr<RecoveredCheckpointIntent>;
}
```

`RecoveredCheckpointIntent` has private fields `runtime: Arc<SharedMvccRuntime>`, `intent: MirroredCheckpointIntent` and `pause: CheckpointPause`. Read-only `record()` and `guard()` accessors suffice for the first slice. It exposes no completion proof, mutable mapped state, ordinary replay entry, consuming generic guard handoff or readiness method.

`RecoveryIntentEvidence` has private fields binding runtime, original full ManifestRecord, actual manifest Arc/native ID, actual main/WAL native IDs and Weak original PublicationLease. Its private constructor receives only the freshly reselected actual manifest. `validate(file, record, guard)` checks those associations, the original intent/no-downgrade shape and native no-snapshot locks; it does not read `guard.published()` or clear recovery state.

The outer driver retains the captured full record and original pause, consumes the selection's guard into `ManifestLoad::new(runtime.files.manifest.clone(), ...)`, and requires byte-for-byte record equality after fresh selection. It waits for read completion retirement before starting writes. It retains the evidence while driving publication and validates the returned actual record/guard again before returning the sealed owner. Error/drop releases only the owned state; backend completions retain the existing authority ownership pins.

Minimal manifest adapter, using the existing loop:

```rust
#[cfg(all(host_shared_wal, unix))]
impl LoadedManifest {
    pub(in crate::mvcc) fn mirror_recovered_checkpoint_intent(
        mut self,
        evidence: &crate::mvcc::database::RecoveryIntentEvidence,
    ) -> Result<CheckpointIntentPublication> {
        evidence.validate(&self.file, self.record, self.guard()?)?;
        if !self.guard()?.io_retired() {
            return Err(LimboError::Busy);
        }
        let file = self.file.clone();
        let publication = ManifestPublication {
            file: file.clone(),
            ownership: Arc::new(Mutex::new(self.guard.take())),
            slot: 1 - self.slot,
            record: self.record,
            stage: PublicationStage::Start,
        };
        Ok(CheckpointIntentPublication {
            file,
            publication,
            mirrored: false,
            durable: None,
            terminated: false,
        })
    }
}
```

This copies the actual selected intent to the opposite slot first, then rewrites the original slot. It does not call `begin_checkpoint`, advance sequence or create a checkpoint target. `MirroredCheckpointIntent` remains an intent-only capability; ordinary materialization authorization still rejects an unrecovered source.

## Task 1: Genuine crash fixture and missing mirror regression

**Blocked by:** Parent review of this exact first-slice seam.

**Files:** New fixture and intent test child; minimal runtime declarations and explicit missing-implementation entry.

**Interfaces:** `IntentCrashFixture::new(point, acknowledged_commits)` returns actual retained runtime/files, saved main/log/ID bytes and the actually decoded selected intent. `CrashPoint` has exactly `SecondIntentWrite` and `FirstMainBackfillWrite` for this slice.

- [x] Create an existing marked database with the established real fixture, then start a child process that opens it normally in test-only shared mode and acknowledges one or two logical updates before starting `SharedCheckpointPublication::new`.
- [x] At `SecondIntentWrite`, defer submission of the second intent-slot write only after the first intent write and FULL sync complete. At `FirstMainBackfillWrite`, defer the first main write only after both intent mirrors and a WAL FULL sync. Classify manifest writes by their decoded actual intent record so earlier normal commits cannot satisfy the counters.
- [x] Send the parent acknowledgement only after the targeted physical operation is retained. Parent kills the child with SIGKILL and waits for exit; do not unwind the coordinator or run normal cleanup. Bound child acknowledgement to 15 seconds and exit to 5 seconds.
- [x] Open only `SharedMvccRuntime::open_existing` from actual retained native files in the parent. Save actual selected intent/prefix/IDs; assert old main is unchanged, logical prefix is nonempty, and the second crash point has actual committed WAL frames. Read WAL through the existing recovery read transport when inspecting it.
- [x] Add the explicit missing mirror implementation and run `checkpoint_recovery_intent_native_mirrors_actual_crashed_selection` against both crash points and both manifest-slot parities. Expected initial RED: exactly one parent test fails with the explicit missing-implementation error, not a compiler error, timeout or fixture failure.

The required runtime call sequence is:

```rust
let mut opening = fixture.runtime.begin_checkpoint_recovery().unwrap();
let selection = fixture.io.block(|| opening.poll()).unwrap();
let expected = selection.manifest.record();
let mut mirroring = selection.begin_intent_mirror().unwrap();
let recovered = fixture.io.block(|| mirroring.poll()).unwrap();
assert_eq!(recovered.record(), expected);
assert!(recovered.record().checkpoint_intent().is_some());
assert!(recovered.guard().requires_recovery());
assert!(recovered.guard().published().is_err());
```

## Task 2: Fresh intent mirroring with no readiness publication

**Blocked by:** Task 1's genuine behavior RED.

**Files:** Runtime intent child; narrow reexports; manifest adapter above; intent tests.

**Interfaces:** Consume the actual selected intent and continuous guard/pause; produce only `RecoveredCheckpointIntent` after both mirrors and backend retirement.

- [x] Replace the explicit missing error with fresh actual-file selection, sealed native/lease evidence, existing publication loop and final association checks described above. Keep normal constructors, replay, materializer and completed-confirmation guards untouched.
- [x] Require exactly two new writes and two FULL syncs in the parent recovery operation; decode both durable slots and compare their complete records with the original selection. Assert sequence, source/target generations, source timestamp, salt and acknowledged prefix remain unchanged.
- [x] Assert main, WAL, logical log and ID bytes remain unchanged, the original recovery flag is not cleared, runtime replay remains absent, and actual ordinary opens cannot admit the intent. Keep the returned owner alive while checking admission and competing authority are `Busy`.
- [x] Reject normal records, same-byte nonnative manifest substitutions, replacement leases and actual manifest changes between selection and fresh reload before any manifest write. Restore the original actual intent after corrupt-selection injection and verify a fresh retry.
- [x] Inject error/short-write at each of the two writes and error at each FULL sync. Re-select on retry and preserve at least one valid original intent throughout the opposite-slot-first sequence.
- [x] Cancel at fresh manifest read, first write/sync and second write/sync, before callback and after callback but before backend Arc release. Both competing authority and functional admission remain `Busy` until actual backend release; no completed proof, physical mutation or readiness is produced.
- [x] Add causal controls for accepting a same-byte memory manifest, skipping the second fresh FULL-sync mirror, and returning before completion retirement. Verify each intended assertion fails, restore exact source and rerun.

## Task 3: Narrow verification, review and handoff

**Blocked by:** Tasks 1–2.

- [x] Run the native fixture/mirror tests on an exact pinned source with explicit Bash and `set -euo pipefail`; preserve source SHA, raw parent summaries, actual RED/control logs and restored results. Use unquoted artifact globs inside YAML block paths.
- [x] Run existing `checkpoint_recovery_entry_native_`, recovery physical inspection and completed-confirmation selectors to prove the intent slice does not broaden normal completion. Run manifest publication/codec and authority suites, then strict library Clippy and formatting.
- [ ] Confirm Windows compilation with the new seam fully Unix-gated. Obtain independent source review and push the verified private component.
- [ ] Hand off the sealed actual mirrored-intent owner for a separately reviewed physical-WAL recovery plan. Its next stage must validate actual committed WAL/main state and reconcile physical coordination before main-only inspection; it may not infer completion from timestamps or bypass existing completed-ID preparation/finalization.

## Design stress test and boundaries

- **One durable intent and one older normal record:** rewrite the opposite slot first, preserving the actual selected intent until its replacement is FULL-synced; then rewrite the original. Keep sequence unchanged.
- **Committed WAL with old or partial main:** mirroring remains valid because it claims no physical state. Existing main-only physical inspection continues to reject nonempty WAL.
- **Main already at the acknowledged timestamp:** still return only mirrored intent. This timestamp grants neither materialization proof nor finalized IDs.
- **Idle handles in another process:** do not demand guessed exclusive lifetime ownership or trust mapped frame metadata. Native snapshot/admission exclusion is required here; physical coordination reconciliation remains a distinct next-stage requirement.
- **Cancellation after a successful callback:** callback success is not backend retirement. Retain the existing pin/lease ownership until the actual backend Arc is released.
- **A new selected record under the same guard:** reject a changed full record before writes. Never publish a newly constructed normal record to make a normal-only reader accept it.

No new business-domain term or hard-to-reverse disk decision is introduced. The existing checkpoint intent, materialization and completion distinctions remain the domain model. The parent's review of this seam is the design hardening gate; no separate user approval is needed within the already approved protocol.


## Verification evidence

Engine `28f55174b3ec300d137734ced23e5793448960ce` passed [native and Windows validation](https://github.com/HundredBillion/turso/actions/runs/42496250878). The workflow uses Rust1.88, two build jobs, explicit Bash and `set -euo pipefail`; its validation-only branch is excluded from product integration.

The actual final Linux parent summaries are: intent9 passed; restored recovery24 passed; manifest31 passed/4 ignored; authority15 passed; full core2900 passed/143 ignored. All report zero failures. Strict library Clippy (`bench,conn_raw_api`), formatting and source integrity passed. Windows library checking with the same features also passed.

Three removals each produced the intended zero-pass/one-failure result: accepting a copied memory manifest, observing only one write/FULL sync, and returning an owner before backend retirement at operation3. Restoring the exact source preceded the24-test recovery run. The genuine missing-implementation RED is separately recorded by run42493100112 on `ed4502bee2ca597eb13045edc8dd65d5fc866f4a`; earlier compile and fixture framing failures are not counted as behavior RED.

Raw source pins, parent summaries, removal-control logs and run archives are retained in `.local/issue-resolution/interrupted-intent-mirroring/`, including artifact13624265407 under `final-28f/`. Independent final source review and the next-stage handoff remain explicit completion gates above.
