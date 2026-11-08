# Normal Recovery Bootstrap Technical Spec

> **For agentic workers:** REQUIRED SUB-SKILL: Use dmi-superpowers:executing-plans to implement this TSP task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn a genuinely recovered, finalized and reclaimed NORMAL checkpoint into one private Database/Store/Pager and reopen admission only after their metadata, schema, append state and generation are certified together.

**Architecture:** Consume ReclaimedRecoveredCheckpoint into a single owning bootstrap state. Its linear Store reservation constructs exactly one Store; a private Database factory initializes retained main/WAL storage under the original proof/authority/pause and requires that same sealed source on every poll. Install the prepared physical base and identical schema/replay context, certify the actual final connection's Pager, then publish replay and finish authority before releasing admission and returning a usable connection.

**Tech Stack:** Rust 1.88; existing retained-file factories, PreparedGenerationReload, PhysicalBaseParts, SharedReplayContext, CompletedCheckpointProof, FinalizedIds, GuardedFile read bindings and pager generation certification.

## Global Constraints

- Preserve reclaim release `194546230` on its branch. Initial design base `0f539e9f8fc6f06d49540640d027d980a4cb596b` combines that release with independently verified integrated runtime `fa22532749e2c20839ad12b678fc8985d9b83fa6` (native joint `42492094119` SUCCESS). The sole merge conflict preserved both sets of shared_runtime child declarations; union libcheck, generation2 and IDs78/1ignored plus format passed locally.
- Before production implementation, integrate the parent's verified six-PR union and update this plan/ledger to its exact source. That union moves private fields into DatabaseSharedState and removes read-only promotion. Modify only the new unique shared state before wrapping/cloning it; do not revive promotion or ReadonlyStorage.
- Parent reviews this complete written seam before production. Existing user-approved #7 recovery protocol applies; no additional user design permission is required for its implementation.
- Only a sealed source-identity-free NORMAL completed proof with finalized target IDs, empty retained WAL, reclaimed logical file and exact empty append state is accepted. Intent/nonempty-WAL recovery and terminal downgrade are separate owners.
- The original native authority lease and admission pause remain continuous through construction, I/O retirement, metadata installation, Pager certification and authority.finish. Never drop/reacquire the guard or manufacture a proof from timestamps/source generation.
- Runtime replay remains None until all fallible physical construction, retirement and local installation checks pass. No Database, Store or writable connection is returned before the actual finish boundary.
- Store attachment is one-shot per Runtime. Repeated polls retain the same actual Store and Database. Cancellation after attachment never frees the Runtime's permanent claim; retry requires dropping all old owners and genuinely reopening/recovering a new Runtime.
- This private completion path uses the existing SQLite/PDO EngineDialect and default embedded database configuration, deriving read-only permission from the original Runtime open rather than a new caller assertion. It performs no WAL-to-MVCC conversion, AUTOINCREMENT migration, bootstrap metadata writes or PRAGMA journal_mode change. The proof already certifies an existing materialized MVCC database. Public URI/PDO activation remains disabled until the complete crash-acceptance package passes.
- Coordinate core/database.rs changes with the downgrade owner and root. Keep new factory logic in its own child. Do not merge validation workflow commits into product. Reuse existing build targets, jobs2, pinned Rust1.88, debug/incremental off; explicit Bash/pipefail for Linux validation.

## Ownership and interfaces

Create `core/mvcc/database/shared_checkpoint_recovery_bootstrap.rs` as a child of the reclaim module. It consumes private reclaimed fields without exposing raw constructors. Create `core/database/shared_recovered.rs` for retained physical construction; add only its module declaration and the required unique-state setup access in database.rs.

```rust
impl ReclaimedRecoveredCheckpoint {
    pub(in crate::mvcc) fn begin_bootstrap(self) -> Result<RecoveryBootstrap>;
}
impl RecoveryBootstrap {
    pub(in crate::mvcc) fn poll(&mut self) -> IOResultOr<RecoveredDatabaseReady>;
}
```

RecoveryBootstrap privately owns RecoveryBootstrapEvidence and RecoveredDatabaseFactory. RecoveryBootstrapEvidence is crate-visible solely to let the Database child consume validated facts; its fields and constructors stay private. It retains runtime, the actual Store, physical parts/header/schema, the genuine finalized IDs/proof/guard, selected normal manifest record, prepared roots/replay, original connection Weak identity once captured, main/WAL checkpoint bindings, and admission pause. No generic LoadedIds or constructor permitting arbitrary proof/parts is added.

RecoveredDatabaseReady is sealed until authority.finish succeeds. Only then does its consuming accessor return the actual `Arc<Database>` and `Arc<Connection>`. The private Database is not inserted into the public registry by this prototype; public activation must connect this owner to the normal registry/open lifecycle rather than publish another entry point.

Store construction extends SharedStoreReservation with one consuming operation:

```rust
pub(super) fn construct_store(
    self,
    allocator: crate::alloc::DynAllocator,
) -> Result<Arc<crate::mvcc::MvStore>>;
```

It allocates a fresh Store using actual runtime.storage, the supplied default Database allocator and MvccClock, with passive checkpointing disabled. It retains the reservation while allocation may fail. Under the existing claim mutex, it validates Reserved, attaches this newly created, never-exposed Store to the actual Runtime, transitions to Attached and disarms the token before its destructor runs. No fallible operation follows a successful attachment before the token is disarmed. Failure before attachment drops the unused Store before releasing the reservation. No arbitrary preexisting Store is accepted. Preserve same-Store ordinary attach idempotence.

Runtime retains the original read-only open mode as an immutable private field initialized by open_existing; factory flags and final connection permission derive from this field. This does not make a read-only source writable. Coordination sidecars remain writable as already approved; read-only user SQL remains enforced.

Database factory interface:

```rust
impl RecoveredDatabaseFactory {
    pub(crate) fn new(source: &RecoveryBootstrapEvidence) -> Result<Self>;
    pub(crate) fn poll(
        &mut self,
        source: &mut RecoveryBootstrapEvidence,
    ) -> IOResultOr<()>;
    pub(crate) fn finish(
        self,
        source: RecoveryBootstrapEvidence,
    ) -> Result<RecoveredDatabaseReady>;
}
```

The factory never returns an intermediate Connection or Database and has no raw-file-only constructor. It captures the original source Runtime/Store/lease identity and rejects a different source on every resume. Its fields retain one raw/Arc Database, one InitState, retained-WAL builder, one Pager and one final normal connection across I/O. It does not call ordinary begin_open/connect admission, which must remain Busy while the recovery pause is held. It does not call ordinary MVCC bootstrap, which would reacquire authority or run migration SQL.

PhysicalBaseParts already contains the actual parsed schema, sequence descriptors, table-valued functions, type rows and physical base timestamp from the read-only collector. Use its exact schema Arc for both Database and Connection and for the prepared replay context. Do not reparse into a second unbound schema or fabricate default descriptors. Native acceptance includes sequences and AUTOINCREMENT to verify this claim; any missing read-only catalog initialization must be completed under the retained source before Ready, never hidden by prematurely releasing admission or enabling mutation bootstrap.

## Construction and publication sequence

```text
Consume genuine reclaimed cap
  -> validate native owner/lease/normal target/IDs/empty concrete storage/no snapshots/retired IO
  -> allocate and permanently attach exactly one Store
  -> preflight physical root map and replay context using that Store and actual selected record
  -> bind retained main and physical-WAL reads to original authority
  -> construct unique private Database state with actual runtime/storage/Store
  -> build empty retained WAL; initialize its one private Pager (yielding)
  -> validate actual header bytes/layout/extent, exact Store/Runtime/native aliases, retire all IO
  -> install prepared Store metadata and identical captured schema Arc in DB/connection
  -> certify actual final connection Pager with original CompletedCheckpointProof
  -> validate exact original connection identity + metadata/schema/replay/storage/IDs/frontiers again
  -> publish prepared runtime replay
  -> consume original guard in finish; on error immediately withdraw runtime replay
  -> clear temporary bindings/constructor state; release pause last
  -> return usable Database/Connection
```

All allocations/context/root preparation happen before destructive local installation. Preparation can reuse PreparedGenerationReload::install with the real proof on the newly allocated no-transaction Store; it must preserve the global ID journal's permanent floor and route subsequent allocation through existing shared-ID APIs. The selected record must be the real normal empty target retained by reclamation. A freshly reselected record before final publication must still match that target; do not infer completion from mapped timestamps.

Build the Database from actual retained main DatabaseFile and retained physical WAL, using the proof-gated raw constructor under the current DatabaseSharedState layout. Do not reopen a basename WAL or synthesize page one. Initialize the retained WAL before Pager creation. As in HeaderValidationState::OpenWal, explicitly install the actual retained WalFile on the private Pager after initialization even when the empty WAL reports no enabled frames; never let an empty-file fast path leave a no-op or basename WAL attached. End the temporary initialization read transaction before changing/resetting its private cache, then validate the attached WAL runtime file Arc/native identity against the original retained file. Reject nonempty WAL and zero/changed main extent before initialization; header bytes, marker, page size, reserved space, schema cookie and layout must match captured physical facts before installation. The private factory may bypass ordinary header-validation/open-MVCC bootstrap only because the sealed source revalidates these actual facts; no ordinary public checks are relaxed.

The final normal connection is created exactly once with the private new_connection path, without SQL execution or snapshot registration before Ready. If a temporary bootstrap reader becomes necessary for a missing read-only catalog, it must be read-only, remain entirely inside the factory, retire its statements/read transaction and drop before final Pager certification. It cannot migrate/write physical metadata or be returned as the normal connection.

Capture the final connection Weak identity when installation begins and require that exact identity throughout certification/finish. Validate ownership through existing validate_shared_pager_owner/validate_shared_base_layout and certify_completed_pager. The latter must see runtime.replay None, same proof lease, exact target map, no live snapshots and retired physical I/O. Then check durable/last-commit/header frontiers, exact Schema Arc in both caches, prepared replay target, concrete runtime.storage file/offset/CRC/header/no-pending state and finalized target IDs.

Authority.finish consumes the original guard, but the source owner retains the pause until the factory has packaged the ready result and performed all cleanup. If finish fails, withdraw replay before any pause release. Any earlier error/drop leaves replay None. Drop private Connection/Pager/init state before dropping Store/finalized guard and finally pause; physical backend pins retain authority after callback until actual retirement. A discarded attached Store never makes the old Runtime fresh again.

## Task 1: One actual Store and full bootstrap counterexample

**Blocked by:** Parent approval, exact verified six-PR integration and updated source layout audit.

**Files:** shared_store_claim.rs, shared_runtime.rs, new bootstrap child and native bootstrap tests; TSP/ledger.

**Interfaces:** Exact begin_bootstrap/poll/result types above, initially returning `InvalidArgument("normal recovered database bootstrap is not implemented")` after genuine reclamation. Add immutable original-readonly Runtime state and the consuming reservation constructor.

- [ ] Extend the genuine fresh-process fixture: drop every old Database/Store/proof/Runtime/I/O holder, restore only actual earlier coordination image as already approved, strictly reopen, confirm/finalize/reclaim, then invoke full bootstrap and require post-recovery SELECT/UPDATE/commit plus fresh peer reopen. Run actual native 0/1 RED at the missing bootstrap diagnostic after existing recovery cohorts pass.
- [ ] Add consuming reservation tests proving exactly one actual Store pointer survives repeated polls; another ordinary attachment is rejected, same-Store attachment remains idempotent, allocation failure releases only an uncommitted reservation, and dropping an attached construction never permits a replacement Store on the same Runtime.
- [ ] Implement permanent attach/disarm using the existing runtime claim mutex. No arbitrary Store/proof constructor or LoadedIds escape. Run native ownership positives, existing attachment/thread tests, strict lint/format and independent source review before the dependent constructor work.

## Task 2: Private retained Database and Pager under the continuous source

**Blocked by:** Task 1.

**Files:** core/database/shared_recovered.rs, narrow unique-state setup in database.rs, bootstrap child and tests.

**Interfaces:** Source-gated factory new/poll/finish above; no intermediate Connection accessor or public activation.

- [ ] Write yielding constructor tests using actual retained main/WAL and DeferredIo-style callback/physical-retirement controls. While main header or page-one I/O is held, ordinary prepare/begin/checkpoint and competing publication remain Busy. Resume with foreign Runtime/Store/lease evidence must reject before another read.
- [ ] Implement one private DatabaseSharedState with actual retained runtime.main, runtime.storage, original read-only mode, the one claimed Store and actual retained WAL. Use Arc::get_mut only before sharing the new state. Drive BuildSharedWal and _init_nonblock; retain the same objects across yields. No basename or public registry/open route.
- [ ] Validate actual header bytes and all source aliases/layout before installing metadata; await backend retirement. On cancellation end the private pager read transaction and drop private views before the authority/pause owner. Add tests for callbacks retaining backend authority after the whole constructor is dropped; retry requires genuinely reopening a new Runtime after retirement.
- [ ] Run native constructor positives/foreign-owner/failure/cancellation controls plus existing normal shared-connection and pager-generation cohorts, strict lint and Windows cfg checks for every new host+unix seam. Obtain independent source review.

## Task 3: Metadata installation, certification and actual Ready

**Blocked by:** Task 2.

**Files:** bootstrap child, factory and focused native tests; reuse existing generation/replay APIs.

**Interfaces:** Produces only fully finished RecoveredDatabaseReady; no separate partially ready connection result.

- [ ] Add genuine native acceptance with preserved rows/index, actual persistent sequence descriptor/watermark, AUTOINCREMENT high watermark and retained ID floor. Before the final step assert replay None, no snapshots and admission/authority Busy. After the step perform SELECT, UPDATE and INSERT with COMMIT; sequence/AUTOINCREMENT values and shared logical IDs must not reuse prior values. A fresh peer/process reads the committed result and can commit back.
- [ ] Preflight actual root map/context/schema/replay allocations, install using real completion proof and same guard, publish identical schema Arc in Database and final normal connection, certify its actual Pager with certify_completed_pager, then validate all frontiers/identity/storage and fresh actual normal record. Use exact final connection Weak identity; reject a different connection sharing the same Database/schema/Pager.
- [ ] Publish runtime replay only after all physical retirement and certification. Consume guard.finish; withdraw replay on failure while pause remains held. Release pause only after cleanup/package moves are infallible. The first SQL statement executes through normal shared admission and snapshot registration, not bootstrap bypass.
- [ ] Verify read-only recovered sources can SELECT after Ready, reject writes with the ordinary read-only error, and preserve main bytes. Coordination finalization/reclamation retains the already approved metadata-write policy. No writable promotion is added.
- [ ] Run actual positive and post-ready peer/reopen cases with causal bypasses for exact original connection, Pager certification, late validation and replay withdrawal on finish failure. Restore exact source before rerunning affected cohorts.

## Task 4: Crash/cancellation acceptance and release

**Blocked by:** Task 3.

**Files:** Native constructor/bootstrap transport tests, separate validation workflow and TSP evidence.

**Interfaces:** Same genuine recovered cap and real retained files; no fake proofs, mutable readiness test escape or inferred source generation.

- [ ] Inject failure/cancellation at constructor header/page reads, metadata preparation/installation boundary, certification and finish. Test before callback and after callback-before-physical-retirement for every actual I/O. No Ready result or replay readiness may escape; peers cannot ACK/commit until backend retirement and fresh legitimate recovery.
- [ ] Prove late source/extent/header/record/Store/schema/frontier changes reject under the original lease. Once the Store was attached, a dropped state keeps the old Runtime permanently claimed; after dropping ALL old holders, genuinely reopen and recover a new Runtime and finish successfully.
- [ ] Exercise completed-normal crash images with no logical tail, obsolete logical tail, prepared-vs-already-finalized IDs, fresh incarnation with real Some(max_epoch), and read-only source. Interrupted intent/nonempty physical WAL and terminal downgrade still explicitly reject this entry.
- [ ] Run exact source causal REDs for omitted source/lease ownership, missing physical retirement, premature replay/finish cleanup and skipped Pager certification; restore source and require all native recovery/constructor/generation/ID/sequence/watermark/readonly cohorts, full core, SDK checks appropriate to the final union, strict lint/format and Windows cfg checks.
- [ ] Obtain independent whole-branch review including actual raw logs and exact source/workflow SHA. Retain the private branch for parent integration. Keep public PDO activation disabled until the broader process-death/crash matrix and public registry/URI integration are separately verified; do not mark PR8 mergeable from private Ready alone.
