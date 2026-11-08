# Shared Physical Base Reader Technical Spec

> **For agentic workers:** REQUIRED SUB-SKILL: Use dmi-superpowers:executing-plans to implement this TSP task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stage the actual materialized header, schema, persistent MVCC timestamp, and root facts without changing an existing database instance or entering ordinary shared admission.

**Architecture:** A private Database child constructs an isolated, read-only physical reader from retained files and configuration. It has its own pager state, cache, schema, and bootstrap connection, and no MvStore. An MVCC-owned caller retains authority and admission pause, validates the staged facts against durable protocol state, waits for physical I/O retirement, and only then constructs the appropriate opaque evidence.

**Tech Stack:** Rust; existing Database/Pager/schema parser; retained native files; ReadPinnedFile; shared manifest/ID protocol.

## Global Constraints

- Private shared-MVCC integration only; no public activation or ordinary-open behavior changes.
- Initial supported platform remains Linux 64-bit on validated ext-family filesystems with OFD/mmap coordination.
- PDO-only initial public scope and explicit offline upgrade requirements remain unchanged.
- Never fabricate a source database identity from a timestamp or a caller-supplied generation.
- Never publish a staged schema into an existing Database or mutate its MvStore during physical inspection.
- Never open/recreate main, WAL, logical, or coordination files by basename in this reader.
- A nonempty physical WAL or unresolved intent needs reconciliation; an artificial no-op WAL is not evidence of a valid main-only view.
- The existing coordinator tail retains authority and admission pause. The reader does not release either or reclaim logical history.

## Source audit and creation order

Audited generation-reload worktree at `9a70a0f5f` without source edits.

1. `core/database.rs::open_shared_async` creates SharedMvccRuntime and its retained files, calls `begin_open`, then calls `poll_manifest` while phase is Init. Only afterward does `do_open_async_internal` construct Database through `Database::new`. Therefore fresh recovery at the manifest rejection boundary has **no source Database or MvStore**. Do not require an existing Database as the reader factory's receiver.
2. `begin_open` already owns an AdmissionGuard. It cannot retain that shared admission token while acquiring CheckpointPause. Use a separate explicit recovery entry before normal begin_open. If recovery is entered after a failed ordinary shared-open attempt, drop the normal open state, acquire pause then authority, and reselect the manifest. The earlier selection is not retained proof across reacquisition.
3. `Database::new` begins with an empty MvStore slot and a no-op WAL, and constructs fresh SharedPagerState/cache/schema. It also synthesizes initial page 1 for an empty file. Reject empty/short input before creating the private Database; never use that synthesis as a recovered physical image.
4. Avoid `header_validation` and `do_open_async`: those paths decode/transition journal mode, open WAL/log storage, and may construct/bootstrap an MvStore. `Database::_init_nonblock` can initialize a private main-only pager without those actions, but its page-layout checks are not a substitute for explicit shared-marker/magic/header validation.
5. `Connection::drive_reparse_building` updates sequence watermarks through `self.db.get_mv_store()`, even when the connection is a bootstrap connection or publication is disabled. An existing Database with its MvStore attached is therefore unsuitable. The isolated private Database must keep `get_mv_store()` empty throughout parsing.
6. Reparse installs the connection schema but does not guarantee equality with the Database schema Arc. `update_schema_if_newer` only replaces the database cache when the cookie strictly increases. After parsing, explicitly assign the **same parsed Arc into both private caches**, including equal/zero-cookie cases. Never perform this assignment on the existing live Database. Reparse's temporary namespace/type/sequence schemas are private and its existing schema-reparse guard prevents ordinary cache adoption during the build.
7. `shared_bootstrap::prepare_shared_base_context_nonblock` requires persistent bootstrap connection plus exact connection/database schema Arc equality. Its metadata query uses no store state, but the method currently requires `&MvStore`, and its completion builds RecoverCtx from that store's logical file/encryption context. That receiver is unavailable in fresh recovery. Extract the physical fact collection from the final RecoverCtx construction rather than creating a fake MvStore solely to satisfy this API.

## Proposed ownership and interfaces

Names below are proposed private seams, not existing capabilities or completed proof APIs.

- Database child `core/database/shared_physical.rs`: `SharedPhysicalRead::new(input: PhysicalReadInput) -> Result<Self>` and `poll(&mut self) -> IOResultOr<PhysicalBaseParts>`.
- `PhysicalReadInput` carries retained read-pinned main and physical-WAL handles, expected retained native file identities, IO, a persistent diagnostic path, database feature configuration, dialect, and allocators. It does not require a source Database, caller-supplied Schema, or generation masquerading as physical evidence. Do not use arbitrary custom storage/codecs outside the supported shared-runtime contract.
- The private Database is constructed with ReadOnly/NoLock policy and a DatabaseFile over the pinned read-only main facade. Mutating File operations fail ReadOnly even if a parser path unexpectedly requests a write. Use `new_connection` directly with bootstrap=true and deferred/no speculative connection-time stats work; do not call public connect/open. The no-op WAL is installed only after the retained physical WAL is proved empty under authority.
- `PhysicalBaseParts` contains actual header/layout, parsed Schema Arc, sqlite_schema rows, type SQL, persistent timestamp, and checked physical root/page bounds. It is staging data, **not CompletedCheckpointProof**. Keep construction private to the reader; the proof-producing runtime path obtains it only from its owned reader, not as an arbitrary public argument.
- Keep `AuthorityGuard` confined to MVCC visibility. The MVCC-owned driver wraps the validated retained file with ReadPinnedFile before calling the Database child and retains the original guard/lease identity across every poll. It validates matching runtime/main identity, selected manifest/ID state, quiescence, and actual persistent metadata before producing any recovery evidence.
- The driver waits for `guard.io_retired()` and its pending retirement completion before evidence escapes. Do not hold a permanent counted IoPin in the reader: that would prevent retirement from ever becoming true. Each submitted physical read owns its own pin until backend completion retirement.
- The reader owns only private pages/schema. Cancellation drops statements/cursors before private connection/pager state. A late callback may finish its detached private buffer but cannot install a page into an old or newly reloaded live pager.

## Task 1: Isolated main-only physical staging

**Blocked by:** None; this delivers staging facts, not completed-generation authorization.

**Files:**
- Create `core/database/shared_physical.rs` with private factory/state and focused tests.
- Modify `core/database.rs` only for child declaration and narrowly necessary private constructor access.
- Refactor `core/mvcc/database/shared_bootstrap.rs` physical fact collector without changing the legacy bootstrap contract.

**Interfaces:** Consume validated retained pinned files/configuration. Produce `PhysicalBaseParts` without requiring an old MvStore.

- [ ] Write a native regression where old live schema/root/sequence-watermark state differs from materialized disk, and snapshot every old state object before the reader runs.
- [ ] Observe RED for the missing isolated reader; do not substitute normal shared open or an old-store reparse.
- [ ] Build the private Database/pager from retained main with verified empty WAL; explicitly reject empty input, invalid marker/layout, codecs, mismatched identities, and nonempty WAL.
- [ ] Drive schema reparse nonblocking, assign one Arc to both private caches, and collect timestamp/schema/type/header facts without attaching a MvStore.
- [ ] Verify old store/schema/pager/sequence state and all file bytes remain unchanged; validate actual on-disk payload/root facts rather than merely a header mutation.
- [ ] Cover the equal/zero schema-cookie case and absence/invalidity of persistent metadata.
- [ ] Run focused native tests and strict lint; commit the staging-only component with its exact limits.

## Task 2: Callback retirement and cancellation boundary

**Blocked by:** Task 1.

**Files:** The reader test module; reuse existing ReadPinnedFile machinery rather than changing ordinary pread semantics.

**Interfaces:** A driver retains exact authority while the reader may yield; no evidence returns until physical completion retirement.

- [ ] Add delayed header and schema-page reads; cancel before callback and after callback while the backend retains the physical Completion Arc.
- [ ] Prove competing publication authority stays excluded until the backend retires the completion, and no mutation/read admission occurs through the abandoned private reader.
- [ ] Verify the old live pager/cache and newly staged schema never receive the cancelled callback's page.
- [ ] Add successful callback-before-retirement coverage so callback readiness cannot be mistaken for proof readiness.
- [ ] Run causal bypass control against unpinned input, restore, verify GREEN, and commit.

## Task 3: Runtime evidence and explicit recovery entry

**Blocked by:** Tasks 1–2 and the separately reviewed checkpoint recovery protocol.

**Files:** MVCC runtime/checkpoint recovery child; existing completed-proof module only through a reviewed narrow constructor.

**Interfaces:** Live entry consumes the coordinator's retained tail. Fresh entry starts from runtime/files/configuration and pause→authority→fresh manifest selection, not an unavailable source Database.

- [ ] Add a fresh-process fixture whose normal open rejects intent before Database construction.
- [ ] Demonstrate separate recovery acquisition/reselection and reject a changed lease or stale manifest identity.
- [ ] Reconcile nonempty physical WAL/unfinished materialization before constructing completed physical evidence. An intent may coexist with an already-materialized target; equal timestamps alone do not identify a source generation.
- [ ] Compare actual persistent timestamp/layout/root bounds with the selected completed target, validate ID selection/floor, and freshly mirror required durable witnesses before creating the corresponding opaque recovered proof.
- [ ] Cover first completed-manifest-slot durability, loss of that newer slot, prepared-ID selection, and already-finalized target-only selection without inventing source identity.
- [ ] Hand verified staging plus proof to generation reload, then reclaim logical bytes and release authority/admission only through the complete coordinator tail.

## Bounded factory milestone (2026-11-06)

The audit above did not implement recovery. The separately approved factory subset is now implemented at `784b38afc`: `SharedPhysicalConnection` consumes retained storage/configuration, constructs an independent readonly bootstrap Database and pager, reparses physical schema, and explicitly assigns the same parsed Arc to both private caches. It neither opens a pathname nor joins the registry. No source Database or dummy MvStore is required.

The caller must supply a readonly storage facade with physical read pins and must already have verified/reconciled empty physical WAL under retained authority. The factory does not validate authority, marker identity, checkpoint completion, or persistent metadata semantics; it returns only a connection for staging reads. Empty/unretained storage and unsupported encryption/codec/durable-storage configurations are rejected. This narrow factory is not `PhysicalBaseParts` or `CompletedPhysicalProof`.

Evidence:
- Portable missing-factory RED: the retained-main test reached the explicit unimplemented error (zero passed, one failed).
- Portable GREEN: three tests cover retained-main reads with zero schema cookie and no registry/path-open/write attempts; physical MVCC metadata and sequence parsing without changes to the source schema/root/watermark; and empty/unretained input rejection.
- Native run [42318232064](https://github.com/HundredBillion/turso/actions/runs/42318232064) checked exact source `784b38afc2ddfae50bd25e7f9f9a6ee98490a37d`. The real completed marker254 fixture passed using retained runtime main wrapped in ReadPinnedFile and DatabaseFile. It read the materialized payload and exact persisted checkpoint timestamp, preserved source state/main bytes, and verified read pins retired before returning control to the caller.
- The same run passed coordinator3, completion3, ID-finalization3, guarded15, shared221 (20 explicitly ignored), native materialization/open/commit two each, and strict library Clippy. Local format/diff checks and strict library Clippy also passed.

The factory evidence above covers the independent staging connection. Subsequent physical facts and retirement checks are recorded below; neither milestone is end-to-end recovery or public-mode acceptance.

## Verified live physical staging milestone (2026-11-07)

`d4013baff` extracts store-independent `PhysicalBaseReadState` and `PhysicalBaseParts` while preserving the existing bootstrap-to-RecoverCtx wrapper. The timestamp reader/validator is shared through an unconditional module, so ordinary metadata reads do not depend on Unix/shared-WAL support. Raw parts contain actual header, schema/type rows and persistent timestamp; they do not authorize recovery or generation publication.

`b5205e5f4` consumes the coordinator's original PublishedSharedCheckpoint and returns verified physical staging with the same FinalizedIds, completion proof, authority and admission pause. It checks exact store/runtime/lease/native main identity, quiescence, empty WAL, marker/header extent, physical schema roots, persistent target timestamp and ID floor. All main reads use the read-pinned retained file and an isolated bootstrap connection. Evidence does not escape before backend completion retirement.

Native [run 42430397394](https://github.com/HundredBillion/turso/actions/runs/42430397394) verified exact source `7df8f89ec43d405fe89d78b5939f1dd6e0b75157`: three live staging cases, four fact/bootstrap cases, three publication-coordinator cases, two read-pin tests, 558 MVCC database tests (22 ignored), strict library lint, formatting and source integrity passed. Cancellation coverage delays both the 512-byte header read and a schema-page read, before and after callback readiness, while checking authority exclusion and unchanged live schema/cache/main bytes. Successful fact collection also waits while the backend retains the completed physical Completion.

Two deliberate controls failed at their intended assertions, then the restored implementation passed: bypassing read pins released authority before backend retirement; bypassing the retirement wait returned physical evidence too early. The original staging stub also failed at its explicit missing implementation in [run 42429344746](https://github.com/HundredBillion/turso/actions/runs/42429344746).

This milestone does not install store metadata or pager generations, replace local replay/schema state, reclaim the logical log, finish authority, release admission, or enable public mode. Fresh interrupted-checkpoint acquisition, physical reconciliation and recovered-proof construction remain separately owned recovery work.
