# Shared checkpoint entry technical spec

> **For agentic workers:** REQUIRED SUB-SKILL: Use dmi-superpowers:executing-plans to implement this TSP task-by-task.

**Goal:** Make explicit Core and SQL checkpoints complete the existing shared publication, physical verification, local installation, and Ready handoff.

**Architecture:** Keep the caller's explicit-checkpoint statement guard. A single blocking adapter drives existing typed state machines; the coordinator retains actual materialization metrics with native checkpoint locks released at their original boundary. Return those metrics only after the installed generation is certified Ready.

**Tech Stack:** Rust 1.88, native Linux filesystem locks, existing MVCC state machines.

## Global constraints

- Ordinary MVCC and WAL checkpoint paths retain their existing behavior.
- Shared checkpoints use the globally quiescent FULL/TRUNCATE protocol for all public checkpoint modes; internal timestamp bounds are rejected before mutation.
- Live readers cause bounded BUSY; no reader eviction or retries.
- Public C/PDO activation remains disabled until combined acceptance.
- No separate PRD was supplied; the approved issue #7 design is the source requirement.

### Task 1: Route and verify explicit checkpoints

**Blocked by:** None

**Files:** `core/connection.rs`, `core/vdbe/execute.rs`, `core/mvcc/database/shared_checkpoint.rs`, `core/mvcc/database/shared_runtime.rs`, `core/mvcc/database/mod.rs`, `core/mvcc/database/shared_checkpoint_entry_tests.rs`, `docs/sql-reference/pragmas.mdx`.

**Interfaces:** Consume `SharedCheckpointPublication::new(store, connection)`, `PublishedSharedCheckpoint::begin_physical_read`, `VerifiedPhysicalBase::begin_local_install`, and `InstalledLocalCheckpoint::publish_ready`. Produce `pub(crate) fn checkpoint_shared(connection: Arc<Connection>, mode: CheckpointMode) -> Result<CheckpointResult>`.

- [x] Add actual public API, SQL PRAGMA, and live-reader checkpoint regressions using `shared_runtime_fixture`; assert persisted payload, reclaimed logical file, repeated commits/checkpoints, and reader preservation.
- [x] Run the three tests on native Linux against unchanged production code; retain actual failure output and positive selected-test counts.
- [x] Retain materialization result in the coordinator and call `release_guard()` immediately on Done. Drive publication, physical read, installation and Ready in the adapter with `io.block`.
- [x] Route shared stores in Core checkpoint and the SQL opcode to the adapter after existing explicit guards; leave ordinary branches intact.
- [x] Run native regressions, existing checkpoint ownership/install/readiness suites, Rust formatting and strict core all-target lint. Document shared-mode quiescence and correct the contradictory read-only paragraph.
- [x] Commit, push to fork, record evidence, and obtain peer review before integration.

## Acceptance evidence

Exact runtime7a8717eee3c664433230789fe00f0ffc507f82ba passed native Linux run42520469060: allfourAPI/SQLmodes and bounded/readonly/reader refusal controls3, existingReady4 andinstallation6; strict productionlib andalltargetsClippy withconn_raw_api,bench,io_uring, andfmt. Baselinebff42519852274 selected3andfailed3 for missingmirroredintent; expandedc2 baseline42520262278 selected3andfailedonlySQLPASSIVE attranslation. Independentperformancepeerreviewaccepted thetypedlifetime/resultlockchain andthe7-linefollowup. Ordinary C/PDOactivationremainsdisabled.
