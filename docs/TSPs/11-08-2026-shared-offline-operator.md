# Usable explicit shared-MVCC offline operations

Status: proposed for source review; no operator/API implementation yet.

## Existing source and remaining seam

`mode_transition/operation.rs::execute_offline` already selects upgrade, new-file
publication, prepared-marker resume, downgrade-marker resume, and marker2 cleanup
resume. It requires `OfflinePrerequisite::AllOlderWorkersStopped`; each concrete
backend retains native identity/exclusion proofs. It is private and cannot be
used by the ordinary CLI. Fresh downgrade currently begins only by consuming
`InstalledLocalCheckpoint::begin_downgrade`, deliberately before publish_ready.

`cli/main.rs` opens the normal shell through `Limbo::new` before dispatching its
modes. `cli/app.rs::Limbo::new` parses `Opts` then immediately opens the database.
An offline operation must branch before this normal open; a dot command would
already retain the wrong lifetime and is therefore unsuitable.

`OpenOptions::with_shared_mvcc_for_test` is cfg(test). A non-test caller cannot
intentionally open shared mode today. The underlying default resolver already
has a distinct validated shared-mode path; the new entry must select that path,
not reinterpret ordinary URI filenames or replace native storage.

## Proposed bounded Core and CLI interface

1. Add a small exported Core offline operation enum and an explicit prerequisite
   acknowledgement, wrapping the existing private implementation. Supported
   operations: upgrade-existing, publish-new (distinct staging/destination),
   resume-upgrade-marker, downgrade-existing, resume-downgrade-marker,
   resume-downgrade-cleanup. No proof constructors or mutable metadata escape.
   Validate path/budget/operation combination before opening or creating files.
   Unsupported builds return an explicit unsupported error without mutation.
2. Add an intentional `OpenOptions::with_shared_mvcc(history_limit)` entry using
   the existing private option. It opens only an already marked supported native
   database; it does not auto-upgrade/create a shared namespace. Keep the test
   helper delegating to that same entry. Existing ordinary open/URI defaults and
   the C/PDO hard rejection remain unchanged until their final activation gate.
3. Add `tursodb PATH --shared-mvcc-offline OP --all-workers-stopped`, with a
   history-limit option (existing default64MiB) and staging path required only
   for publish-new. Reject SQL, read-only/custom-VFS, MCP/sync-server, codec and
   incompatible experimental open options for this administrative path.
   Parse Opts once in main, branch into the offline operation before any shell
   construction, and pass parsed options into the existing normal Limbo ctor.
   Preserve the ordinary CLI and mvcc_repl paths. No automatic retry or inferred
   worker shutdown. Errors are nonzero exit; success means the whole selected
   operation completed, including downgrade cleanup.

## Fresh downgrade obtains a genuine installed proof

A path entry is orchestration, never authority by itself. The private Core
operator opens the existing marked database with owned SyscallIO/default native
options, obtains its actual connection/store, then drives this existing chain:

- `SharedCheckpointPublication::new(store, connection)` and poll: stop admission,
  require real quiescence, refresh, durable intent, materialize, mirror completed
  target, finalize IDs under the same authority/pause.
- `PublishedSharedCheckpoint::begin_physical_read` and poll: read the physical
  metadata through the retained proof/namespace, wait for actual read retirement.
- `VerifiedPhysicalBase::begin_local_install` and poll: reclaim/reset/install the
  actual generation, return the genuine InstalledLocalCheckpoint. Do NOT call
  publish_ready on this branch.
- Consume that exact token through `begin_downgrade`, poll the terminal mirrored
  intent, then `commit_marker(intent)?.cleanup()`.

Keep each state machine and all file/connection owners alive through its I/O and
terminal handoff; drop completed intermediate state machines when their result
has taken ownership. No raw page/header patch, metadata-derived fake proof,
release/reacquire shortcut, or copied replacement database. Existing native
checks may report BUSY; do not retry SQL or evict readers. The operator must not
construct an Installed token for an interrupted/uncertified checkpoint: the
normal recovery bootstrap owner must first supply the proven reusable runtime.
If that integration is not ready, report the precise recovery-required state;
resume-downgrade handles only an already terminal downgrade intent.

## Validation sequence

1. Non-test Core/CLI causal test: current binary cannot perform the explicit
   operation; implement the interface and verify flags reject missing worker
   acknowledgement/incompatible modes BEFORE any file creation. Windows build
   plus unsupported-operation no-write check.
2. Actual CLI process upgrade and publish-new fixtures: materialized ordinary
   data/index/schema preserved, inode policy respected, marked database opens
   only via intentional Core option, ordinary/C URI still refuse activation.
3. Actual fresh downgrade with committed shared rows/index and IDs: drive the
   complete production checkpoint/install/intent chain above (no test-only
   helper construction), assert ordinary marker2, namespace/log removal, reopen
   through ordinary public Database/CLI and integrity_check. Confirm active
   peer transaction returns BUSY with no forced eviction/false success.
4. Reuse genuine subprocess crash fixtures via the exported Core/CLI operation
   entries for interrupted upgrade, terminal downgrade and partial cleanup.
   Preserve missing/swapped WAL and unknown-sidecar rejection. Require IPC and
   exact nonzero child counts; cleanup omission must fail restored-positive
   controls as already verified for private dispatcher237a.
5. Root combined six-PR facade core/SDK/C/PDO and native Linux gates remain
   required. No claim that PHP shared mode is enabled until actual PDO worker
   acceptance and intentional C option wiring are complete.

## Dependencies and ownership

Root's integrated facade004998ec8 contains the accepted lock/downgrade2d release;
root is merging accepted dispatcher237a. Implementation should branch from that
verified union to avoid rebasing old Database fields over DatabaseSharedState.
Agent11554 owns fresh normal recovered runtime/bootstrap; do not edit its proof
constructors. Agent11558 owns current-prefix/reload; use its certified runtime,
not a second proof interpretation. Proposed edits are a narrow Core public
wrapper/option entry, private offline orchestration child, and CLI argument/early
dispatch plumbing. Coordinate core/database.rs export/option hunks with root.
