# Shared MVCC runtime integration

> Execute inline using the existing approved PRD and live replay plan. Private components do not enable public shared-mode opens.

Source: `docs/PRDs/11-06-2026-independent-php-worker-concurrency.md` and `docs/design/php-worker-mvcc-protocol.md`.

## Invariants and interfaces

- Ordinary version 1/2/255 opens and their storage activation remain unchanged. A private shared option accepts only an already marked version-254 database. Existing ordinary files require the approved offline transition; fresh files are initialized privately before atomic publication.
- Shared opens retain the verified identity-derived main/logical/manifest/physical WAL/coordination handles. Reject unsupported storage, filesystem, codecs and platforms. Read-only participants retain read-only content handles while coordination remains writable. Never create a missing published sidecar or fall back to basename WAL files.
- Admission takes the shared admission gate before publication authority. It validates the durable manifest under authority, imports its complete prefix, then registers the globally issued snapshot and inserts the local transaction before releasing authority and admission gate. A peer that reserved a commit timestamp but has not published or aborted prevents admission. Existing transactions retain their registrations until commit/rollback cleanup.
- An acknowledged zero-length logical prefix has no header: do not inspect a physical tail. The first nonempty durable prefix establishes its header salt from the predecessor's zero salt; later same-generation salt changes remain corruption. Nonempty prefixes use strict ending checksum validation.
- Bootstrap may build a fresh store, but live refresh uses the atomic installer and retains pending prepared frames on failure. Preserve the local replay context across yields and cancellation; a frame consumed from the reader cannot be skipped after failed installation. Retain generation protection throughout each read/import operation.
- The local clock advances monotonically when observing a peer commit. Shared snapshots and commits always receive authority timestamps; there is no local timestamp fallback. Exhaustion fails before transaction insertion or timestamp packing.
- A commit acquires shared authority and imports the latest prefix before entering Preparing or validating writes. Existing group commit cannot independently publish a process-local batch in this mode. Local transaction execution remains concurrent; only publication is serialized.
- Shared content writes require the active authority's I/O lease. Backend completions retain opaque lease pins through physical retirement, including caller abort/drop. Logical-log commit writes bind only the logical file; checkpoint writes also bind main and physical WAL. Short writes fail before manifest publication.
- Log serialization checks the retained-history limit before issuing bytes. Oversize frames fail before append; a full retained log returns bounded BUSY without aborting existing readers. Successful log write alone is not irrevocable: shared `log_appended` is set only after durable manifest publication. Force durable log sync before manifest write and manifest sync before mapped publication/acknowledgement.
- Checkpoint pause excludes new admissions while existing writers can still acquire publication authority. Poll registrations with a bounded deadline, without holding publication authority while waiting for active writers. Only an empty registration set permits materialization/reclamation. The pause token alone is not proof of quiescence.

## Task 1: Private open and bootstrap

Add private options/state and retain the validated existing-file bundle. Route the physical WAL and logical storage to their retained identity files. Accept marker 254 only with the private shared option; ordinary opens still reject it. Keep the public C URI/capability disabled.

Bootstrap under captured authority and a validated durable manifest. Build schema/base metadata from durable storage, recover only the acknowledged prefix, preserve the resulting replay context, and avoid content writes during read-only bootstrap. Initial empty-prefix opening must ignore complete, corrupt, and torn unacknowledged physical tails. Missing state, incompatible options and unsupported storage must leave files unchanged.

## Task 2: Yielding refresh and transaction admission

Create a driver that owns the admission permit, authority/loaded manifest, replay cursor and pending prepared frame across yields. Complete refresh before registering a snapshot; retain registration until actual transaction insertion. Route fresh begins and special inner MVCC transactions through the same protocol, and retain existing registrations on read-to-write upgrades. Add result/admission conflict checks so invalidated writers cannot emit more rows or commit.

Tests: delayed log reads and cancellation, old snapshots across peer commits, retry after allocation failure, and an independent peer paused between timestamp reservation and publication while another process attempts admission. No new snapshot may be issued during that interval. Cover exhausted authority timestamps without creating a local transaction.

## Task 3: Commit and recovery publication

Connect shared ownership before Initial, import before conflict validation, and supply the reserved global commit timestamp to the local Preparing publication. Serialize/log-limit-check, append, fully sync, publish the next manifest record, publish the mapped boundary, finalize local versions and release ownership. Defer the existing irreversible-log marker until manifest durability. Aborted unacknowledged suffixes remain outside the committed prefix, and pending writes retain ownership until physical retirement.

Tests: disjoint and conflicting independent writers; delayed append/sync/manifest operations; state drop at each boundary; synced external acknowledgement ledger and fresh-process recovery. Validate all acknowledged frames; tolerate an unacknowledged successor only if its referenced prefix was already durable and passes strict validation.

## Remaining required integration

Global row/table/sequence reservations, checkpoint/reclamation and interrupted-checkpoint recovery, the concrete offline backend adapter, fresh-file initialization, the public PDO URI/capability path, and the complete independent PHP-worker acceptance suite remain required. No task above is equivalent to completion of issue #7.

## Restart incarnation recovery

Mapped authority bytes are coordination state, not a durable incarnation ledger. A machine failure can restore an older epoch and timestamp while acknowledged ID reservations survive. The behavioral regression restores those older bytes after all handles close and checks that the acknowledged owner cannot be issued again.

Every exclusive lifetime acquisition clears incarnation readiness. No snapshot can register until bootstrap has recovered the manifest and logical prefix, loaded the matching durable ID journal, and reconciled the epoch. The journal maximum includes retired owner records and owners in every active or prepared bank retained by the selected witness. Reconciliation requires no live registrations and chooses an epoch strictly above that maximum, with checked overflow. A private protocol revision excludes older participants that do not enforce readiness.

Before using this maximum, bootstrap must write and FULL-sync the selected ID witness to both selector slots and wait for physical I/O retirement. This matters after interrupted compaction: one newer floor-only selector may contain no owners while an older valid selector still names their journal. Merely reading the newer selector could permit identity reuse followed by later fallback to those old owners. Mirroring eliminates that fallback before admission. Completed quiescent compaction can then forget old owner identities because neither durable selector can select them.

Cancellation or failure before the mirrored proof leaves admission disabled and recovery required. Successful reconciliation changes only coordination metadata; read-only database contents remain unchanged. Subsequent processes joining a live reconciled authority do not change its incarnation. The proof assumes the existing failure model of checksum-detected torn writes and successful FULL-sync durability; it does not treat process-kill tests as power-loss evidence.

Every bootstrap validates the selected ID journal, including processes joining an already reconciled live authority. Readiness only controls incarnation reconciliation; it is not a substitute for durable metadata validation. A prepared checkpoint generation requires coordinator recovery before admission, even when the completed manifest is already durable and an idle participant kept the authority alive across the coordinator's death. Matching finalized state on a live incarnation needs no selector writes.

## Overlapping schema publication audit (2026-11-08)

Issue7 requires coordinated schema publication; this audit found no source-backed
requirement for a separate global DDL-exclusive lock. The existing commit path
refreshes peers under publication authority before validating the transaction.
Peer schema installation advances `last_committed_schema_change_ts`; preparation
rejects a write whose snapshot predates that timestamp, including a transaction
that changed its own schema header.

Test-only source `f448a526c9b7c367c272bc2870b43c34a4d59724`, based on composite
`2049aa8457f1368c9b14b50ecfa2b2bc98039d62`, adds native coverage in
`core/vdbe/shared_schema_publication_tests.rs`. Three parent tests cover16 cases:
CREATE TABLE and CREATE SEQUENCE, same versus different names, both commit orders,
and independent stores versus real independent worker processes. Both workers
acknowledge BEGIN, a snapshot read and staged CREATE before either may commit.
Table cases also stage distinct marker data. The controller records each actual
COMMIT acknowledgement without SQL retry and verifies exact catalog counts, no
duplicate names, acknowledged data/sequence start values, and rejected-object
absence through existing peers, a fresh peer and reopen. Worker replies and exit
handling have bounded deadlines.

Run https://github.com/HundredBillion/turso/actions/runs/42482021571 passed all
three tests:16 first-commit acknowledgements and16 `SchemaConflict` responses for
the second commit. This includes different-name DDL: current validation is
conservative rather than merging concurrent schema transactions. First table-only
run42481656583 independently passed8 cases.

A validation-only control disabled **only** the `schema_updated` rejection in
commit preparation. Both different-name commits then acknowledged, but a later
catalog/data verification failed with `no such table: created_b` (0passed/1failed).
Restoring the exact source passed all3 parent tests again; strict library Clippy
passed and the workflow confirmed a clean source tree. The control demonstrates
the existing check's necessity; it is not a defect present in the tested product
source. Artifact `shared-schema-publication-audit` retains logs, source identity,
toolchain and the one-line control diff.

No engine behavior, ordinary-mode path or public activation changed. This is
bounded CREATE/publication evidence, not coverage of every ALTER/DROP/index DDL,
DDL crash recovery, arbitrary I/O failure, or the complete PHP release gate.

## Independent-worker DML and API audit (2026-11-08)

Existing `shared_commit_native_` tests already covered separate stores, overlapping
updates, stable/fresh readers and reopening acknowledged writes. The existing
process harness covered sequence-owner liveness and schema publication. Neither
was evidence for real-process delete/secondary-UNIQUE conflicts or an external
ACK ledger across multi-row rolling restarts, so the new cases reuse the schema
worker protocol rather than introducing another service or retry layer.

Exact test source `4c4cbb97bec4444c00d15c08a41aca42ca823834` passed Linux native
run https://github.com/HundredBillion/turso/actions/runs/42484506488:

- Update/update, delete/update, and secondary-UNIQUE conflicts, each in both worker
  orders. Both processes first hold explicit snapshots. Updates reject the losing
  write with `Busy` during staging. Delete/update and UNIQUE cases stage both
  writes, then reject the losing COMMIT with `WriteWriteConflict`. UNIQUE cases
  use distinct primary keys; the shared secondary key is the conflict. A fresh
  process verifies exactly the accepted state and `PRAGMA integrity_check='ok'`.
- Disjoint updates, a delete and an insert stage in two processes before either
  commits, and both commits succeed in either order. Fresh-process rows and
  integrity checks match the independent expected state.
- Three FULL multi-row commits transfer10 between two balances and append one
  transfer record in the same transaction. A surviving controller fsyncs an
  external ACK record only after COMMIT replies. A long-lived reader retains
  `[100,100]` and zero transfer rows throughout; fresh workers observe each
  acknowledged version. Workers exit/reopen, including asserted SIGKILL after
  the second acknowledged commit. A final new process verifies `[70,130]`, exact
  transfer IDs1–3, total30, and integrity. Main-file bytes remain unchanged,
  keeping this test within one physical generation.

All5 parent tests and strict library Clippy passed. Artifact
`shared-worker-acceptance-audit` records source/toolchain/logs. The first tracer
run42483728194 passed the ACK/restart case but incorrectly required both updating
workers to stage successfully; its `Busy` result was an allowed early conflict,
not a product failure. Phase-specific tests in42484118213 confirmed that behavior
before the disjoint-write/integrity controls were added. No engine code changed.

C API source audit: `sqlite3_step` drives the current core statement once through
`run_one_step_blocking`; Busy and WriteWriteConflict map to SQLITE_BUSY, while
SchemaConflict maps to SQLITE_SCHEMA. `sqlite3_exec` stops/finalizes at the first
error and has no transaction replay loop. Core schema reprepare handles
SchemaUpdated, not SchemaConflict, and the busy handler resumes execution state.
These are source findings, not new-mode PHP end-to-end evidence. One separate
mapping inconsistency remains: core/open handling maps BusySnapshot to busy,
while C's step error mapper falls through to SQLITE_ERROR. Its producer found in
this audit is ordinary WAL snapshot upgrade; no actual shared-mode counterexample
or C behavior change is claimed here.

Public C/PDO opt-in remains rejected. These process tests do not establish PHP
acceptance, power-loss/torn-write recovery, every partial-statement error path,
checkpoint-generation transitions, or a fix for the legacy-WAL report11383.
