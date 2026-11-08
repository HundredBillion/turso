# Offline shared-to-WAL marker transition and recovery

Status: live marker slice verified; missing-WAL creation, interrupted resume and
cleanup remain unimplemented. Root approved the staged seam.
Base: verified preparation6640d5b8642625b9f63a2d56a24e0cc910d353bf,
whose runtime16fd0d695 passed native/Windows42488827632.

## Existing seams and constraints

- `ordinary.rs::commit_marker_checkpoint_sync` already demonstrates the required
  mechanism: actual `BEGIN IMMEDIATE`, `Pager::with_header_mut`, `COMMIT` with FULL
  synchronous mode, truncate checkpoint and main sync. It handles upgrades only;
  it must not be broadened to accept a timestamp-only downgrade assertion.
- `Database::open_offline_marker_recovery` opens the same native main inode with
  forced SyscallIO and NoLock under an Arc-retained private proof, bypassing the
  ordinary registry. It checks actual main/WAL identities. Its version exception
  accepts only255/254 and its recovery proof requires an INITIAL generation1
  namespace. That proof cannot authorize a later-generation downgrade.
- `InstalledLocalCheckpoint::begin_downgrade` and the mirrored terminal v3 record
  now retain genuine materialization/ID-finalization proof, the same authority,
  admission pause, original connection metadata and whole-file main OFD lock.
  They leave runtime replay absent. This is the only in-process starting point.
- The old generic `OfflineModeChange::downgrade` accepts only a timestamp bound
  and uses generic name-based removal. The concrete operation dispatcher does not
  expose it. Do not route the new implementation through that weak proof path.
- The manifest's terminal intent records the actual basename WAL FileId. Both
  copies must be durable before a marker transaction. A selected terminal intent
  must never be converted back to a normal shared record.

## Proposed typed opener and transaction

Introduce a separate private `PreparedDowngradeMarker` capability, held by Arc in
both OpenDbAsyncState and Database exactly as the upgrade-recovery proof is held.
Keep the upgrade proof/checks intact. A small internal enum may share the raw
opener plumbing, but must dispatch proof-specific validation and allowed versions;
no public options boolean or arbitrary marker whitelist.

The live constructor consumes `MirroredDowngradeIntent` with its real installed
source, original guard/pause and retained files. Synthetic tokens have no source
and must be rejected. It checks both slots equal the intended terminal record,
zero registrations, retired prior I/O, unchanged path identities and empty logs.
No guard release/reacquire or Ready publication occurs.

The private raw opener uses owned Syscall aliases, actual main identity and the
recorded basename WAL identity. Feed the retained WAL Arc directly to the WAL
scanner; ordinary OpenWal adds Create and therefore cannot provide the required
no-recreation guarantee before its post-open identity check. For this capability only, raw254 or2 decode as
ordinary WAL; it must never initialize an MVCC store, shared replay or namespace.
The proof stays alive through all raw connection/storage drop paths. Opening the
same main through the normal registry is not permitted.

Before mutation validate the effective WAL-visible header and physical metadata:
- read/write versions agree and are254 or2;
- only page1 may occur in the ordinary WAL frame cache;
- the singleton persisted MVCC timestamp exactly equals the terminal target;
- database/page layout and physical roots agree with the materialized target;
- shared logical/physical WAL history is empty and selected IDs are finalized for
  that exact generation, with no prepared selector and a covering logical floor.

If effective version254, execute BEGIN IMMEDIATE and modify only the version
fields to2 through `with_header_mut`; COMMIT under synchronous FULL. Do not issue
PRAGMA journal_mode (which could add another header transition). If effective
version2, do not run another marker transaction. In either case finish a truncate
checkpoint, fully sync main, close/drop raw connection/storage, then revalidate
main marker2, actual main/WAL identities, empty ordinary WAL and terminal manifest.
A phase-specific revalidator is necessary: the preparation's current revalidate
requires marker254 and empty WAL, which intentionally ceases to hold after COMMIT.
It must not be silently relaxed for pre-publication callers.

## Missing basename WAL before intent

Extend ONLY the genuine installed-proof preparation path to create a missing
basename WAL with create_new, mode0600 and no-follow, under the retained main
OFD lock and checkpoint admission/authority ownership. Validate the namespace,
ordinary-log aliases, parent directory and all existing identities first. Validate
and retain the new native inode; sync the empty file and parent directory before
recording its identity in terminal intent. Existing files remain existing-only,
empty and identity checked. Never truncate, replace or overwrite them.

A crash or error after creation leaves the empty WAL intact; removing it could
invalidate a durable but uncertain terminal intent. A new attempt can reuse the
same verified empty inode. This supersedes the first slice's explicit missing-WAL
error only when the genuine installed capability is available. Recovery of an
already published terminal intent must never recreate a missing recorded WAL.

## Fresh interrupted recovery

Add an explicit private `ResumeDowngrade` operation; do not infer it from a failed
ordinary/shared open. Require AllOlderWorkersStopped and obtain native whole-file
main exclusion before inspecting sidecars. Main marker may be254 or2. Capture and
retain canonical directory/main/WAL identities, then select terminal manifest for
that main inode and actual basename WAL. No initial-namespace assumptions apply.
Acquire existing admission pause and authority, reject live registrations, and
perform read-only physical/ID validation before any map recovery or mutation.
Do not set incarnation/runtime Ready merely to use the ordinary ID loader.

Dependency: review11554's `SharedIds::inspect_completed_target(guard, identity,
physical_floor)` returns `UnrecoveredIds` without requiring guard.published or
publishing readiness. It accepts an empty same-generation target after our own
terminal proof. The normal `InspectedRecoveryBase` wrapper rejects terminal records
and must not be bypassed by fabricating a normal manifest. Add a narrow read-only
`has_pending_checkpoint` accessor (owner-coordinated) because source_database=None
alone does not exclude a future prepared reference. Stable helper ancestor is
6680b79c6; current owner branch a15fabb1a includes further finalization work.

A single surviving selected terminal copy can be mirrored under retained ownership
before the marker transaction; no fresh sequence, target or timestamp is invented.
The persisted terminal record is the restart certificate, not a synthesized live
CompletedCheckpointProof. Main254/WAL2 and main2/WAL2 both recover through the raw
ordinary WAL path; main2 recovery never replays the former shared logical prefix.
Any different WAL inode, unexpected page, target timestamp, ID generation/pending
selector, path alias or live owner fails closed without rewriting the marker.

## Cleanup after durable marker2

Cleanup is a distinct phase after main marker2 is checkpointed/synced and the raw
connection/storage have dropped. Retain main exclusion, native identities and the
terminal intent fence throughout. Prevalidate every owned path and reject unknown
namespace entries before removing anything. Revalidate each identity before
unlink. Also remove the retained valid header-only ordinary .db-log before the
terminal manifest: normal WAL opens reject a nonempty logical-log sidecar. Include
a real header-only-log ordinary-reopen regression. Follow the existing operator-owned-path assumption (not claiming
protection against a hostile replacement after the final check).

Do not call the current generic remove_namespace: it deletes the manifest first,
which destroys restart evidence while other files still remain. Remove validated
non-manifest files first; sync the namespace. Remove the terminal manifest last,
sync the namespace, remove the now-empty directory, then sync its parent. The
basename WAL remains as the ordinary database WAL; never remove the main inode.

Partial-cleanup recovery is a separate marker2-only path: terminal manifest remains
until all other known files are gone. It may tolerate missing removed sidecars
without recreating them or reconstituting a Ready shared runtime. Main lock,
marker2, empty/matching ordinary WAL, target physical metadata and surviving
terminal manifest still bind the cleanup to this database. Missing authority/IDs
in this phase cannot be used to authorize another marker transaction. If the last
manifest unlink happened before process death, only an already-empty expected
namespace directory may be removed; unknown residual files fail closed. A missing
namespace with no terminal certificate is not proof of a new downgrade operation.

No namespace removal occurs on uncertain marker/checkpoint outcome. Errors after
durable marker2 report that the mode changed but cleanup remains pending, avoiding
misleading retry semantics. Preserve the original error and fail closed on any
identity mismatch; never restore the shared marker or rebuild its namespace.

## Test-first sequence

1. Genuine installed->mirrored->marker stub must fail the real positive test.
   Implement live marker first; same main FileId, FULL WAL commit, ordinary fresh
   reopen, catalog/index/custom type/sequence and data checks, integrity_check.
2. Missing WAL creation controls: actual file absent before prepare, durable inode
   recorded, no overwrite on symlink/alias/existing nonempty; creation failure
   produces no marker/manifest writes. Recovery missing recorded WAL rejects.
3. Real subprocess exits after mirrored intent, after marker COMMIT before main
   checkpoint, and after checkpoint before cleanup. External acknowledged data
   remains the independent oracle; fresh resume must preserve it and same inode.
   Persisted terminal record must fence new shared opens at every pre-cleanup point.
4. Invalid resume controls for swapped WAL/main, malformed/disagreeing intent,
   unrelated WAL pages, wrong timestamp/root floor/ID generation, prepared IDs and
   live registrations. Capture all durable bytes before expected rejection.
5. Interrupt cleanup after each unlink and each directory-sync boundary; fresh
   marker2-only cleanup must complete without replay, file recreation or deleting
   unknown/replaced files. An error after publication must have accurate status.
6. Restore/run existing upgrade, prepared-marker recovery, downgrade preparation,
   manifest admission-fence and ordinary WAL tests; native Linux and Windows
   compilation gates remain explicit. Public C/PDO shared-mode rejection remains.

Root approved the concrete seam, beginning with the live marker transaction
before broader resume and cleanup implementation. No public operation/API or executable downgrade acceptance is claimed yet.

## Verified live-marker milestone

Runtime `b7c6161b8682e1daa7b0dfcbfb04556542825481` implements only the live,
254-to2 marker step from the genuine installed/mirrored capability. It does not
accept a freshly recovered2 marker or perform namespace/ordinary-log cleanup.
The returned token retains the original authority/admission pause and native
main exclusion. No public dispatcher/URI was enabled.

[Native and Windows run42492367153](https://github.com/HundredBillion/turso/actions/runs/42492367153)
pinned that source and passed: new live marker3; previous preparation7, ready4,
installer6; ordinary transition/recovery36 with3 ignored helpers; manifest32 with4
ignored; separately executed terminal fence1; strict Linux library Clippy and
formatting; Windows core library compilation. The positive uses a real pager WAL
transaction and truncate checkpoint, checks unchanged main inode and terminal
namespace, then reopens ordinary WAL and verifies acknowledged payload and
integrity after dropping the returned token. It is not a crash/recovery test.

The genuine stub failed first at the intended missing marker operation:
[run42491635402](https://github.com/HundredBillion/turso/actions/runs/42491635402),
source `b5257eec5a461586f9366bef3496cc84cb76abc0`, 0 passed/1 failed. No completed
checkpoint fields were manufactured for this fixture.

The installer now retains the already validated physical header page size,
reserved-space and declared page count, plus main byte length, into the opaque
installed source. Before BEGIN the raw opener's actual header and current byte
length must match. The prior negative logical-floor bound alone does not establish
exact extent. Native controls change the declared count or append a physical page
after genuine intent; both reject without marker/WAL/namespace mutation. Missing
or replaced recorded WAL likewise rejects without recreation.

[Single-check removal control42492820507](https://github.com/HundredBillion/turso/actions/runs/42492820507)
removed only the exact-layout validation call from that same source. The appended
page was then wrongly accepted and the rejection assertion failed (0 passed,
1 failed). Restoring the exact file and checking a clean diff reran all3 marker
parents successfully. This control does not disable the floor/metadata/provenance
validators and does not infer causality from an unrelated failure.

Remaining gates are the stages above: actual interrupted-process recovery, safe
missing-WAL creation before intent, header-only ordinary-log cleanup and reopen,
manifest-last partial-cleanup recovery, and the operator lifecycle. In particular,
old same-process main descriptors can remain owned by external runtime Arcs after
the capability drops; their later closure and ordinary POSIX-lock exclusion need
an explicit lifecycle test before operator exposure. This is an unverified source
concern, not a new policy requirement or a claim that the tested marker failed.

## Approved continuation interface

Root approved the continuation after the verified live marker slice. The private
capability will distinguish Live(MirroredDowngradeIntent) from a separately sealed
RecoveredTerminal source. RecoveredTerminal validates the selected terminal
record, actual main/WAL identities, physical metadata and read-only finalized IDs
with no pending selector; it does not require or fabricate normal published/Ready
mapped state. It retains the same continuously held authority and admission pause.
Only then may a surviving terminal copy be mirrored and an effective254 marker be
committed; effective2 follows checkpoint/recovery without another marker commit.
Partial-cleanup mode is separate, marker2-only, and cannot enter marker/replay or
recreate IDs. Missing-WAL creation remains genuine-Live-only and durable before
intent. A bounded real child-lock probe runs before any policy choice for old
same-process descriptors. Dependencyfa225 was merged without production conflicts;
review11554 transferred the narrow pending-selector accessor hunk to this task.

## Continuation evidence

Missing-WAL creation source `f267be5c9` passed Linux native creation2,
preparation7 and marker3 plus strict library lint/formatting in
[run42503599577](https://github.com/HundredBillion/turso/actions/runs/42503599577).
The genuine desired operation first failed at the explicit missing-WAL restriction
(0 passed/1 failed) in run42502871275. Invalid ordinary log/coordination files are
rejected before creation. A WAL missing after terminal publication still fails
without recreation. Process-death coverage of creation is still pending.

The actual old-handle lifecycle probe passed in
[run42503446343](https://github.com/HundredBillion/turso/actions/runs/42503446343),
source `36dfcb9ba`. Weak references prove the old shared Database, runtime and
native main handle retired before the second independent child attempted its
nonblocking ordinary RW lock; the child remained excluded. This bounded path did
not reproduce the hypothesized close-any-inode lock loss, so it does not justify
an additional operator lifetime policy. It is not evidence for arbitrary legacy
handle interactions or multi-process permission enforcement.

Fresh-terminal resume source `d47369981` has an intentional stub. Its genuine
live checkpoint publishes terminal intent, drops the live source and then asks
for fresh resume. Run42503731841 observed the intended missing-operation failure
(0 passed/1 failed). The replacement implementation is under verification and
must not be treated as a completed downgrade until recovery and cleanup fault
controls pass.

### Subprocess evidence correction

The earlier old-handle probe result42503446343 is **not accepted lock evidence**:
its `--exact` child selector retained the `turso_core::` crate prefix, while the
Rust test harness exposes names without that prefix. A zero-selected child could
exit successfully. Source3351513 strips the prefix in both new downgrade child
controllers and requires the lock child's actual one-test successful summary.
Source71f7 adds a deliberate child failure control that the supervisor must reject.
The initial actual-crash suite failed because its child exited before IPC readiness;
it did not establish a protocol failure. Corrected child-count/IPC runs are pending.
Other helpers and unrelated evidence are not invalidated by this local finding.

### Approved cleanup-only view

Partial cleanup has its own capability, not a marker-capable or fabricated ID
proof. It requires native main exclusion, actual ordinary-WAL header and aligned
physical extent, an empty identity-matched basename WAL, and the surviving
terminal manifest's main/WAL identities. A separate forced-Syscall read-only raw
view retains this capability and checks the materialized singleton timestamp
against the terminal record before any unlink. No writable marker API, runtime
Ready publication, ID reconstruction, or old-log replay is available. Close the
actual view before unlinking; FULL-sync main first because observed marker bytes
after process death alone do not establish durable completion. If terminal
manifest is already gone, only an empty expected namespace and absent ordinary
log qualify for idempotent directory removal. Unknown entries fail before unlink.

The recovered mirror must refresh durability even when both slot bytes already
match. Root review identified that observed matching bytes may still belong to an
interrupted writer. Genuine recovery with matching slots first failed the added
barrier assertion (0 versus2) in run42504814020. Source49f1 records the actual
selected slot under continuous ownership and writes/FULL-syncs the opposite copy
before writing/FULL-syncing the selected copy, without advancing record/sequence.
This preserves the selected valid record if the first operation fails.

Completed-token cleanup source26f48 passed native2/2 plus strict library lint and
formatting in run42505270402: actual ordinary-MVCC-generated header-only log is
removed before ordinary reopen, and unknown/replaced namespace files reject
before any unlink. This is not partial-cleanup or process-death acceptance.

## Confirmed ordinary lock lifetime defect and bounded prototype

Corrected source273a in run42506332370 executes the actual child: the ordinary
owner excludes it BEFORE old native-handle retirement, then the child acquires
the lock AFTER the old Database/runtime/main aliases retire. The deliberate child
failure control observes its exact failure diagnostic and one failed child before
proving the supervisor rejects it. The earlier zero-selection conclusion is
retracted, not treated as evidence against this defect.

Both UnixFile and UringFile currently acquire process-scoped F_SETLK and explicitly
unlock in Drop. Closing a different descriptor for the same inode can therefore
release the live ordinary owner's lock. No Unix per-inode deferred-close registry
exists in this source. A bounded prototype routes both backends through one helper:
Linux64 uses nonblocking whole-file F_OFD_SETLK, including unlock; other Unix keeps
the existing process-lock implementation. NoLock, ReadOnly and the explicit
lock-disable environment behavior remain unchanged. Unsupported Linux64 OFD
requests fail rather than silently restoring the unsafe process-lock ownership.

A distinct raw same-process ordinary open now conflicts, matching exclusive
ownership; process-wide Database cache reuse still shares the existing owner.
Validation must cover actual Python SQLite POSIX locks in both cross-process
directions, native/NoLock/read-only aliases closing, hardlink cached RW/RO callers,
ordinary-owner SIGKILL release, the real downgrade regression, and optional Uring
execution when the kernel actually permits it. Classic external SQLite ownership
inside the same process is not claimed safe against arbitrary unrelated file
closures; the existing explicit offline prerequisite still requires older workers
to stop. No new user lifetime policy is being substituted for the defect fix.

Partial-cleanup candidate cd5e4ec61 passed native run42506877040: final parent
summaries were partial4, extent1, resume4, completed-cleanup2, missing-WAL2,
preparation7 and marker4 (the known failing old-handle-close case was explicitly
excluded). Partial coverage includes real SIGKILL at ten cleanup boundaries,
read-only-view mutation rejection, stale target metadata and unexpected remaining
files rejected before unlink. Strict Linux library lint/format and Windows check
also passed. Downloaded logs are retained under
`.local/issue-resolution/downgrade-partial-cd5` in the root checkout. These results
do not clear the separately confirmed ordinary lock lifetime defect.

The OFD prototype also changes ordinary native Linux64 Syscall/Uring opens on
kernels older than 3.15
or filesystems/kernels that reject OFD locking: those opens now fail with the
existing locking-error result instead of silently using process-scoped locks.
This affects ordinary mode, not only the already-OFD-dependent shared mode.
ReadOnly/NoLock still do not acquire this ordinary lock. Linux64 Drop relies on
closing the descriptor, with no extra unlock syscall, so unsupported acquisition
returns a normal locking error rather than panicking while dropping the failed
handle. A child-only seccomp fault returns EOPNOTSUPP for the actual OFD syscall
to test that error path; another test retains a duplicated descriptor to require
ownership until the final native descriptor closes. The final compatibility decision must account for that older-kernel
tradeoff. Non-Linux Unix and Linux32 retain the existing implementation. We do not
add a fallback that would reintroduce the demonstrated alias-close loss.

### Broader ordinary ownership validation remains a gate

Run 42508495510 verified the original ordinary-lock two-parent suite, actual Uring
parent, and all five marker parents (including the corrected old-handle-close
counterexample). Its test-lint step failed on native fixture warnings; it was not
a wholly green release. Source 2f455 subsequently removes Linux64 explicit Drop
unlocking. A disposable child denies the actual OFD syscall with EOPNOTSUPP and
checks a normal open error plus safe NoLock/read-only destruction. Restoring the
old Drop produced the required failure; restoring the candidate passed. A second
test retains a duplicated descriptor to keep ownership until its final close.

The first full ordinary run 42509335725 was **not accepted**: 2785 passed, 115
failed, 173 ignored. Existing restart fixtures evicted the registry and reopened
before releasing their old Database; several raw WAL-coordination fixtures also
requested two exclusive ordinary handles instead of explicit cooperating handles.
The bounded first repair fda7b28 drops the fixture's old owner before reopen,
uses the normal resolver for codec cache tests, and sets NoLock only in eight
explicit shared-WAL coordinator fixtures. Run 42510713011 then passed 57 codec tests,
16 shared-WAL tests and 41 SDK tests, with 487 MVCC tests passing and 14 still failing. Each remaining
failure was traced to a retained closed connection, completed state machine,
pager/store, or artifact fixture owner. Source 62a62c explicitly retires those
objects. Run 42511287821 passed 2899 core tests with one remaining failure: the
logical-log fuzz fixture retained the previous pager across restart and used it
with the new store. Source 2d826 corrects that fixture to obtain the new
connection's pager. Exact-source run 42512200110 passed the full core suite
with conn_raw_api,bench,io_uring (2902 passed, zero failed, 174 ignored), SDK41,
actual native ordinary ownership5 (including Uring), marker5, strict Linux
library/tests Clippy and formatting, plus Windows compilation. The marker log
includes the deliberately failing child used to verify supervisor rejection; its
final parent summary is five passed and zero failed. Root's combined six-PR
integration remains a separate gate. No production lock was weakened and no test
copies/replaces the database to avoid exclusion.

Linux test-lint cleanup is separately released as 6ada01704 (tree-identical to
15196c7, validated by run 42510494682). All non-harness warnings are fixed. Rust 1.88's
large_stack_frames lint reproduces on a standalone crate with 3076 empty tests,
allocating 541416 bytes for the generated libtest harness. The exception is scoped
to cfg(test); the workspace threshold and non-test denial remain unchanged. An
injected oversized non-test engine function was rejected at its actual core/lib.rs
span, and restored production lint passed. This explicitly relaxes that one lint
for test builds, not for production engine builds.

The strengthened exclusion applies to independently opened raw native handles
in the same process. High-level cached Database opens still share their existing
owner and are tested separately by the SDK and cached RW/RO/hardlink controls.
The change does not promise compatibility with ordinary locked opens on systems
without OFD support. NoLock/ReadOnly behavior is not used as a blanket escape
for restart tests: previous owners must actually retire before a new owner opens.

### Explicit downgrade recovery dispatch

The private operation selector adds ResumeDowngradeMarker and
ResumeDowngradeCleanup. Marker recovery consumes the completed marker capability
through cleanup before returning success; cleanup recovery accepts only the
existing marker2-only capability. Neither operation creates fresh downgrade
authority from a pathname. Source-proof rejection keeps its original diagnostic;
errors after entering marker completion report that ordinary WAL may already be
published and recovery or cleanup remains incomplete. The existing actual
three-phase marker SIGKILL and ten-phase cleanup SIGKILL tests exercise these
operation entries, with namespace/log removal and ordinary payload reopening as
postconditions. Exact-source run 42512918309 passed native resume4/partial4/cleanup2/creation2/extent1/marker5, strict Linux library/tests lint and Windows compilation. Removing cleanup from each dispatcher arm caused its actual SIGKILL parent to fail (exit101, zero passed/one failed); restored marker and partial-cleanup parents passed. These are process-death tests, not power-loss simulation.
