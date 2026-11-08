# Offline shared-to-WAL marker transition and recovery

Status: root approved the seam; genuine live marker regression is the first implementation gate.
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
