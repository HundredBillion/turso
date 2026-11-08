# Private downgrade-intent preparation from installed checkpoint

Status: bounded implementation verified on Linux and Windows. This implements the approved
mirrored downgrade-fence scheme; it does not implement marker2 publication,
namespace removal, public operation dispatch or C/PDO activation.

## Source and ownership

`InstalledLocalCheckpoint` is created only after completed-manifest proof, ID
finalization, verified physical-base reading, retired logical truncation/sync and
local metadata installation. It owns the original authority, admission pause and
completed proof. Runtime replay remains absent until `publish_ready`. Neither a
raw identity nor a copied timestamp is an acceptable substitute.

Add a child module of `shared_checkpoint_install` so it can consume the existing
private fields without exposing setters or a general proof constructor. Factor
the existing `publish_ready` metadata/original-connection validation into a private
helper shared by that method and downgrade preparation; preserve ready behavior.
The new entry is conceptually:

`InstalledLocalCheckpoint::begin_downgrade(connection, OfflinePrerequisite)`
`  -> SharedDowngradePreparation::poll() -> MirroredDowngradeIntent`

It consumes the installed token and never calls `publish_ready`. Its opaque
source proof retains the real completed proof, original connection/schema/replay
identity, admission pause, and native offline files. Preparation reselects the
manifest under the **same** authority, checking exact target database/generation/
timestamp/empty prefix/salt, no live registrations and retired prior I/O.

## Native files and publication

Require `AllOlderWorkersStopped`; locking cannot prove that operator action.
Reuse the native existing-only opener and whole-file OFD lock on the actual main
inode. Verify marker254 and the runtime main identity. Open the existing basename
WAL without creation; require zero length, a distinct same-device native identity,
no symlink/nonregular path, and no alias to retained shared files. Retain both
handles, validate the ordinary-log sidecar is absent or valid header-only, and
reject an ordinary basename coordination sidecar. Revalidate paths/identities and
WAL emptiness before publication. No file is created, truncated or replaced.

As with the existing private offline recovery opener, use a forced SyscallIO
manifest handle whose native identity equals the runtime's retained manifest.
The installed runtime may use its existing supported backend; prior I/O must be
retired. Transition I/O always uses the owned synchronous alias. Physical I/O
therefore completes before each syscall returns; this avoids adding a new generic
resource-retention policy merely to preserve the native main lock across abandoned
asynchronous work. Existing manifest authority pins remain unchanged.

Give `DowngradeIntentPublication` a production constructor consuming the opaque
installed source proof and loaded manifest, while retaining its synthetic fixture
constructor under cfg(test). The source proof remains owned by publication and
then by its returned mirrored token, including main/WAL handles and admission
pause. It authorizes only normal empty completed target -> existing terminal v3
intent with actual basename WAL FileId. Preserve the mapped incarnation-ready
metadata check, but never publish Database/runtime Ready or the replay context.
Both slots must be FULL-synced and existing completion retirement satisfied before
return. On error/drop, replay stays absent and authority requires recovery; no
marker operation is possible. A partially published valid terminal intent remains
an admission fence under the existing selector rules.

## Verification before integration

1. Use the genuine completed-checkpoint fixture, finalizer, physical reader and
   local installer. A stub preparation entry must fail the positive test before
   implementation; never synthesize the completed proof or ID finalization.
2. Publish the real intent, independently decode both slots and compare exact
   target identity/WAL identity. Main marker/bytes, basename WAL and finalized ID
   files remain unchanged; only manifest intent publication is allowed.
3. While returned proof is held, native main exclusion and admission pause remain
   held. After it drops, new shared opens and previously idle participants reject
   the terminal intent rather than acknowledge reads/writes.
4. Wrong original connection, nonempty/replaced/aliased basename WAL and path
   identity changes reject before intent writes, preserving captured bytes.
5. Retain existing mirrored-publication short-I/O/error/retirement tests. The real
   native constructor is synchronous-only; do not claim asynchronous offline
   cancellation coverage from synthetic publication fixtures.

Marker transaction/checkpoint, interrupted-downgrade resumption and safe namespace
cleanup remain mandatory later stages. An absent basename WAL is an explicit
error without creation in this slice; safe creation and interrupted-resume handling
remain required for the complete operator. No ordinary-mode behavior is changed.

## Initial causal evidence

Native run 42487138081, source d82be999aa783d88e6856ed6cb523981d1c3f6b4,
reached the genuine installed-checkpoint constructor and failed exactly its
not-implemented stub (0 passed, 1 failed). No proof fields were fabricated.

## Verified implementation and review controls

Runtime source: `16fd0d6956e4d2a08815e35a5a9388b5a6f2fd9b`.
[Final native and Windows validation](https://github.com/HundredBillion/turso/actions/runs/42488827632)
pinned that exact source; every piped command used explicit Bash `-eo pipefail`.
Downloaded main result summaries show:

- `shared_downgrade_prepare_native_ --ignored`: 7 passed. These use genuine
  completed checkpoint, finalized IDs, physical read and installed local state.
  They verify mirrored intent/actual WAL identity, unchanged main and ID bytes,
  retained main OFD exclusion and admission pause, five invalid-sidecar cases,
  replacement between construction and poll, wrong original connection, a retired
  PlatformIO runtime, drop before publication, and manifest-backend substitution.
- Existing `shared_checkpoint_ready_native_ --ignored`: 4 passed;
  `shared_checkpoint_install_native_ --ignored`: 6 passed. The factored handoff
  validation preserves readiness and failure behavior.
- Existing `commit_manifest`: 32 passed, 4 ignored; the separately executed
  `commit_manifest_downgrade_native_ --ignored` terminal fence passed 1 test.
  That existing synthetic-publication fixture supplies the idle-participant
  admission check; the new genuine-proof fixture checks a fresh shared open.
- Strict Linux library Clippy (`conn_raw_api,bench`), formatting and diff checks
  passed. Windows core library `cargo check` passed; it is compilation coverage,
  not executable downgrade support on Windows, and retains baseline warnings.

Independent source review identified two additional requirements, both included:
platform-specific proof fields/imports have matching Unix configuration, and the
loaded manifest must belong to the exact retained synchronous `Arc<dyn File>`.
The latter is stronger than equal bytes or native identity alone. A genuine
installed source paired with a same-bytes memory manifest was accepted before
that guard: [causal run](https://github.com/HundredBillion/turso/actions/runs/42488497608),
source `e36afaf27f171e4541162a0952bc65b65622ad97`, failed the rejection assertion
exactly (0 passed, 1 failed). The final native run passes that control.

The earlier implementation run 42487977399 passed preparation 6, ready 4,
installer 6 and manifest 32; its workflow then stopped at a stale portable-count
assertion expecting 30. It was not a complete acceptance run. Subsequent alias
validation setup failures (short checkout ref; missing test trait import) were
corrected and are not counted as causal product evidence. Final broad protocol
checks require nonzero successes plus a successful Cargo exit, while the new
native selectors retain exact counts.

No public entry point, marker transaction, namespace cleanup, or ordinary-mode
behavior was added. The preparation token remains terminal and fail-closed until
the later marker/recovery stages are implemented. The operator prerequisite is
still required: file locking cannot evict legacy workers that already hold
uncoordinated handles. The completed slice deliberately rejects a missing
basename WAL without creating anything. Synthetic asynchronous publication tests
do not establish arbitrary-backend offline cancellation safety; production
transition I/O is restricted to the retained synchronous alias.
