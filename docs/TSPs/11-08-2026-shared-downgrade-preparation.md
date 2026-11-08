# Private downgrade-intent preparation from installed checkpoint

Status: proposed bounded slice for root review. This implements the approved
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
`  -> SharedDowngradePreparation::poll() -> PreparedSharedDowngrade`

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
No custom/asynchronous backend is accepted for this offline slice. Physical I/O
therefore completes before each syscall returns; this avoids adding a new generic
resource-retention policy merely to preserve the native main lock across abandoned
asynchronous work. Existing manifest authority pins remain unchanged.

Give `DowngradeIntentPublication` a production constructor consuming the opaque
installed source proof and loaded manifest, while retaining its synthetic fixture
constructor under cfg(test). The source proof remains owned by publication and
then by its returned mirrored token, including main/WAL handles and admission
pause. It authorizes only normal empty completed target -> existing terminal v3
intent with actual basename WAL FileId. Do not require or publish runtime Ready.
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
cleanup remain mandatory later stages. No ordinary-mode behavior is changed.
