# Private shared-MVCC file transitions

This component implements the approved offline transition boundary. It does not
expose a new URI, enable shared transactions, or claim full engine upgrade
acceptance. `mode_transition.rs` is private to MVCC. Its marker is raw read/write
version 254; the ordinary `Version` enum continues to reject it.

An explicit upgrade requires the operator to stop **every older worker**,
including unlocked read-only handles. An exclusive database lock cannot discover
or revoke those handles. Ordinary shared-mode opens of existing ordinary files
must reject the request without changing the file. Existing marked files must
have a valid manifest for their inode; opening cannot initialize missing state.

The offline backend owns exclusive database/protocol ownership and quiescence.
Its synchronous methods must finish recovery/materialization and durability,
or a pager-mediated marker transaction and complete checkpoint. They return only
after actual I/O retirement, holding ownership through errors and cancellation.
They are not ordinary transaction or async poll entry points. Concrete live
MvStore admission and quiescent-checkpoint integration remains separate work.

New-file preparation occurs at a private staging pathname. The final pathname is
bound to its canonical parent before publication. A manifest, empty logical log,
and coordination file live under `.turso-mvcc-{device:016x}-{inode:016x}` in that
parent. Same-parent hardlink aliases therefore resolve the same namespace;
aliases in other parents cannot invent a missing namespace. Unknown versions,
wrong identities, absent manifests and nonmaterialized initial log prefixes fail.

Publication synchronizes each sidecar, the main inode, the sidecar directory and
its parent before creating the final name with a non-overwriting hard link.
Linux publication links the retained inode through `/proc/self/fd/<fd>` with
`linkat(AT_SYMLINK_FOLLOW)`, so replacing the staging pathname cannot substitute
another inode. This requires procfs; unavailable procfs fails closed. The Linux
[linkat manual](https://man7.org/linux/man-pages/man2/link.2.html) documents this
unprivileged alternative to `AT_EMPTY_PATH`. The component synchronizes the parent
again before removing the staging name. A competing
creator wins or loses atomically; it is never replaced. The final pathname never
exposes an empty or ordinary-format database. Crash before linking leaves no new
final pathname; crash after linking leaves a complete marked image and recovery
state, with the link's persistence subject to directory synchronization.

Failure after the final link is created is an ambiguous publication result, not
rollback. In particular, a staging-name removal or directory-sync error may be
returned after the final database is durable. No failure path deletes that final
name or its sidecars. The caller must inspect the final inode and marker before
retrying and must not overwrite it. Staging names belong exclusively to their
creator; cleanup must not race publication.

The current old-binary probe uses the preserved optimized CLI built from
`eda0afbc4` (production runtime equal to `4ccf547a4`), SHA256
`7f178cb517da8f7171f0a2ba8d8ba45007108ddb36f7dc4f40389aa70c0cfe5f`.
It rejects a synthetic 254/254 fixture in ordinary read-write, read-only and
multiprocess-WAL modes, with unchanged main bytes. The ordinary-format control
reads the original row. This verifies marker exclusion only: raw fixture edits
are not an upgrade/downgrade implementation.

Initial namespace preparation now uses the existing shared-authority constructor
on Linux and the actual `MappedSharedWalCoordination::create_or_open` constructor
for `physical-tshm` with 64 reader slots. `physical-wal` starts empty. Their files
are synchronized before publishing the marker. The offline layer accepts the
configured history limit explicitly; it does not define a public default.
The publisher synchronizes all five namespace files. Runtime opens must use the
non-creating strict coordination open and route physical WAL access into this
namespace; existence checks alone do not prevent an unlink/recreate race.

The pager test backend exercises ordinary WAL transactions and checkpoints; it
is not the final MvStore quiescence/materialization implementation. Linux-only
checks cover real initial authority construction. The earlier portable publication tests exposed a staging-path replacement race;
the corrected descriptor-based publisher and its process-death tests are Linux-only.
macOS checks cover real pager marker writes, format exclusion and rejection of
ordinary shared-mode opens, not Linux publication or authority correctness. Process-kill checks
retain the host page cache and are not simulated power-loss/fsync-failure tests.

The backend must route marked physical WAL/checkpoint operations into the identity
namespace. Downgrade must leave a fully synchronized ordinary main image with no
stale basename WAL that an old executable could replay. Namespace cleanup follows
that durability proof and completed marker checkpoint. The private test backend
uses ordinary WAL only and does not establish this final runtime routing contract.

## Verification at the component milestone

Local Rust 1.88 checks passed for refusal of implicit upgrade, rejection of a
nonmaterialized manifest, and rejection of nonempty physical WAL. The preserved
old CLI also rejected the real pager-produced marked image in all three tested
open modes. Formatting and diff whitespace checks passed. These local checks run
on macOS and do not establish the Linux publication implementation.

Behavioral failures were observed before fixes for the missing publisher,
nonmaterialized logical boundary, missing authority, physical WAL presence and
staging-path replacement. The latter test exposed publication of the wrong inode
and motivated descriptor-based linking. Native Linux tests remain required for
the final descriptor link, initial authority construction, same-parent aliases,
four process-kill publication points, offline retry/downgrade and the live
WAL-only user-row regression. No public enablement follows from this milestone.

Run `cargo test -p turso_core --features conn_raw_api mode_transition_` on Linux.
The two ignored old-executable tests additionally require `TURSO_OLD_CLI` pointing
to an independently built, provenance-recorded older Linux CLI; run the same
filter with `-- --ignored --nocapture`. Keep the older executable outside the
candidate build target so a rebuild cannot silently replace the control.

## Existing authority files

`SharedMvccAuthority::open_existing` opens atomically without `Create`. It rejects
missing files, every length other than 4096 bytes, uninitialized headers and
incompatible database identities/versions/configurations. It never repairs or
recreates their format. A live mapping whose pathname was unlinked cannot cause
a second authority inode to be initialized by this entry point.

A valid authority still uses its lifetime locks: if no participants survive,
reopening advances the epoch, clears abandoned registrations and requires recovery.
That valid-state bookkeeping is distinct from resetting an invalid file. Runtime
callers must retain the returned authority through their use; a path-existence
precheck followed by the creating constructor is not equivalent.

The Linux unlink regression has a test-only negative control. Set
`TURSO_AUTHORITY_LEGACY_CREATE_CONTROL=1` and run the exact filter
`authority_existing_only_does_not_recreate_unlinked_live_mapping`: the unchanged
creating opener must make the assertion fail. Run again without that environment
variable to test the existing-only constructor. The remaining authority tests
cover missing/short/uninitialized/incompatible state, unchanged bytes/inode,
live contention and required recovery after participant close or process death.
Native Linux execution of this followup is pending at commit time.

### Hosted component validation

[Linux run 42258321658](https://github.com/HundredBillion/turso/actions/runs/42258321658)
validated mode source `eb235884d` plus `89ab458f7` in union revision `cd05dd0ad`.
All 14 selected mode checks passed (including the subprocess entry point), with
two older-executable checks explicitly ignored pending a separately built Linux
control. This includes the descriptor/path-replacement regression, four SIGKILL
publication boundaries, real authority/physical coordination initialization,
offline pager transition/retry, and committed WAL-only row rejection before
checkpoint. The same run passed 7 authority, 8 committed-prefix and 15 manifest
checks. These are component results; the public shared transaction protocol is
still disabled, and real MvStore offline materialization is not supplied by the
test backend.

## Retained runtime files

The private `SharedMvccFiles::open_existing` component accepts a retained main
file and its expected native identity, a canonical path, native I/O, the history
limit, and a read-only-content flag. It opens all sidecars without `Create`,
retains their actual handles, and compares each with a native descriptor identity.
The main marker and manifest size must validate before coordination admission.
Physical coordination and logical authority use their strict existing-only
constructors; missing or invalid state cannot fall back to basename files.
The returned handles are not a recovered snapshot: later admission must load and
validate the committed prefix while holding the required authority. Reading manifest
checksums without that guard could observe slots across concurrent publications;
the constructor deliberately does not interpret their mutable bytes.

Initial filesystem support is the ext family identified by Linux `fstatfs` magic
`0xef53`; that magic does not distinguish ext2, ext3 and ext4. Every actual native
file descriptor is checked, and all sidecars must share the database's device.
Unknown filesystems, FUSE, network types, overlay and tmpfs are rejected pending
validation. Existing coordination constructors exercise actual OFD locks and
shared mappings. This is stricter than a platform-only capability boolean or the
ordinary WAL network-filesystem exclusion list. The native Linux fixtures print
their detected filesystem magic so validation records the environment actually
used.

Read-only participants receive read-only logical-log and physical-WAL handles.
Main-file read-only opening and custom durable-storage rejection remain caller
preconditions. Manifest, authority and physical coordination must be writable
metadata; unavailable permission fails explicitly. Opening reads/validates content
and may perform valid coordination epoch/recovery bookkeeping, but never repairs,
truncates, creates or writes database/WAL/log content. Namespace symlinks and
nonregular sidecar files are rejected. Capability and identity checks precede
coordination metadata changes.

Native tests cover retained handle identity, read-only write rejection and content
hash preservation, every missing sidecar, live authority unlink, wrong main
identity, invalid manifest length, coordination permission denial and implicit-upgrade
refusal. Local checks cover unsupported backend rejection and the filesystem
policy. Native execution is pending for this new component.
