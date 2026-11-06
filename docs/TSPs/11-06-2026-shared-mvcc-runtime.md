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
