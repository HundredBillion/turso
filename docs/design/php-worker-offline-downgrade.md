# Private offline shared-MVCC downgrade proposal

Status: source-grounded protocol proposal for the already approved explicit offline downgrade. This document does not enable a public command or supply the unfinished checkpoint coordinator. The target is raw marker **2 (ordinary WAL)**, matching `OfflineModeChange::downgrade`; it is not ordinary MVCC marker255.

The operator must stop every older worker, including idle or unlocked readers. Native main-file whole-file OFD exclusion, the shared admission pause, and publication authority are additionally retained throughout the transition. These locks do not evict preexisting unlocked handles.

## Existing seams and the missing proof

`core/mvcc/mode_transition.rs::downgrade` currently trusts a generic backend's returned timestamp if it is at least the selected manifest timestamp. Its pager test backend demonstrates ordinary marker/checkpoint ordering, not materialization of real shared history. The concrete ordinary backend only publishes marker254, and the private operation dispatcher has no downgrade operation. Keep that boundary until the real proof exists.

The coordinator owner distinguishes two proofs:

- `CompletedCheckpointProof` is planned to certify both FULL-synced target manifest slots, retired backend completions, the exact target generation/timestamp, and an empty committed log prefix.
- The later owned `QuiescentMaterializedDatabase` must additionally retain publication authority and the admission pause after ID finalization, logical/physical reclamation, local generation reload, and a fresh zero-live-registration check. This final token is the downgrade input.

A raw `RecoveryIdentity`, a decoded manifest, a timestamp comparison, or the first proof alone is insufficient. The adapter must not construct the final token from these values. Its production constructor belongs to the checkpoint coordinator. Explicit fake proof providers are permitted only in contract tests and are not native materialization evidence.

The final token must bind the retained main identity, selected manifest sequence, database/log generation, materialized timestamp, and empty prefix. It also carries the relevant retained files and ownership through errors, cancellation, and backend completion retirement. No ordinary connection or shared transaction may be returned from this state.

## Why a durable downgrade fence is necessary

`SharedMvccFiles::open_existing` selects identity-directory physical WAL storage. It does not interpret an ordinary basename WAL as a mode-transition fence. If an offline process committed marker2 into a basename WAL while main still contained254 and then died, the OFD locks and admission pause would be released. Another shared worker could otherwise ignore that marker WAL and acknowledge new shared log writes. Finishing the old downgrade later could lose those acknowledgments.

The transition therefore needs a durable, explicit **downgrade intent** before any marker write. Every shared open and every fresh shared admission must reject that state, including admission through an already open idle handle. A cached generation/prefix equality check cannot bypass the intent. Existing transactions are absent before intent publication because the final checkpoint token proves quiescence.

Three approaches were considered:

| Approach | Assessment |
| --- | --- |
| Timestamp check followed by marker write | Rejected: no actual materialization proof and no post-crash admission fence. |
| New standalone namespace intent file | Possible, but adds creation/directory durability and a second mutable authority for mode selection. It still needs explicit open/admission checks and recovery identity binding. |
| Distinct mirrored manifest downgrade-intent state | Recommended: reuse the retained authority, two-slot publication, completion pins, and existing manifest load at admission. Give this state its own semantics; do not encode downgrade as an ordinary checkpoint intent. |

The manifest owner controls the concrete codec change. The intent must bind main FileId, the exact completed checkpoint identity, target mode2, and the verified basename WAL FileId. Both slots must independently contain a valid intent, be FULL-synced, and have their backend completions retired before marker I/O starts. Unknown/corrupt states fail closed; readers must not fall back to an older ordinary manifest merely because they do not recognize a checksummed mode-intent record. Codec compatibility tests must establish that property. No public format compatibility promise is introduced by this private proposal.

## Adapter boundary

The proposed production entry consumes the owner's final opaque token plus an explicit offline prerequisite. It does not acquire a second snapshot, release/reacquire authority, run another materialization algorithm, or accept caller-supplied timestamps as proof.

Conceptual ownership flow:

```text
QuiescentMaterializedDatabase
  -> verified ordinary WAL/log identities
  -> MirroredDowngradeIntent (both slots durable and retired)
  -> private ordinary-WAL marker connection
  -> durable marker2 checkpoint, connection fully dropped
  -> identity-safe namespace/old-log cleanup
  -> release retained ownership
```

The exact Rust methods will be agreed with the coordinator owner when the final proof type exists. A synchronous native adapter is preferable to adding another asynchronous state machine: internally created SyscallIO, retained proof Arc on its database/open state, existing-only main/WAL handles, and explicit identity checks. This mirrors the bounded upgrade recovery opener but is a distinct authorization type and mode policy.

Production ownership should remain separated:

- `database/checkpoint_state_machine.rs` and the shared coordinator construct the final materialization token.
- `commit_manifest.rs` and its publication child implement the distinct intent codec/publication and provide the durable intent token.
- A new `mode_transition/downgrade.rs` owns marker publication, explicit offline recovery, and cleanup sequencing.
- `shared_files.rs`, shared refresh/admission, and manifest loading reject intent for ordinary shared work.
- `database.rs` contains the narrowly proof-authorized raw opener, if needed; ordinary/public opening behavior remains unchanged.
- `mode_transition/operation.rs` gains no executable downgrade variant until all real proof, fence, recovery, and native checks pass.

## Ordered protocol

1. Retain native whole-file main exclusion and strict native file identities. Pause shared admissions, drain registrations under the coordinator's existing bounded protocol, and obtain `QuiescentMaterializedDatabase`. Do not hold publication authority while waiting for existing writers to finish; the coordinator already owns that ordering.
2. Validate or create exclusively the ordinary basename WAL and synchronize any newly created pathname before publishing its identity. It must contain no unrelated history. Reject symlinks, aliases, foreign devices, missing required files, stale WALs, and nonempty ordinary logical history. An ordinary logical log may be absent, empty, or an exactly validated header-only file; retain its identity for cleanup. Creation must never overwrite an existing file.
3. Publish and retire both mirrored downgrade-intent slots under the retained authority. No main/header/WAL marker bytes may be changed before this step completes. An uncertain intent sync result requires explicit recovery; it is not a rollback permission.
4. Open the materialized image through a private WAL-only connection. It may interpret254/2 only under the retained downgrade proof. Use the actual basename WAL FD whose identity is bound by the intent. Do not use the shared physical WAL, attach the shared MvStore, replay an old shared prefix, or expose this connection to ordinary SQL callers.
5. Validate the effective base schema and exact materialized metadata at the premarker boundary. Prepare a header-only pager transaction: `BEGIN IMMEDIATE`, change both raw version bytes to2, `COMMIT` with FULL synchronization. The marker transaction must not modify user/schema pages or run metadata-creating bootstrap code. Inspect its WAL frame page IDs; only page1 may be introduced by this transaction. Use a freshly parsed materialized schema, not a connection whose logical-root schema survived an in-memory mode demotion.
6. Complete the ordinary WAL checkpoint, synchronize the main file, and verify the effective and persisted marker2. Require the transition WAL to be fully backfilled/truncated and all backend I/O retired. Drop the private connection and its storage handles while keeping the outer proof/native lease.
7. Only now remove captured header-only ordinary logical-log artifacts that would make a normal WAL opener reject the file. Remove the obsolete identity namespace using retained identities and an explicit ordered cleanup plan. Keep the durable intent discoverable until the other obsolete members have been handled, then synchronize the directory/parent after removal. Never delete a replacement inode or newly added history. If cleanup fails, report that ordinary-mode publication is durable and cleanup remains; do not present the error as a rolled-back downgrade.
8. Release authority/admission/native ownership last. Normal ordinary WAL opens may then use the materialized image. Ordinary WAL behavior applies after this explicit transition; shared-mode reservation semantics are not silently promised across a later downgrade/re-upgrade cycle.

As with fresh staging cleanup, unlink-by-name is not atomic conditional unlink-by-inode against uncooperative replacement after verification. Offline names remain operator-owned. The implementation must prevalidate the complete cleanup set before deleting its first member and retain the necessary file descriptors.

## Crash recovery and admission matrix

| Durable boundary at process death | Normal shared behavior | Explicit offline recovery |
| --- | --- | --- |
| No durable intent; no marker writes issued | Existing shared recovery remains valid. | Re-run the coordinator and acquire a new final token; never infer the old in-memory proof survived. |
| Intent durable; marker transaction absent or uncommitted | Refuse open/admission with recovery-required error. | Retain/recover authority and pause, validate the intent and exact checkpoint/FD identities, then finish the marker2 transition. Keep intent fenced throughout. |
| Marker2 committed in basename WAL; main still254 | Refuse open/admission. | Recover only the verified ordinary WAL, confirm the committed marker-only transition, then checkpoint and synchronize main. Never import a newer shared prefix over the intent's checkpoint. |
| Main marker2 durable; namespace still exists | Shared mode rejects marker2. | Treat shared namespace as obsolete cleanup state. Ordinary WAL recovery is authoritative; never replay old shared logical history. |
| Namespace partly or fully removed | Shared mode rejects marker2. | Preserve ordinary data and perform only cleanup whose retained identities/proof can still be established. Missing evidence is not authorization to delete arbitrary files. |

The premarker recovery constructor must rebuild ownership from durable intent, not fabricate the original checkpoint token. It validates that the selected completed checkpoint and physical/log/ID state match the intent before permitting marker completion. Both normal shared opens and fresh admissions remain fenced while it works.

After a crash with durable main marker2, ordinary applications may have committed new work before another offline command is run. Recovery must preserve that work. It must not demand that the ordinary WAL still contain only the original page1 marker transaction and then replace it with old shared data. The marker-only restriction belongs to the transition being written and to the premarker254 recovery case. Once main2 is established, recover the ordinary WAL under normal durability semantics and limit shared-side work to identity-safe cleanup. Loss of the original materialized metadata through legitimate later ordinary DDL is not permission to restore stale shared metadata.

## Checks required before executable downgrade

Contract tests can be implemented first with an explicitly fake backend to verify ownership/order, cancellation behavior, and refusal to run marker/cleanup steps before the required proof. They must not be described as evidence that shared data was materialized.

The real integration gates are:

- A live writer or reader prevents token construction; existing writers can finish while admissions are paused. No marker or intent content changes occur on bounded contention.
- Matching or higher timestamps with the wrong main identity, generation, manifest sequence, prefix, ID-finalization state, or outstanding I/O do not authorize downgrade.
- Every shared open and fresh admission path, including idle handles, rejects a valid downgrade intent. Deliberately bypassing that guard makes a native multi-process control fail.
- One torn/corrupt intent slot cannot expose a normal old state. Short writes, failed sync, aborted logical completions, and retained backend completions never permit premature marker writes or ownership release.
- Real acknowledged rows, indexes, custom-type metadata, deletes, row-ID/sequence state, and schema survive full shared materialization and ordinary WAL reopening. This requires the real coordinator, not the pager fixture.
- Process kills before intent, after each intent sync, after marker COMMIT, during/after marker checkpoint, and during namespace cleanup preserve acknowledged data and select the intended recovery branch. Native process-kill tests are distinct from torn-write/power-loss injection.
- Shared admission cannot acknowledge anything after durable intent. A hypothetical attempt to bypass this fence must fail the control before downgrade is accepted.
- New ordinary commits made after durable main2 survive cleanup recovery; neither their WAL nor their schema is overwritten from the obsolete shared namespace.
- Replaced WAL/log/namespace paths and invalid header-only logs are preserved and rejected. Cleanup errors explicitly report already-durable ordinary-mode publication.

The current generic timestamp-only downgrade test must remain labeled as a pager fixture. It is not a production adapter and must not be wired into the explicit operation dispatcher. Replace that generic production seam with the final owned-proof API when the coordinator can construct it; do not weaken the proof to make a scaffold executable sooner.
