# Complete an interrupted shared checkpoint without admitting a partial Store

Status: proposed private implementation seam for parent review. Builds on the approved interrupted physical-WAL recovery plan and the verified normal completed-checkpoint confirmation/finalization/reclamation work. Public mode remains disabled until integration and PHP acceptance pass.

## Outcome and invariants

Consume the actual `RecoveredIntentPhysicalBase`, preserve its original runtime, selected INTENT, original open configuration, publication lease and admission pause, and finish the acknowledged checkpoint. An ACK physical base needs real target-ID preparation and durable completed records. A SOURCE physical base additionally needs one actual private Store, bounded replay of the acknowledged logical prefix and actual materialization. Both paths must eventually enter the verified normal completion chain and produce a usable Database containing every acknowledged row.

Never manufacture a Normal record for a replay API, infer a completed proof from a timestamp, clear mapped recovery to make an ordinary helper work, reset the Runtime's permanent Store claim, or construct a second Store after SOURCE replay. No changes to the disk format or the approved public policy are proposed.

Source references inspected in the normal-bootstrap worktree:

- `shared_store_claim.rs`: `Free -> Reserved -> Attached`; generic attachment and fresh reclamation reserve only a Free Runtime. Attached is permanent.
- `shared_checkpoint_recovery_reclaim.rs`: `begin_log_reclamation` currently always reserves a fresh Store before truncating the retained logical log.
- `shared_checkpoint_recovery_bootstrap.rs`: `prepare_store` constructs from that reservation; the final evidence already verifies actual Store/Runtime/config/Database identity, schema, target frontiers, original connection, retired I/O and the real completed proof before exposing replay and releasing admission.
- `shared_refresh.rs`: ordinary refresh rejects INTENT, validates the bounded log salt/CRC/endpoint/timestamps and imports prepared frames; its tail recovers mapped state. The new path must reuse frame mechanics without calling that ordinary tail.
- `checkpoint_state_machine.rs`: normal shared authorization uses `validate_oracle_identity`, and its normal materialized proof checks `guard.published()`. Both must remain strict. The actual materializer already supports retaining the logical log and FULL/TRUNCATE.
- `shared_ids/checkpoint_prepare.rs`: ordinary preparation requires `guard.published()` and finalized source selectors. It cannot be called on an unrecovered intent by silently recovering the guard.
- `checkpoint_publication.rs`: ordinary completion likewise requires published source-prefix authority plus live materialization and prepared-ID evidence. A separate sealed recovery adapter is necessary.

## Ownership and interfaces

### Common intent owner

`RecoveredIntentPhysicalBase` is the only entry. Its physical parts choose SOURCE or ACK by exact values already verified against the real INTENT; no caller-supplied timestamp/config replacement is accepted. Integrate `Arc<RecoveryOpenConfig>` once from the original options and carry the same Arc throughout. Fresh manifest selections must use the actual retained manifest Arc/native identity and the original lease. Actual main/WAL/layout evidence must be revalidated before each mutation and after yielded I/O. Content-readonly recovery remains a clear error.

### SOURCE-only Store owner

Introduce an opaque, non-Clone `IntentRecoveryStore` in the recovery child. Its constructor consumes a genuine fresh `SharedStoreReservation` while the original intent is held, constructs exactly one `Arc<MvStore>` using the actual config allocator and runtime storage, and records that exact Store, Runtime, config, source identity and publication lease. No constructor accepts a generic existing Store. No setter converts Attached back to Free or relabels an ordinary Store as recovery-owned.

The owner is carried through replay, materialization, real Normal completion, fresh normal selection, physical/ID confirmation, ID finalization, log reclamation and final local installation. While the owner exists, runtime replay stays None; no public Database/connection escapes. Every resume checks actual Store pointer, Runtime pointer, config Arc, lease, no local transactions and actual selected intent/completed identity. All owner/state fields and explicit abort paths destroy private source/materialization connections, pagers and Store references before the original guard and pause. Backend operations retain the publication/physical ownership needed through their actual retirement; late callbacks must not reopen admission. Dropping a failed owner is fail-closed; asynchronous backend completion keeps its authority until retirement. A retry that cannot retain that exact owner reconstructs through a fresh Runtime after the old owner and I/O have gone, rather than resetting Attached.

### Disjoint reclamation continuation

Carry a private `RecoveryStoreContinuation` through the normal recovery owners:

- `Fresh`: the existing normal/ACK path. `begin_log_reclamation` retains its present `reserve_store` requirement and stale/preexisting-Store rejection.
- `Owned(IntentRecoveryStore)`: constructible only by the sealed SOURCE path. At reclamation, validate its exact Store/Runtime/config/lease and completed target instead of reserving another Store. At final bootstrap, extract the same actual Store instead of calling `construct_store`.

This enum is not a public flag and has no conversion from an arbitrary Arc<Store>. Ordinary normal selection initializes Fresh. Only the genuine intent completion owner can attach Owned to a fresh selection of the real completed records. The actual guard/pause/physical parts remain continuously owned while this continuation moves through existing confirmation states.

Before final Ready, close and destroy the private source/materialization connections and pagers, verify their outstanding I/O is retired and no local transaction remains, then construct the final Database around the same Store. Capture and validate the actual Store frontier and source map/schema generations before preparing the final reload: replay/materialization may already have advanced them to ACK. Never substitute an assumed SOURCE frontier or reset Attached. Install the real completed physical maps/schema, reset/certify the final actual pager, reconcile the actual reclaimed append offset/CRC/header, and use the normal final evidence checks. Native callbacks must retain the original ownership during cancellation. Public admission is last, after `guard.finish()` succeeds; finish error withdraws replay as in the existing chain.

## Task 1: seal the SOURCE Store and continuation

Blocked by: verified physical crash-class milestone and integrated normal-bootstrap source base.

1. Add a genuine SOURCE crash fixture calling the intended complete-recovery API; observe the explicit missing continuation/factory failure before adding behavior.
2. Implement `IntentRecoveryStore` from a consumed fresh reservation and the original physical owner/config. Add the private continuation to the normal owner chain without changing Fresh behavior.
3. Test actual pointer identity across every ownership handoff, second-reservation rejection, wrong Runtime/lease/config/Store rejection before mutation, no transactions/replay while held, and backend retirement on cancellation. Existing generic attached-Store rejection must remain green.
4. Do not reclaim any log yet. Commit and independently review the ownership seam.

## Task 2: bounded real INTENT replay

Blocked by: Task1.

1. First inspect the actual source ID selectors/banks through a sealed read-only intent operation under the unrecovered lease. Capture the actual source floor/owner-epoch/allocation facts; seed private allocation bounds conservatively before replay/materialization can allocate. Reject incompatible source/prepared identities without mutation. This inspection yields facts, not normal readiness or mutation permission. Bind the actual logical file with the retained intent lease and callback-retirement ownership. Use the actual selected `log_salt`, committed endpoint, running CRC and final timestamp; parse every frame only within that acknowledged prefix.
2. Factor or reuse existing header/frame preparation/import mechanics. A distinct sealed intent reader/driver owns its recovery context and never returns LoadedManifest or invokes ordinary refresh's mapped-recovery/admission tail.
3. ACK path validates the retained prefix without constructing a Store; frames at/below the actual materialized ACK cutoff are not applied. SOURCE path seeds the one actual Store/context from the actual source physical parts and imports newer frames through existing prepared-frame installation. At completion require exact final ACK timestamp, CRC and endpoint, no local transaction, correct schema/sequence descriptors and private visible frontiers, with the same actual source identity.
4. Add altered salt/CRC/endpoint, truncated acknowledged bytes, invalid schema/sequence frame and beyond-endpoint controls. Add cancellation after a partially installed frame and retry/reopen with the same retained acknowledged boundary. Verify no prefix is published, no logical bytes reclaimed and no second Store is constructed.

## Task 3: actual SOURCE materialization and ACK receipt

Blocked by: Task2.

1. Construct a private writable materialization Database/connection around the owned Store and exact original config/retained storage; retire the earlier read-only physical staging connection first. This is a new private constructor, never read-only promotion or a public open.
2. Add a sealed intent-recovery authorization to the existing checkpoint materializer. It validates the actual recovered SOURCE owner, fully checked ACK replay, same Store/connection/pager/runtime/native files, original lease, global native quiescence, local no-transactions and FULL/TRUNCATE. Ordinary `authorize_shared_materialization` and ordinary proof requirements remain unchanged.
3. Drive the actual existing write-set/materializer and pager durability operations. Produce a distinct recovered-materialization receipt only from its actual completed state, FULL main sync, empty physical WAL, fresh actual ACK main parse/floor and retired I/O under the original intent. ACK physical output can produce the corresponding receipt only from the already verified actual copy/sync result plus fresh validation; no generic timestamp-only constructor.
4. Suppress a required user-page backfill as a causal control; actual SQL ACK-row checks must fail. Inject errors/cancel across actual writes, main sync, WAL truncate/sync and retirement; a fresh recovery must preserve the original logical ACK. No completed proof/Ready yet.

## Task 4: real target IDs and both durable completed records

Blocked by: Task3.

1. Add a separate sealed intent-ID operation. Under the unrecovered authority, load the actual active-old source selectors/banks using source generation and source timestamp encoded in the real intent, with its ACK prefix. Preserve source references, floor and captured maximum owner epoch. Reject unrelated prepared targets; reuse only an exactly matching prepared target or prepare the actual intent target bank with the verified physical floor. Do not call ordinary preparation through an early `recover`.
2. Reuse the existing bank/selector encoding, mirrors, FULL-sync and retirement mechanics. The recovered materialization receipt and actual intent lease authorize this operation; no public unchecked source identity input.
3. Only after the actual target bank and both selector witnesses are durable, publish two genuine Normal completed records with proper sequence and FULL sync under the same guard. A private recovery-completion adapter may reuse publication mechanics, but ordinary Normal/source-published guards remain strict. Do not produce a CompletedCheckpointProof before these real operations.
4. Release only temporary bindings, keep original guard/pause and Store continuation, freshly select the actual completed manifest, then enter normal physical inspection -> target IDs -> two-witness confirmation -> epoch/finalization. Normal recovery proof construction remains the authority for completed proof. Add active-old/prepared/both-Normal crash boundaries, wrong lease/native file/target/floor rejection and per-mirror cancellation/retirement tests.

## Task 5: reclaim, reuse the owned Store and admit last

Blocked by: Task4.

1. Extend normal reclamation/bootstrap only through the sealed continuation. Fresh path remains exactly reserved-fresh. Owned path verifies the real completed identity and same Store, then performs the existing actual logical truncate/FULL-sync/retirement and append-state adoption/reset without allocating another Store.
2. Drop all private source connection/pager ownership; prepare/install the actual completed generation into the same Store, preserve monotonic allocation clock/IDs and original dialect/extensions/allocators, build final Database/connection, and complete existing pager/schema/frontier/lease checks.
3. Positive native acceptance: each genuine committed old/partial/new state and every precommit/header/torn class, both slot parities, returns a Database with the original ACK value and can commit a new update; fresh writable and content-readonly processes see it. SOURCE and ACK paths both eventually work without a caller-forced preliminary SELECT. No interim public replay or admission.
4. Required negative/control checks: ordinary already-attached Store still rejected by Fresh path; removing Owned Store identity check fails; constructing a second Store fails; releasing before private pager/physical callback retirement admits no peer; original readonly/config mismatch cannot write; finish error clears replay; retained log corruption never yields Ready.
5. Run targeted native cohorts first, then the final integrated core/C/PDO/Windows/worker acceptance and source review required for PR8. Do not label the issue complete at the private-capability milestone.

## Review focus

The parent reviews the typed owner/continuation before behavior. The normal-bootstrap owner reviews only the continuation additions to finalization/reclaim/bootstrap; SOURCE replay/materializer and intent-ID adapters have separate source reviews. Check every error/drop path for continuous authority and backend lifetime, same Store identity, no fake Normal/mapped recovery, durable target-ID/manifest ordering and admission last. The full implementation must demonstrate actual user data, not only matching metadata.
