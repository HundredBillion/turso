# PHP worker concurrency: upstream reuse

Reviewed 2026-11-06 against fork baseline `fc83c01c6faee96402d04201dbf856fec7690f6b`. This is the dependency record for [fork issue #7](https://github.com/HundredBillion/turso/issues/7). The initial survey read 321 open PR titles and bodies, then inspected the relevant diffs, comments, reviews and failed-job logs. No reviewed open PR supplies the full cross-process MVCC protocol.

## Repair/import candidates

| Upstream | Pinned head | Purpose | Findings to resolve before declaring ready |
| --- | --- | --- | --- |
| [#10117](https://github.com/tursodatabase/turso/pull/10117) | `0a32ad41bf7356d219398257b8027326d2780c10` | Keep page-1 allocation ownership across yielded I/O and recheck initialization under the lock. Discovered in PHP runtime concurrency. | Port to current pager; investigate close with a retained live statement and a delayed actual write. The failed JavaScript sync RSS assertion has not been proven unrelated; preserve its threshold. |
| [#11509](https://github.com/tursodatabase/turso/pull/11509) | `2033e0add3bc12fa2b8e4a952f8f1fd8e097861a` | Avoid concurrent startup probe conflicts and wait for coordination-file publication. | Its new mapped-header atomic use does not compile under Shuttle. Keep publication ordering and creator recovery semantics. |
| [#11508](https://github.com/tursodatabase/turso/pull/11508) | `74b1f8c62dc02f157aeebd271caebf34add2d2dd` | Give file-backed attached databases shared WAL coordination. | Unconditional inheritance breaks memory/TEMP/memory-VFS attachments. Normalize after URI and VFS resolution and retain readonly permission rules. |
| [#9360](https://github.com/tursodatabase/turso/pull/9360) | `8fa2ee20aee94b77583cde816ca85fcd45a49f32` | Reject shared database options the existing authority cannot provide. | Upstream draft, no reviews, successful observed CI. Independent scoped review found no introduced blocker. Preserve the fork's writable-activation checks on both registry paths. The underlying passive-VACUUM assertion is intentionally not fixed by this patch. |
| [#8968](https://github.com/tursodatabase/turso/pull/8968) | `1030a98d60637b3a6b68a683397afa8f61e7d47c` | Grow the shared WAL index before frames exceed its old capacity. | Rebase conflicts; historical Windows materialized-view test timeout; incompletely specified earlier MVCC/FTS mismatch reports. Exercise real subprocess growth, old readers, allocation failure, rollback and reopen. |

Original commits retain contributor authorship through cherry-pick provenance. Local repair commits must explain deviations from these pinned heads. Build/test results are recorded as execution proceeds; this initial record is not evidence that every listed item is fixed.

## Already present

Reader/cache/reopen changes from [#8476](https://github.com/tursodatabase/turso/pull/8476), [#8851](https://github.com/tursodatabase/turso/pull/8851), [#8967](https://github.com/tursodatabase/turso/pull/8967) and [#11114](https://github.com/tursodatabase/turso/pull/11114) are ancestors of the fork. Do not import them twice. Closed, unmerged [#9031](https://github.com/tursodatabase/turso/pull/9031) is not an implicit dependency.

## Adjacent work

[#9772](https://github.com/tursodatabase/turso/pull/9772) retries eligible MVCC autocommit statements within busy deadlines. It does not replay explicit transactions or supply interprocess coordination. [#10829](https://github.com/tursodatabase/turso/pull/10829) adds data-version polling but explicitly errors in MVCC mode. [#10715](https://github.com/tursodatabase/turso/pull/10715) and [#10872](https://github.com/tursodatabase/turso/pull/10872) improve MVCC cursor paths while leaving process-local transaction/log/checkpoint ownership in place. They are not required imports for this foundation.

## Reliability and full-protocol boundary

[Issue #11383](https://github.com/tursodatabase/turso/issues/11383) reports acknowledged commits lost and index corruption in experimental multiprocess WAL, including a two-process rolling-restart report. These reports have not been independently reproduced or repaired in this initial dependency survey. Startup, ATTACH and capacity fixes do not establish a fix for that durability problem.

The existing MVCC/multiprocess rejection must remain until a shared logical transaction protocol is implemented. A PHP configuration switch, physical WAL reader slots, or a global log offset alone cannot share transaction visibility, uniqueness validation, snapshot retention and checkpoint publication. The full PRD requires those contracts and real PHP worker, acknowledgement-ledger and crash/recovery evidence before the implementation PR leaves draft.
