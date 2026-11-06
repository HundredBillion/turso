# PHP worker concurrency: upstream reuse

Reviewed 2026-11-06 against fork baseline `fc83c01c6faee96402d04201dbf856fec7690f6b`. This is the dependency record for [fork issue #7](https://github.com/HundredBillion/turso/issues/7). The initial survey read 321 open PR titles and bodies, then inspected the relevant diffs, comments, reviews and failed-job logs. No reviewed open PR supplies the full cross-process MVCC protocol.

## Imported prerequisites and local repairs

| Upstream | Pinned head | Purpose | Repairs and verified boundary |
| --- | --- | --- | --- |
| [#10117](https://github.com/tursodatabase/turso/pull/10117) | `0a32ad41bf7356d219398257b8027326d2780c10` | Keep page-1 allocation ownership across yielded I/O and recheck initialization under the lock. Discovered in PHP runtime concurrency. | Ported to the current pager. A delayed actual write after close reproduced loss of an acknowledged table; close now drains initialization I/O before releasing ownership. The unchanged JavaScript RSS check also failed on the pre-import baseline; eliminating an owned-blob copy and temporary base64 string brings the combined branch below its original limit. |
| [#11509](https://github.com/tursodatabase/turso/pull/11509) | `2033e0add3bc12fa2b8e4a952f8f1fd8e097861a` | Avoid concurrent startup probe conflicts and wait for coordination-file publication. | Replaced the instrumented-atomic overlay of persisted bytes with native mapped atomics while preserving release/acquire publication. Actual Shuttle-profile and native coordination runs pass. |
| [#11508](https://github.com/tursodatabase/turso/pull/11508) | `74b1f8c62dc02f157aeebd271caebf34add2d2dd` | Give file-backed attached databases shared WAL coordination. | Normalize inherited coordination after URI and VFS resolution. Preserve private memory/TEMP behavior, existing named-memory VFS semantics and read-only permissions. Windows validation exposed a sibling filesystem bug: both Windows backends now reject read-only creation without creating a file. |
| [#9360](https://github.com/tursodatabase/turso/pull/9360) | `8fa2ee20aee94b77583cde816ca85fcd45a49f32` | Reject shared database options the existing authority cannot provide. | Imported after independent scoped review; retain the fork's writable-activation checks on both registry paths. Registry option coverage passes. The underlying passive-VACUUM assertion is outside this patch; no fix is claimed. |
| [#8968](https://github.com/tursodatabase/turso/pull/8968) | `1030a98d60637b3a6b68a683397afa8f61e7d47c` | Grow the shared WAL index before frames exceed its old capacity. | Resolved conflicts while preserving newer deferred mapping and reader/reopen behavior. Actual independent-reader growth beyond the old index capacity passes on Linux and Windows. The previously timed-out materialized-view case also passes on both. Historical incompletely specified MVCC/FTS mismatches remain unproven, rather than declared fixed. |

Original commits retain contributor authorship through cherry-pick provenance. Local repairs are separate commits. The combined source was reviewed independently after integration.

## Verification of the prerequisite milestone

The Linux and Windows [focused validation run](https://github.com/HundredBillion/turso/actions/runs/42176860230) passed against runtime source `408e17949`. Its workflow override exists only on the separate validation branch and is not part of this feature. Coverage includes shared coordination, real independent processes, initialization/close, file permissions, registry options, the reported Windows timeout case, large shared-index growth and sync ownership. Windows multiprocess helpers explicitly use IOCP; unsupported backend cases remain explicit skips rather than evidence for those backends.

Local combined checks also covered actual Shuttle scheduling, attachment/checkpoint/MVCC/header/autovacuum behavior, C compatibility against Turso and stock SQLite, and PHP callback, mixed-open, module CRUD and read-only file-preservation probes. These checks exercise existing modes; they do not establish cross-process MVCC support.

With Node 24.22.2, matched fresh-process debug builds failed the unchanged 16 MiB blob RSS budget before the memory repairs (about 144 MiB growth) and passed after them (113–123 MiB across four runs). The body size and six-times-body limit were unchanged. This is local debug-build evidence, not an optimized production benchmark. The full JavaScript sync file also passed with its existing skips.

A modern replay of seed `4849548719320319367` with `turso_whopper --mode fts-merge --max-steps 60000` passed in both WAL and MVCC modes. The original report lacks its full workload configuration and scheduling coordinates, so this is additional coverage, not proof that the historical mismatch is repaired.

The branch includes the independently reviewed aggregate misuse guard and matching C header version from the PHP compatibility follow-up, now merged separately to fork main in [fork PR #10](https://github.com/HundredBillion/turso/pull/10). Invalid registered aggregate placement now returns an error instead of aborting the host. The expanded [Linux and Windows validation](https://github.com/HundredBillion/turso/actions/runs/42179131016) also passes the formerly failing header-lifecycle and PostgreSQL read-only probe cases. These changes do not resolve the separate deferred writer-activation or read-only startup hashing review findings.

## Already present

Reader/cache/reopen changes from [#8476](https://github.com/tursodatabase/turso/pull/8476), [#8851](https://github.com/tursodatabase/turso/pull/8851), [#8967](https://github.com/tursodatabase/turso/pull/8967) and [#11114](https://github.com/tursodatabase/turso/pull/11114) are ancestors of the fork. Do not import them twice. Closed, unmerged [#9031](https://github.com/tursodatabase/turso/pull/9031) is not an implicit dependency.

## Adjacent work

[#9772](https://github.com/tursodatabase/turso/pull/9772) retries eligible MVCC autocommit statements within busy deadlines. It does not replay explicit transactions or supply interprocess coordination. [#10829](https://github.com/tursodatabase/turso/pull/10829) adds data-version polling but explicitly errors in MVCC mode. [#10715](https://github.com/tursodatabase/turso/pull/10715) and [#10872](https://github.com/tursodatabase/turso/pull/10872) improve MVCC cursor paths while leaving process-local transaction/log/checkpoint ownership in place. They are not required imports for this foundation.

## Reliability and full-protocol boundary

[Issue #11383](https://github.com/tursodatabase/turso/issues/11383) reports acknowledged commits lost and index corruption in experimental multiprocess WAL, including a two-process rolling-restart report. These reports have not been independently reproduced or repaired in this initial dependency survey. Startup, ATTACH and capacity fixes do not establish a fix for that durability problem.

The existing MVCC/multiprocess rejection must remain until a shared logical transaction protocol is implemented. A PHP configuration switch, physical WAL reader slots, or a global log offset alone cannot share transaction visibility, uniqueness validation, snapshot retention and checkpoint publication. The full PRD requires those contracts and real PHP worker, acknowledgement-ledger and crash/recovery evidence before the implementation PR leaves draft.
