# Mixed-write application protocol experiment

This is a test-only implementation of the [approved bounded protocol](cache-mixed-write-protocol.md). It does not change the engine or expose a production service API. Corrected hosted screening and the longer comparison against upstream and current fork main are complete. Database-only requests have lower medians in this mutable workload, but tail latency varies too much to support a general cache-removal recommendation or an experimental-runtime speedup.

`benches/examples/cache_mixed_write_probe.rs` reuses the immutable probe's SQL reads, response encoding, loopback framing and Redis transport. Every write changes document version, typed/JSON facets and inventory, and inserts an order in one ordinary WAL/FULL transaction. An independent in-memory model checks exact content, orders, inventory and integrity. All application reads and writes use one shared read/write gate; successful commits advance cache generation before releasing the writer guard. A lost or invalid commit response closes application admission.

This protocol covers one application process and requires every writer to use its gate. It does not prove cache consistency across independent applications or the multiprocess MVCC protocol in #7.

## Reproduction

Build with the repository's pinned Rust toolchain:

```sh
cargo test -p turso_benches --example cache_mixed_write_probe
cargo build -p turso_benches --example cache_mixed_write_probe
cargo clippy -p turso_benches --example cache_mixed_write_probe --no-deps -- -D warnings
BIN=target/debug/examples/cache_mixed_write_probe
$BIN init /tmp/mixed-example.db 1000
APP_WORKERS=4 APP_REQUESTS=2000 APP_RATE=2000 APP_WRITE_EVERY=20 \
  APP_CHECKPOINT_EVERY=200 APP_WARM=true APP_HOT=true \
  APP_TRACE=/tmp/mixed-trace.csv $BIN run /tmp/mixed-example.db 1000
```

Each trial requires a newly initialized database. Set `APP_REDIS=127.0.0.1:PORT` for a dedicated Redis instance with bounded `maxmemory` and eviction; the runner does not clear arbitrary Redis databases. `serve DATABASE 127.0.0.1:0` prints a loopback endpoint; pass that endpoint in `APP_DATABASE` to exercise the shared-service path. Each connection waits until its statements are prepared. Row count is bounded to 100–10,000, workers to 1–32, and requests to 100,000.

Positive `APP_RATE` schedules fixed arrival deadlines across finite workers; rate zero is closed loop. Trace rows contain sequence, read/write kind, arrival latency, late start, gate wait, service time, model publication time and cache-hit status. Request timing includes gate acquisition, FULL commit, generation publication and model publication. Read-oracle validation occurs after timing; it still consumes worker CPU and can delay later requests. Model publication can copy outstanding reader snapshots, including accumulated orders, so its cost is workload-dependent rather than a constant shared across cache variants. Its total and p50/p99 are reported, and must be examined before interpreting comparisons. Gate wait includes waiting for that test-oracle work.

A separate connection performs PASSIVE checkpoints after every configured number of completed requests. Its queue can lag; output records the channel threshold, an atomic count of validated completions observed at checkpoint start, start/end offsets from the benchmark origin, elapsed time and SQLite busy/log/checkpointed triple. The summary separately records the last timed request completion offset. These distinguish checkpoints during traffic from backlog drained after traffic. Request completion time and checkpoint-drained time are separate. A final fresh connection compares the complete database with the independent model. Resource counters for a process include preparation and oracle work, not only database request execution.

## Correctness checks and current evidence

Seven focused Rust tests pass, including existing immutable request/READY checks, exact multi-row state, fail-closed admission, an actual TCP write that commits but receives an invalid acknowledgement, and simulated storage failure. Disabling the fail-closed flag caused the lost-ack test to fail by observing a subsequent wire request; restoring it passed. The storage test retains only synced bytes after an acknowledged FULL commit followed by an uncommitted partial inventory update. It also recovers a checkpoint image containing both persisted and dropped writes. Both cases recover the independently expected orders, inventory and full document content with `integrity_check=ok`.

Local debug functional runs covered four-worker embedded and loopback service requests with real Redis, 400 requests, 5% writes, both closed-loop and 2,000/s scheduling, and four concurrent checkpoint attempts per run. Every response matched the model. These short shared-host runs establish execution/correctness coverage, not performance acceptance.

`cache-check DATABASE REDIS_ADDRESS` uses a fresh 100-row fixture to check point, typed-facet and JSON-facet generation invalidation. It also reconstructs application state with a fresh namespace while retaining deliberately stale old-namespace entries. This command alone is an in-process restart simulation.

The separate `scripts/check_cache_mixed_recovery.py` controller runs real child processes, persists external intent and acknowledgement records with `fsync`, kills workers at deterministic barriers, and launches fresh recovery processes. See its `--help` for bounded execution options. The worker supports barriers before COMMIT, after successful FULL COMMIT, after model/generation publication, and after the external controller receives its success acknowledgement. Recovery expectations come from the controller's predefined transaction and observed phase, not the database being checked. Completed transactions without an acknowledgement are classified explicitly. These SIGKILL controls are process failures, not power-loss tests; the independent UnreliableIo test supplies the narrower synced-byte/torn-checkpoint evidence.

Both cacheless and combined Redis process controls passed all four crash phases. Their external ledgers, events, fresh-checker responses and binary hashes are retained in [cacheless evidence](results/cache-mixed-process-control-latest) and [Redis process evidence](results/cache-mixed-process-redis-verified). Five Python negative controls reject silent workers, incomplete events, incorrect acknowledgements, incorrect recovered generations and reused namespaces.

The optional Redis worker/checker path retains entries across actual process death. The old worker deliberately seeds initial responses at generations 1 and 2 so reuse after a committed update would expose stale data. The new process must attest a different random namespace, confirm old entries remain, obtain misses in the fresh namespace and validate recovered responses. Do not infer this combined property from the cacheless controller and the in-process command alone.

## Interpretation

The corrected screening and longer comparison completed successfully. The longer trials retain lower database-only medians, but invalidate a claim of consistently better database-only tail latency. The [sustained immutable result](cache-application-sustained-results.md) already shows lower database-only medians but better Redis tails; it does not justify universal cache removal.

## Hosted comparison runner

`python3 scripts/compare_cache_mixed.py --output /tmp/mixed-comparison` builds the identical immutable/mixed examples and simulated-I/O fixture in one canonical worktree path for upstream `2709799`, current fork `4ccf547`, and experimental static-traversal runtime `ad54b7a`. This candidate differs from the earlier immutable application's `c480869` runtime and is not production-accepted. Each runtime uses the same Cargo.lock, Rust 1.88.0 and optimized profile, passes focused example tests, and has its executable/hash preserved. All builds finish before measurement. The dispatch-only `cache-mixed-comparison.yml` workflow uploads raw artifacts.

The separate `run_cache_mixed_probe.py` accepts prebuilt `--binary label=PATH` arguments. Defaults are a bounded screening matrix: 1,000 rows, 4,000 requests at 2,000/s, eight workers, 1%/5% writes, embedded/shared, database-only/Redis, warm/hot, three rotated repeats. This is 72 approximately two-second trials, not sustained acceptance. Each trial initializes a fresh database and resets only its task-owned capped Redis container. Recovery checks run before timing; Redis readiness has a deadline. The runner verifies sequence/write counts, timing arithmetic, trace percentiles, offered-rate horizon, checkpoint cadence/backlog evidence and binary hashes. It records whole-process `/usr/bin/time`, shared-service `/proc`, Redis INFO, and DB/WAL sizes. Initializer cost is outside the timed process; request-process resource counters include warmup/oracle/final validation. Select a sustained subset only after these results establish capacity.


## Corrected hosted screening

[Run 42268556981](https://github.com/HundredBillion/turso/actions/runs/42268556981) completed all 72 trials and 288,000 requests on an AMD EPYC 7763 runner. The same fixture revision `8a14bdb01` was built for the three pinned runtimes. Every runtime passed the external four-phase crash/restart controls with retained Redis entries and fresh namespaces before measurement. All trials completed the offered approximately 2,000 requests/second; this establishes capacity for the selected followup rate, not maximum throughput.

At 5% writes, medians of three trial percentiles were:

| Runtime | Deployment | Database-only p50 / p99 (us) | Redis p50 / p99 (us) |
| --- | --- | --- | --- |
| Upstream | Embedded | 129 / 806 | 369 / 1370 |
| Fork main | Embedded | 127 / 801 | 363 / 1179 |
| Candidate | Embedded | 130 / 814 | 362 / 1200 |
| Upstream | Loopback service | 209 / 897 | 448 / 1438 |
| Fork main | Loopback service | 207 / 922 | 442 / 1348 |
| Candidate | Loopback service | 211 / 930 | 441 / 1358 |

These mutable requests do not reproduce the immutable warm-cache result: each successful write advances the shared cache generation, so cache invalidation and misses matter. This screening favors database-only requests for this specific workload, while it does not establish an advantage for the experimental JSON runtime over current main. Repeat ranges, read/write percentiles, model-publication cost and checkpoint overlap are retained in [screening evidence](results/cache-mixed-screening). Do not pool the three trial percentiles as if they were one latency distribution.

The earlier run `42263467711` failed during shared-service verification: the benchmark client tried to open the database directly while the service owned its handles. The corrected fixture verifies through that service against the client's independent transaction history. A deliberately bypassed verification failed its regression test before the corrected implementation passed. Partial timings from the failed run are not accepted comparison evidence.

The [sustained followup](https://github.com/HundredBillion/turso/actions/runs/42271229744) reuses the successful screening executables after checking their hashes and fixture identity. It measures 36 trials of 60,000 requests each, eight workers at 2,000 requests/second, 5% writes, and PASSIVE checkpoint requests every 200 completed requests. It completed successfully; results follow.


## Sustained result and decision

The longer run passed all 36 trials and 2.16 million requests, including fresh-process recovery controls for all three preserved executables. Each trial offered 60,000 requests over thirty seconds, with 3,000 FULL-durability writes. Observed throughput was 1,999.925–1,999.990 requests/second across the complete matrix. This is a fixed-rate comparison, not a saturation-capacity result.

The following values are medians of three independent trial percentiles, in microseconds:

| Runtime | Deployment | Database-only p50 / p95 | Redis p50 / p95 |
| --- | --- | --- | --- |
| Upstream | Embedded | 92 / 496 | 185 / 581 |
| Fork main | Embedded | 90 / 481 | 194 / 600 |
| Candidate | Embedded | 91 / 499 | 186 / 602 |
| Upstream | Loopback service | 124 / 556 | 221 / 622 |
| Fork main | Loopback service | 125 / 527 | 222 / 640 |
| Candidate | Loopback service | 125 / 547 | 225 / 641 |

Tail latency is much less stable. Each cell below shows the three trial p99 values as **minimum / median / maximum, in milliseconds**:

| Runtime | Deployment | Database-only p99 | Redis p99 |
| --- | --- | --- | --- |
| Upstream | Embedded | 29.260 / 73.124 / 134.211 | 0.797 / 1.205 / 89.509 |
| Fork main | Embedded | 0.591 / 1.079 / 98.026 | 0.692 / 1.652 / 308.139 |
| Candidate | Embedded | 0.803 / 13.337 / 43.464 | 0.846 / 1.360 / 98.233 |
| Upstream | Loopback service | 0.641 / 93.425 / 115.735 | 0.681 / 0.705 / 2.358 |
| Fork main | Loopback service | 0.720 / 1.098 / 3.429 | 0.995 / 72.375 / 94.970 |
| Candidate | Loopback service | 0.691 / 13.986 / 84.965 | 0.842 / 0.985 / 1.980 |

The traces show occasional slow writes blocking readers through the application gate, followed by delayed request starts. For example, one upstream service write took about 372 ms with negligible initial gate wait; neighboring readers then waited about 370 ms at the gate. A candidate embedded Redis write similarly took about 351 ms. These observations establish application queueing after a slow write, but do not identify its internal database, filesystem or host cause. The harness does not time individual synchronization calls, so attributing these spikes specifically to fsync or dismissing them as host noise would be unsupported.

Each trial completed 300 checkpoint attempts; 299 completed before its final timed request. The last checkpoint drains after traffic, as expected from its completion threshold. Redis read-hit fractions were about 47%, because successful writes invalidate the generation. Timed model-publication work consumed less than 0.003% of the trial horizon in every case; its write p99 was below one microsecond. This narrow model-update cost does not exclude untimed oracle CPU from influencing later requests. Full-process resource counters include setup and verification; shared-service counters and Redis counters are recorded separately.

The [retained results](results/cache-mixed-sustained) include every trial summary, repeat analysis, resource counters, binary/build identities, external recovery ledgers and selected slow-write records. Full traces and executable artifacts are available through the linked runs and retained in the investigation workspace. The screening CPU model was recorded; the reuse-only sustained job recorded its OS/platform but did not separately capture its CPU model. Its copied build metadata describes the screening build host, not new hardware evidence for the sustained run. All variants within the sustained comparison ran on the same hosted job.

**Decision:** database-only is a reasonable simplification candidate for this measured, single-process, warm/hot, small-data request mix because it lowers median latency and removes a cache-coordination dependency. This is not a universal recommendation to remove Redis: the immutable comparison favored warm Redis tails, and this mutable comparison has unstable tails in both modes. No numeric application tail-latency requirement was supplied or invented. The experimental JSON runtime does not establish a consistent application advantage over current fork main and already failed the upstream full-JSON-suite criterion, so it remains unmerged.

These results cover 1,000 documents, eight workers, one offered rate and thirty-second trials on ordinary WAL with FULL writes. They do not establish capacity for large datasets, hours-long operation, multiple independently writing application processes, or cross-process MVCC. The protocol requires every writer to obey one application gate and uses broad generation invalidation; other cache-coherence protocols may have different results. Wider workloads and a product-specific latency target remain necessary before a deployment decision.
