# Mixed-write application protocol experiment

This is a test-only implementation of the [approved bounded protocol](cache-mixed-write-protocol.md). It does not change the engine or expose a production service API. Performance acceptance against upstream and current fork main remains pending.

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

No mutable-workload latency result is accepted yet. Hosted alternating upstream/current-main/candidate trials, recorded model overhead, resource accounting and sustained mixed-write comparisons remain pending. The [sustained immutable result](cache-application-sustained-results.md) already shows lower database-only medians but better Redis tails; it does not justify universal cache removal.

## Hosted comparison runner

`python3 scripts/compare_cache_mixed.py --output /tmp/mixed-comparison` builds the identical immutable/mixed examples and simulated-I/O fixture in one canonical worktree path for upstream `2709799`, current fork `4ccf547`, and experimental static-traversal runtime `ad54b7a`. This candidate differs from the earlier immutable application's `c480869` runtime and is not production-accepted. Each runtime uses the same Cargo.lock, Rust 1.88.0 and optimized profile, passes focused example tests, and has its executable/hash preserved. All builds finish before measurement. The dispatch-only `cache-mixed-comparison.yml` workflow uploads raw artifacts.

The separate `run_cache_mixed_probe.py` accepts prebuilt `--binary label=PATH` arguments. Defaults are a bounded screening matrix: 1,000 rows, 4,000 requests at 2,000/s, eight workers, 1%/5% writes, embedded/shared, database-only/Redis, warm/hot, three rotated repeats. This is 72 approximately two-second trials, not sustained acceptance. Each trial initializes a fresh database and resets only its task-owned capped Redis container. Recovery checks run before timing; Redis readiness has a deadline. The runner verifies sequence/write counts, timing arithmetic, trace percentiles, offered-rate horizon, checkpoint cadence/backlog evidence and binary hashes. It records whole-process `/usr/bin/time`, shared-service `/proc`, Redis INFO, and DB/WAL sizes. Initializer cost is outside the timed process; request-process resource counters include warmup/oracle/final validation. Select a sustained subset only after these results establish capacity.
