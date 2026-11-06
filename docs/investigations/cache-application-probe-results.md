# Immutable application and Redis comparison

Status: reproducible experiment with functional verification; hosted performance
acceptance remains pending. No cache-removal recommendation follows from this
read-only scope. The approved design is `cache-application-probe-design.md`.

`benches/examples/cache_application_probe.rs` supplies initialization, a loopback
framed TCP service, and the application driver. `scripts/run_cache_application_probe.py`
runs prebuilt binaries, owns an ephemeral memory-capped Redis 7 container, and
retains configuration, binary hashes, raw request latencies, resource counters,
CSV summaries and diagnostics. It removes its own Redis container on exit and
never modifies other containers. It resets only that exclusive container's DB
between trials. Redis is reached through a loopback Docker published port; the
database service is a host process on loopback. On macOS this includes Docker VM
networking. Neither topology represents a multi-host service deployment.

## Response contract and workload

The fixture is immutable. Point responses contain document ID, tenant, version
and JSON; facet responses contain ordered IDs, tenants and versions. All four
paths return exactly the same bytes. An independent generator computes expected
responses before timing, and every response is checked after its timer stops.
There are no stale-cache allowances or concurrent writes in this experiment.

The deterministic request mix alternates point lookup, typed-category facet and
JSON custom-field facet. Ten tenants, sixteen JSON custom fields, absent IDs and
empty facet results are included. Uniform traversal and a 90% hot-key pattern
are separate variants. Point and typed-facet statements have ordinary indexes;
JSON facets scan tenant candidates. The same connections and prepared statements
are used for cache misses and DB-only requests. Cache hits return the encoded
response; misses pay GET, database execution and SET. Missing results are cached
as empty byte strings. Cold means empty Redis without application warmup, not
cold OS/database pages. Warm means every scheduled request was executed before
timing; database-only variants receive the same request warmup.

Workers prepare connections, statements and oracle data before reporting ready.
Only then does the coordinator publish a common start time. Rate zero is a
closed-loop run. A positive `APP_RATE` schedules a fixed arrival sequence,
independent of completion; delayed work remains queued at its assigned worker.
The bounded number of worker connections is part of the model. Arrival-to-
completion latency includes late starts; raw traces retain both quantities.
Per-kind service-time summaries subtract start lateness. A run of `N` requests
at rate `R` has a scheduled arrival horizon of approximately `N/R` seconds and
drains all work afterward; it does not discard unfinished requests to inflate
throughput. Preparation errors fail the run instead of waiting at a barrier.

Limits are 100–10,000 documents, 1–32 workers, 1–100,000 requests and offered rate
up to 1,000,000/s. The default dataset is roughly 1 MiB; rate limits are safety
caps, not target performance. The source uses WAL, FULL and a 64 MiB cache limit.
Read-only timing cannot establish commit/checkpoint behavior under mixed load.

## Reproduce

Build all compared binaries before measuring, with the same Rust 1.88 toolchain,
lockfile and profile. The standalone example needs no new dependencies and can
be copied unchanged into upstream/fork control worktrees. Preserve control
production sources and record their revisions separately from the fixture.

```
cargo test -p turso_benches --example cache_application_probe
CARGO_PROFILE_LIB_RELEASE_PANIC=unwind cargo build -p turso_benches --example cache_application_probe --profile lib-release
python3 scripts/run_cache_application_probe.py \
  --binary upstream=/absolute/upstream-probe \
  --binary fork=/absolute/fork-probe \
  --binary candidate=/absolute/candidate-probe \
  --output /new/results-directory --rows 1000 --requests 2000 \
  --workers 1,8,32 --rates 0,2000 --repeats 3
```

The runner rotates binary order by repeat and reverses variant order on alternate
repeats. Run longer scheduled horizons separately, for example 60,000 requests
at 2,000/s for 30 seconds of offered load. Every variant uses a fresh application
process; the per-binary service persists across its variants and is stopped
before the next binary. Databases are initialized independently from the same
generator and remain unchanged thereafter.

`/usr/bin/time` reports the application's whole-process resources, including
preparation and warmup. Linux `/proc` snapshots report database-service CPU,
resident pages and I/O; Redis INFO snapshots report its CPU and memory counters.
Those before/after counters include warmup, administration and the timed phase;
they are not timed-phase-only CPU measurements or simultaneous peak RSS sums.
Redis's peak statistic persists across trials, so compare current memory and
CPU deltas with appropriate care. The driver precomputes expected responses in
memory; application RSS includes that verification overhead. Empty-cache starts,
cache hits and cache misses must be reported separately. All errors terminate
the experiment with a nonzero status; successful CSVs have no ignored failures.

## Current verification and remaining evidence

The focused oracle check covers 10,000 generated requests and malformed request
shapes. A local functional matrix passed embedded/shared × Redis off/on ×
cold/warm with eight workers and 500 scheduled requests per variant. The complete
Python runner then passed all sixteen deployment/cache/warm/hot combinations
with 100 rows, 100 requests and one worker, including trace and resource artifacts
and container cleanup. These are correctness smokes on a shared host, not
performance acceptance. Local scheduler delays were large relative to service
time; raw lateness is retained rather than attributed to database latency.

Outstanding acceptance evidence includes isolated matched upstream/fork/candidate
runs, sustained concurrency, resource tradeoffs and application representative
fixtures. Mixed writes require the separately designed consistency protocol,
FULL commit/checkpoint and crash evidence. A positive read-only result cannot
establish those properties or justify removing Redis from a mutable application.

The runner also injects an incorrect cached response and requires an oracle
failure before timing. It then attempts worker initialization under a nonexistent
parent directory and requires a prompt preparation error. Both controls run for
each binary, preventing a disabled oracle or initialization deadlock from silently
producing benchmark output.

The default 2,000-request/2,000-per-second cases are roughly one-second bounded
screening, not sustained acceptance. After screening, restrict the matrix with
`--deployments shared --cache false,true --warm true --hot true --workers 8
--rates 2000 --requests 60000` for selected 30-second offered-load cases. Capacity
and representative case selection should guide these longer trials.

Service startup has a ten-second deadline, including incomplete startup output.
The runner validates trace counts, hit/miss totals, nonnegative service/lateness,
reported percentiles and completion of the offered-rate arrival horizon. A real
scheduled trace passed this validation; removing a request was rejected. A hung
startup child was rejected by the deadline in a focused control.

Each shared connection now waits for an explicit READY response sent only after
the service opens its connection and prepares every statement. The handshake
regression failed before this synchronization and passed afterward, keeping
server preparation outside cold-trial timing just as embedded preparation is.
Earlier functional smoke traces predate this correction and are not performance
evidence. Final hosted comparisons must use the handshake revision.

The final handshake build passed all 32 functional variants with eight workers,
100 rows and 100 requests: all sixteen deployment/cache/warm/hot combinations
at both closed-loop and 1,000/s scheduled arrivals. Negative controls and trace
validation ran in the same driver. The committed smoke metadata/trace now come
from this final build. Independent review found no remaining methodology blocker
within the stated immutable scope.
