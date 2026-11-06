# Next application comparison

Approved for implementation by the coordinating reviewer on 2026-11-06.

The next bounded experiment compares complete read-only application requests,
not individual engine operations. The dataset stays immutable for each trial;
DB-only and DB+Redis therefore return identical document IDs, tenant IDs,
versions and response bytes. This scope avoids calling cache-aside invalidation
linearizable: a cache miss racing an update can repopulate stale data even when
writers delete keys after commit. Mixed-write equivalence requires a separately
measured consistency protocol before it can support a cache-removal conclusion.

Use one test-only Rust executable with a small framed TCP request loop and no
production API commitment. Reuse the existing core database connection and
prepared point/facet statements. Three request kinds cover tenant-scoped document
lookup, known-field facet IDs, and unpredictable-field facet IDs. Encode complete
responses identically before returning them; validate every response against a
precomputed immutable oracle outside the latency timer. Request scheduling is
deterministic with uniform and hot-key distributions, including missing IDs and
empty results. Cap generated payloads at 100 MiB initially.

All network paths use loopback; results do not establish multi-host network
performance. Compare deployment paths separately:

* Embedded DB-only calls inside each application worker, sharing one database.
* Embedded DB+Redis uses loopback Redis GET, queries the same embedded database on
  a miss, and SETs the exact encoded response. Record hits and misses separately.
* Shared DB-only sends the same framed request to a test-only database service.
* Shared DB+Redis checks Redis in the application and calls that database service
  on a miss. All response encoding and oracle checks remain identical.

Use a task-specific Redis 7 container bound only to loopback, with a fixed memory
limit and eviction policy, no persistence (the cache is disposable), and no
changes to existing containers. Report Redis configuration, memory and process
CPU separately from application and database memory/CPU. Warm-cache and empty-
cache starts are different trials; reset only task-specific keys. Keep database
WAL/FULL settings explicit even though the timed read-only phase cannot measure
commit durability. Run initial concurrency 1, 8 and 32 with fixed request counts,
then a duration-based sustained run at a bounded offered rate. Report completed
throughput, p50/p95/p99, errors, late starts, misses and total resource usage.
Closed-loop concurrency alone does not characterize overload queueing.

Build upstream main, fork baseline and candidate before trials, preserving binary
hashes and identical fixture/configuration. Alternate variants on isolated hosted
Linux. Never substitute a direct engine benchmark for an application request or
compare cache hits against cold disk without labeling the difference.

After read-only equivalence is established, the next experiment can add a
single application writer gate spanning DB commit and cache invalidation, with
readers participating in the same gate. That provides an explicit consistency
protocol at a measurable serialization cost, but applies only to writers and
readers within that controlled service. Independent writers require another
protocol. Exercise acknowledged FULL commits, checkpoint activity and failure
recovery in that mixed workload. No recommendation to remove Redis follows
until sustained workload results and operational tradeoffs support it.
