# Sustained immutable cache comparison

Run: https://github.com/HundredBillion/turso/actions/runs/42255336769

Thirty-second trials at 2,000 scheduled requests/second, eight workers, warm cache and hot keys. Three rotated repeats for each of upstream, current fork main, and JSON candidate; embedded and loopback shared service; database-only and database plus Redis. Preserved screening binaries were hash-verified, not rebuilt. All 36 traces contain exactly 60,000 requests (2,160,000 total); warm Redis trials recorded all cache hits. The runner verified response correctness against the deterministic immutable fixture.

The table reports medians of three per-trial percentile values, in microseconds. These are not pooled percentiles. All configurations sustained approximately 2,000 requests/second; that offered rate does not establish maximum capacity.

| Engine | Deployment | Redis | p50 | p95 | p99 | Late-start p99 |
|---|---|---|---:|---:|---:|---:|
| candidate | embedded | false | 73.086 | 349.901 | 420.041 | 82.882 |
| candidate | embedded | true | 133.569 | 143.789 | 161.721 | 64.290 |
| candidate | shared | false | 120.006 | 453.816 | 474.777 | 65.860 |
| candidate | shared | true | 134.008 | 143.054 | 158.061 | 64.172 |
| fork | embedded | false | 72.221 | 323.416 | 394.538 | 68.732 |
| fork | embedded | true | 134.386 | 143.700 | 161.884 | 64.666 |
| fork | shared | false | 119.687 | 369.397 | 449.676 | 65.338 |
| fork | shared | true | 134.062 | 143.224 | 170.164 | 64.265 |
| upstream | embedded | false | 72.046 | 337.866 | 409.490 | 74.664 |
| upstream | embedded | true | 133.958 | 143.320 | 174.318 | 64.540 |
| upstream | shared | false | 128.011 | 385.879 | 464.432 | 65.382 |
| upstream | shared | true | 134.393 | 146.119 | 162.968 | 64.184 |

Conclusion: database-only improves median request latency in this tested workload, while warm Redis materially improves p95/p99. The candidate does not establish improved application tails over current main: its embedded p99 is 420.041us versus fork 394.538us, and shared p99 is 474.777us versus fork 449.676us. Repeat variation remains visible in raw summaries and no trials were discarded.

No blanket Redis-removal recommendation is supported. If a workload needs the observed cache tail latency, retain the cache under these assumptions. A database-only deployment may still meet a separately chosen application SLO, but the user has not supplied such an SLO and this experiment does not invent one.

Limits: immutable data, hot keys, warm cache, one loopback host, eight workers, one offered rate, and 30-second windows. No mixed-write consistency, long-term checkpoint churn, cross-host network behavior, RAM-pressure dataset, or cold OS-cache claim. Resource counters include setup/warmup/oracle work; they must not be read as timed-request-only memory/CPU. Mixed-write and recovery investigation remains separate.

The 36 per-trial summaries and binary/environment metadata are retained in [results/cache-application-sustained](results/cache-application-sustained). Full traces and resource reports are in the linked hosted run artifact; they are omitted here to avoid duplicating millions of trace rows.
