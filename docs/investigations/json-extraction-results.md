# JSON extraction: correctness, allocations and timing controls

Correctness and allocation checks pass. **Latency acceptance remains pending the
isolated Linux comparison.** The local host had substantial unrelated CPU work;
its timing results must not be presented as a demonstrated speed improvement.

## Change

Searches now use a dedicated path that cannot create intermediate containers and
does not copy the document for mutation rollback. Mutating operations keep their
existing traversal and rollback behavior. Search result buffers start empty and
fallibly reserve space for the selected value, rather than the input document.
Cached document cloning and database-record decoding remain unchanged. This is
not a fully borrowed JSON cache interface, a new JSON representation or a storage
format change.

The search interface distinguishes a missing path from allocation failure.
Selected-result reservation failures propagate through scalar functions and JSON table functions;
they must not become SQL NULL or an empty result set. Existing infallible
allocations elsewhere, including traversal-stack allocation, are not redesigned.

## Validation

- 183 core JSON tests passed, including 500 generated comparisons of nested
  positive/negative array searches against the previous snapshot-based search.
- All 1,296 SQL tests in `sqlite-sqltests/json` passed through the native Rust
  conformance backend, including the six new extraction cases. Those six also
  passed independently against SQLite 3.54.0.
- An isolated integration-test allocator fails exactly one 19-byte selected-value
  allocation after warming the query. Both `json_extract` and `json_each` return
  `OutOfMemory`, then succeed after reset. Deliberately swallowing the error made
  the scalar query return SQL NULL and the test fail; restoring propagation made
  it pass. Stable builds use `std::Vec` for these buffers, so the process-wide
  Turso allocator hook cannot inject this particular failure.
- The file-backed workload probe completed all 36 cases with cardinality and
  final-value checks. This used 100 rows and one timed sample for a correctness
  smoke check; its timings are not benchmark evidence. Plans confirmed scans
  before indexing and covering-index searches afterward.
- Strict core and benchmark/test/probe Clippy passed with the `bench` feature. Default-feature strict
  core Clippy hits an existing unfulfilled lint expectation in `json/cache.rs`;
  the same failure was reproduced on unchanged upstream main.

## Allocation measurements

The separate Divan probe uses the same fixture on actual upstream main and the
candidate. It measures a prepared scalar extraction from a JSONB table row with
the MiMalloc allocation profiler. Values below are the profiler's rounded
per-iteration allocation totals, not resident memory or bytes read from storage.

| JSON text size | Upstream allocated bytes | Candidate allocated bytes | Allocation calls |
|---|---:|---:|---:|
| 1 KiB | 6.370 KB | 4.334 KB | 11 → 10 |
| 10 KiB | 71.92 KB | 51.45 KB | 13 → 12 |
| 100 KiB | 717.0 KB | 512.2 KB | 13 → 12 |

This removes roughly 29% of allocated bytes in the larger cases. It does not
establish the same percentage latency improvement. The Criterion timing suite
uses the system allocator; do not compare its times with Divan's MiMalloc times.
Raw profiler output is preserved with the [local results](results/json-extraction-local/metadata.json).

## Matched timing controls

The original upstream JSON benchmark file matches this fork byte-for-byte. Its
20 conversion/cache/patch cases remain unchanged, and 18 extraction cases cover
first/last keys, nested and missing paths, multiple paths and subdocuments at
1/10/100 KiB. All three engines use the same extended benchmark fixture:

- Upstream main: `2709799dad2ebb0f1749cfa62fa28514f568da28`.
- Fork main: `78a77aac11310d4d928bff623195d34b33242dd5`.
- Candidate: the source diff and executable hashes recorded in the local metadata.

All binaries were built before measurement, with Rust 1.88 and the same optimized
`lib-release` profile. Three alternating orders balance position within the run.
The local protocol used 0.2 seconds warmup, 0.5 seconds measurement, 30 samples
and 10,000 bootstrap resamples per case. The [complete CSV](results/json-extraction-local/summary.csv)
and [per-run estimates/raw samples](results/json-extraction-local/samples.json)
preserve all 38 cases, including unfavorable results.

Local medians were mixed: 1 KiB extraction cases were lower, the 10 KiB
subdocument case was higher, and 100 KiB first/last reads were higher while
several other 100 KiB cases were lower. The repeated unchanged patch cases were
close to their controls. These observations do not establish whether the large
document differences are allocator effects, host contention, or an engine
regression. Retain the allocation result and wait for the isolated comparison
before accepting latency claims.

## Isolated reproduction

```sh
python3 scripts/compare_json_benchmarks.py --output /tmp/json-comparison
```

Run from a checkout containing the candidate commit and both recorded control
commits, with Rust 1.88.0 installed. The script creates temporary detached
worktrees, copies only the benchmark fixture into controls, requires identical
lockfiles, builds all executables, records their hashes, and then performs three
alternating runs per revision. Defaults are one-second warmups, two-second
measurements and 50 samples. Raw compiler logs, executables, metadata, samples,
allocation output and summary CSV are retained in the output directory.

The `JSON performance comparison` workflow runs this on an isolated Ubuntu job
and uploads the complete artifact. A green job means the experiment completed;
it is not an automatic speedup acceptance gate. The comparison still needs
review for regressions and workload relevance. It does not answer whether Redis
can be removed or whether a dense vector index is ready for production.
