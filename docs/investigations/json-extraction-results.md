# JSON extraction: correctness, allocations and timing controls

Correctness and allocation checks pass. **Latency acceptance remains unresolved after multiple hosted comparisons.** Favorable large-extraction medians coexist with repeat variation and smaller regressions. This experimental production diff remains on its review branch; no unconditional speedup or main-branch acceptance is claimed. Local shared-host timings are not acceptance evidence.

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

## Hosted comparison on pinned revisions

[Run 42243464027](https://github.com/HundredBillion/turso/actions/runs/42243464027)
completed all 38 cases on Linux with the recorded Rust 1.88/profile, three
alternating repeats, one-second warmups, two-second measurements and 50 samples.
Candidate source was `68aaebc63b898f4fd07e1f19bf4cea4e8190cc6b`.
[Metadata](results/json-extraction-hosted/metadata.json),
[all-case summary](results/json-extraction-hosted/summary.csv) and
[raw samples](results/json-extraction-hosted/samples.json) are retained.

At 100 KiB, first/last/nested extraction was 12.7–13.5% faster than upstream,
missing-path extraction 25.7% faster, multiple paths 32.9% faster and subdocument
extraction 13.4% faster. The corresponding fork comparisons also improved.
At 10 KiB all six extraction cases improved versus both controls. At 1 KiB,
changes were mixed and small except missing paths, which improved by 9.35%
versus upstream. The largest observed regression versus upstream was the
unchanged small JSONB conversion benchmark, +2.63%.

The fork comparison contains repeatable regressions and is not a clean no-regression
result. Small JSONB conversion rose 24.84% and medium conversion 16.78%; simple
and remove-property patch cases rose 5.67% and 5.33%. For small conversion,
fork repeats were 418.5/443.7/418.1 ns and candidate repeats 522.5/523.4/504.8 ns.
Medium conversion was 659.6/682.2/659.8 versus 775.6/770.5/757.2 ns. Their per-run
confidence intervals do not overlap. These results cannot be dismissed as noise.
The `jsonb()` conversion case does not call the changed search traversal, so the
cause is not established by the source diff; code generation/layout is a
hypothesis, not a finding. A diagnostic rebuild using one canonical source path,
including an identical-source rebuild control, is the next way to distinguish
build-layout effects from an attributable execution regression. Preserve the
current favorable and unfavorable evidence while that investigation remains open.

## Canonical rebuild and same-host counter-evidence

The [canonical-path hosted control](https://github.com/HundredBillion/turso/actions/runs/42250171490) rebuilt all variants from the same path and included an identical-source fork rebuild. The two fork binaries had identical hashes. Earlier small-size/patch regressions shrank or reversed, but 100 KiB first extraction was 23.657 microseconds for the candidate versus 21.335 for fork and 21.108 for upstream (about 11–12% slower). Multiple extraction remained faster: 24.474 versus fork 27.970 microseconds. [Metadata and complete summaries](results/json-canonical) preserve this unfavorable observation.

A subsequent [same-host preserved-binary comparison](json-matched-artifacts-results.md) ran both original and canonical executable pairs without rebuilding. Four rotated repeats found median first-extraction improvements around 21–22% and multiple-extraction improvements around 35–42% for both pairs. However, individual candidate first-extraction repeats ranged roughly 18–62 microseconds. Original small-size and simple-patch regressions persisted at +5% and +4.08%; canonical simple patch was +2.35%. [Hashes and summaries](results/json-matched-artifacts) retain all observations.

The canonical reversal therefore is not a stable property of that executable alone. Its cause remains unproven. The later favorable medians do not erase the regression evidence, and allocation savings alone do not satisfy the requested main-branch performance criterion. Source-based diagnostics and any later candidate must preserve these controls rather than selecting only favorable runs.
