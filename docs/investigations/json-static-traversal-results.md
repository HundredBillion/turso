# JSON extraction experiment: full-suite decision

Status: **do not merge the experimental optimization into main under the agreed benchmark criterion**. The read path reduces allocations and improves large-document extraction, but the full upstream suite also shows reproducible regressions. This is a measured tradeoff, not an unconditional performance improvement.

The static traversal candidate is `ad54b7aaa6bf7b3933d4971ba039f14bab21e3d6`, based on the original extraction candidate `c4808695` and fork main `4ccf547`. It replaces a runtime traversal-mode parameter with a const generic. This is private implementation specialization; SQL semantics and storage format remain unchanged. The experiment remains on `experiment/json-static-traversal`.

## Evidence

- [Targeted canonical-build comparison](https://github.com/HundredBillion/turso/actions/runs/42257719725): all four variants built with the same source directory, fixture files, Rust toolchain and release flags; semantic checks ran before measurement. Eight cases and four rotated repeats showed enough improvement to justify broader measurement.
- [Full preserved-executable comparison](https://github.com/HundredBillion/turso/actions/runs/42262140794): verified the earlier executable hashes without rebuilding; 38 cases, three variants and three rotated repeats, totaling 342 observations. The controls are fork main `4ccf547` and upstream `2709799`. Hardware was AMD EPYC 9V74.
- Prior [full original-candidate](https://github.com/HundredBillion/turso/actions/runs/42243464027), [canonical-path](https://github.com/HundredBillion/turso/actions/runs/42250171490), and [same-host preserved-binary](https://github.com/HundredBillion/turso/actions/runs/42254569396) comparisons remain counter-evidence against interpreting one favorable run as universal acceptance.

## Result

100KiB extraction improves approximately 20–34% versus upstream in the full run, while deeply nested JSONB size is 10.78% slower and deeply nested social-feed patching is 9.49% slower. Several other JSONB-size cases are 7–9% slower. The largest regressions recur in all three repeats with disjoint within-run confidence intervals; they are not dismissed as measurement noise.

For example, deeply nested JSONB-size medians were 1125.5–1137.3ns for the candidate versus 1019.3–1030.9ns upstream. Social-feed patching was 4839.2–4851.5ns versus 4419.8–4486.1ns. The allocation savings remain useful evidence, but do not override latency regressions.

The cause of the unrelated size-path regressions has not been isolated. Code layout and compiler effects are hypotheses, not established explanations. Further tuning requires profiling and a new matched comparison. Neither this result nor the application results justify a JSONB or database-format rewrite.

Percentages below compare medians of three per-trial medians; negative means faster. They are not pooled confidence estimates. No trials were discarded.

| Case | Candidate vs fork main | Candidate vs upstream |
|---|---:|---:|
| JSON Extract/1024/first | -1.85% | -4.48% |
| JSON Extract/1024/last | -1.35% | -5.83% |
| JSON Extract/1024/missing | -10.98% | -14.09% |
| JSON Extract/1024/multiple | -0.57% | -2.48% |
| JSON Extract/1024/nested | +0.22% | -4.20% |
| JSON Extract/1024/subdocument | -0.73% | -3.32% |
| JSON Extract/10240/first | -6.50% | -6.45% |
| JSON Extract/10240/last | -6.13% | -6.64% |
| JSON Extract/10240/missing | -12.76% | -13.02% |
| JSON Extract/10240/multiple | -13.40% | -14.20% |
| JSON Extract/10240/nested | -4.91% | -6.08% |
| JSON Extract/10240/subdocument | -6.84% | -7.33% |
| JSON Extract/102400/first | -20.23% | -20.28% |
| JSON Extract/102400/last | -19.97% | -19.67% |
| JSON Extract/102400/missing | -32.49% | -25.43% |
| JSON Extract/102400/multiple | -30.37% | -34.09% |
| JSON Extract/102400/nested | -20.24% | -20.05% |
| JSON Extract/102400/subdocument | -19.43% | -19.86% |
| JSON Patch - Add New Property/Limbo | +1.26% | -0.37% |
| JSON Patch - Array Update/Limbo | +0.03% | +2.73% |
| JSON Patch - Complex User Object Update/Limbo | +7.68% | +3.47% |
| JSON Patch - Deeply Nested Social Feed Update/Limbo | +8.71% | +9.49% |
| JSON Patch - Large Config Update/Limbo | +6.06% | +2.79% |
| JSON Patch - Nested Property Update/Limbo | +0.44% | -2.92% |
| JSON Patch - Remove Property/Limbo | +2.23% | +1.77% |
| JSON Patch - Simple Property Update/Limbo | +0.60% | -0.23% |
| JSONB Size - Array heavy/Limbo | +6.75% | +8.45% |
| JSONB Size - Deeply nested/Limbo | +8.35% | +10.78% |
| JSONB Size - Large/Limbo | -0.17% | -6.17% |
| JSONB Size - Medium/Limbo | -6.75% | -1.33% |
| JSONB Size - Real world json #1/Limbo | +4.88% | +8.54% |
| JSONB Size - Real world json 2/Limbo | +6.55% | +9.06% |
| JSONB Size - Real world json 3/Limbo | +5.68% | +8.77% |
| JSONB Size - Real world json 4/Limbo | +6.30% | +7.15% |
| JSONB Size - Real world json 5/Limbo | +6.29% | +8.49% |
| JSONB Size - Real world json 6/Limbo | +5.41% | +7.93% |
| JSONB Size - Small/Limbo | -12.54% | -5.00% |
| Sequential JSONB Calls/Limbo - Sequential | +0.54% | +5.27% |

Raw summaries and metadata are in `results/json-static-targeted/` and `results/json-static-full/`. Public run artifacts retain per-repeat logs and Criterion estimates. These microbenchmarks do not establish end-to-end cache removal, and the application comparisons use explicitly recorded candidate revisions.
