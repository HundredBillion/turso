# Matched-host JSON artifact diagnosis

Run: https://github.com/HundredBillion/turso/actions/runs/42254569396

Four preserved executables from the original and canonical build comparisons ran on one AMD EPYC 7763 GitHub-hosted VM. Four rotated repeats per executable, eight benchmarks, 128 observations. No rebuild. Values below are median-of-repeat medians; percentage is candidate versus its matching fork binary. Negative means faster.

| Benchmark | Original pair | Canonical pair |
|---|---:|---:|
| JSON Extract/1024/first | -1.94% | -1.15% |
| JSON Extract/1024/multiple | -2.24% | -2.42% |
| JSON Extract/102400/first | -21.16% | -21.96% |
| JSON Extract/102400/multiple | -34.56% | -41.86% |
| JSON Patch - Remove Property/Limbo | +2.96% | +0.15% |
| JSON Patch - Simple Property Update/Limbo | +4.08% | +2.35% |
| JSONB Size - Medium/Limbo | -1.36% | -5.00% |
| JSONB Size - Small/Limbo | +5.00% | +0.32% |

Large extraction generally improves in this run for both build pairs, so the earlier canonical-host reversal is not a stable property of the canonical executable alone. However, severe per-repeat variation remains: original candidate 100KiB first ranges 18.586–62.009us; canonical candidate ranges 18.616–52.121us. This prevents an unconditional performance acceptance claim and does not establish a specific causal explanation. Small-size original-pair regression remains +5%, and simple patch is +4.08% original / +2.35% canonical. No observations were discarded.

Allocation savings and semantic checks remain independent evidence. Neither these medians nor the prior favorable run erase regressions. Further changes require a source-based hypothesis and matched measurements; do not tune blindly against unstable timings.
