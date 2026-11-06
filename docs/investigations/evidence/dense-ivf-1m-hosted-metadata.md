# Hosted 1m run metadata

Run: https://github.com/HundredBillion/turso/actions/runs/42245689959

Source commit: `5f3a98f0bab60439dfc9b17f2c9aa31ceb777b67`.
Validation revision: `32100d4511d91bf1a0c2b133c807067191023c52`.
Only the validation workflow differs from the source commit.

## toolchain.txt

```text
rustc 1.88.0 (6b00bc388 2025-06-23)
binary: rustc
commit-hash: 6b00bc3880198600130e1cf62b8f8a93494488cc
commit-date: 2025-06-23
host: x86_64-unknown-linux-gnu
release: 1.88.0
LLVM version: 20.1.5
```

## binary.sha256

```text
20c24584cdb8fc1f49207796f6f2d6f94f42fa4d26a40d4b45664646bd753112  /home/runner/work/turso/turso/target/debug/deps/integration_tests-e809222c3dd489f0
```

## cpu.txt

```text
Architecture:                            x86_64
CPU op-mode(s):                          32-bit, 64-bit
Address sizes:                           48 bits physical, 57 bits virtual
Byte Order:                              Little Endian
CPU(s):                                  4
On-line CPU(s) list:                     0-3
Vendor ID:                               GenuineIntel
Model name:                              INTEL(R) XEON(R) PLATINUM 8573C
CPU family:                              6
Model:                                   207
Thread(s) per core:                      2
Core(s) per socket:                      2
Socket(s):                               1
Stepping:                                2
CPU(s) scaling MHz:                      135%
CPU max MHz:                             2300.0000
CPU min MHz:                             800.0000
BogoMIPS:                                4600.00
Flags:                                   fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush mmx fxsr sse sse2 ss ht syscall nx pdpe1gb rdtscp lm constant_tsc rep_good nopl xtopology tsc_reliable nonstop_tsc cpuid aperfmperf tsc_known_freq pni pclmulqdq vmx ssse3 fma cx16 pcid sse4_1 sse4_2 x2apic movbe popcnt tsc_deadline_timer aes xsave avx f16c rdrand hypervisor lahf_lm abm 3dnowprefetch tpr_shadow ept vpid fsgsbase tsc_adjust bmi1 hle avx2 smep bmi2 erms invpcid rtm avx512f avx512dq rdseed adx smap avx512ifma clflushopt clwb avx512cd sha_ni avx512bw avx512vl xsaveopt xsavec xgetbv1 xsaves vnmi avx512vbmi umip avx512_vbmi2 gfni vaes vpclmulqdq avx512_vnni avx512_bitalg avx512_vpopcntdq la57 rdpid fsrm arch_capabilities
Virtualization:                          VT-x
Hypervisor vendor:                       Microsoft
Virtualization type:                     full
L1d cache:                               96 KiB (2 instances)
L1i cache:                               64 KiB (2 instances)
L2 cache:                                4 MiB (2 instances)
L3 cache:                                260 MiB (1 instance)
NUMA node(s):                            1
NUMA node0 CPU(s):                       0-3
Vulnerability Gather data sampling:      Not affected
Vulnerability Ghostwrite:                Not affected
Vulnerability Indirect target selection: Mitigation; Aligned branch/return thunks
Vulnerability Itlb multihit:             Not affected
Vulnerability L1tf:                      Not affected
Vulnerability Mds:                       Not affected
Vulnerability Meltdown:                  Not affected
Vulnerability Mmio stale data:           Not affected
Vulnerability Old microcode:             Not affected
Vulnerability Reg file data sampling:    Not affected
Vulnerability Retbleed:                  Vulnerable
Vulnerability Spec rstack overflow:      Not affected
Vulnerability Spec store bypass:         Vulnerable
Vulnerability Spectre v1:                Mitigation; usercopy/swapgs barriers and __user pointer sanitization
Vulnerability Spectre v2:                Mitigation; Retpolines; STIBP disabled; RSB filling; PBRSB-eIBRS Not affected; BHI Retpoline
Vulnerability Srbds:                     Not affected
Vulnerability Tsa:                       Not affected
Vulnerability Tsx async abort:           Vulnerable: Clear CPU buffers attempted, no microcode; SMT Host state unknown
Vulnerability Vmscape:                   Not affected
```

## resources.log

```text
	Command being timed: "/home/runner/work/turso/turso/target/debug/deps/integration_tests-e809222c3dd489f0 dense_ivf_recall_probe --ignored --nocapture --test-threads=1"
	User time (seconds): 122.86
	System time (seconds): 8.35
	Percent of CPU this job got: 87%
	Elapsed (wall clock) time (h:mm:ss or m:ss): 2:29.73
	Average shared text size (kbytes): 0
	Average unshared data size (kbytes): 0
	Average stack size (kbytes): 0
	Average total size (kbytes): 0
	Maximum resident set size (kbytes): 582196
	Average resident set size (kbytes): 0
	Major (requiring I/O) page faults: 0
	Minor (reclaiming a frame) page faults: 989564
	Voluntary context switches: 762
	Involuntary context switches: 775
	Swaps: 0
	File system inputs: 0
	File system outputs: 9335128
	Socket messages sent: 0
	Socket messages received: 0
	Signals delivered: 0
	Page size (bytes): 4096
	Exit status: 0
```

## lifecycle.log

```text

running 8 tests
test index_method::dense_ivf::dense_ivf_checkpoint_crash ... ok
test index_method::dense_ivf::dense_ivf_concurrent_writers ... ok
test index_method::dense_ivf::dense_ivf_filters_before_limit ... ok
test index_method::dense_ivf::dense_ivf_lifecycle ... ok
test index_method::dense_ivf::dense_ivf_lifecycle_mvcc ... ok
test index_method::dense_ivf::dense_ivf_recall_probe ... ignored, 100k synthetic measurement; run alone and retain stdout
test index_method::dense_ivf::dense_ivf_rejects_invalid_configuration_and_query_shapes ... ok
test index_method::dense_ivf::dense_ivf_write_fault ... 2026-11-06T15:53:00.106888Z ERROR turso_core::statement: Error while draining pending IO during statement reset: I/O error (queued_io): other error
2026-11-06T15:53:00.106928Z ERROR turso_core::statement: Statement reset failed during best-effort cleanup: I/O error (queued_io): other error
ok

test result: ok. 7 passed; 0 failed; 1 ignored; 0 measured; 1323 filtered out; finished in 0.21s

```
