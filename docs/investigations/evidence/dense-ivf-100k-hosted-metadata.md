# Hosted 100k run metadata

Run: https://github.com/HundredBillion/turso/actions/runs/42244303993

Source commit: `5f3a98f0bab60439dfc9b17f2c9aa31ceb777b67`.
Validation revision: `736f704d73e23d7c85f543cd798e5967e2fb7245`.
The diff between these revisions contains only the validation workflow.

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
c7c3cf6a55b79ad932be757bd6e41f10ac2842e2e30716a8a628b25c3113acd2  /home/runner/work/turso/turso/target/debug/deps/integration_tests-e809222c3dd489f0
```

## cpu.txt

```text
Architecture:                            x86_64
CPU op-mode(s):                          32-bit, 64-bit
Address sizes:                           48 bits physical, 48 bits virtual
Byte Order:                              Little Endian
CPU(s):                                  4
On-line CPU(s) list:                     0-3
Vendor ID:                               AuthenticAMD
Model name:                              AMD EPYC 9V74 80-Core Processor
CPU family:                              25
Model:                                   17
Thread(s) per core:                      2
Core(s) per socket:                      2
Socket(s):                               1
Stepping:                                1
BogoMIPS:                                5192.26
Flags:                                   fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush mmx fxsr sse sse2 ht syscall nx mmxext fxsr_opt pdpe1gb rdtscp lm constant_tsc rep_good nopl xtopology tsc_reliable nonstop_tsc cpuid extd_apicid aperfmperf tsc_known_freq pni pclmulqdq ssse3 fma cx16 pcid sse4_1 sse4_2 movbe popcnt aes xsave avx f16c rdrand hypervisor lahf_lm cmp_legacy svm cr8_legacy abm sse4a misalignsse 3dnowprefetch osvw topoext vmmcall fsgsbase bmi1 avx2 smep bmi2 erms invpcid avx512f avx512dq rdseed adx smap avx512ifma clflushopt clwb avx512cd sha_ni avx512bw avx512vl xsaveopt xsavec xgetbv1 xsaves user_shstk avx512_bf16 clzero xsaveerptr rdpru arat npt nrip_save tsc_scale vmcb_clean flushbyasid decodeassists pausefilter pfthreshold v_vmsave_vmload avx512vbmi umip avx512_vbmi2 gfni vaes vpclmulqdq avx512_vnni avx512_bitalg avx512_vpopcntdq rdpid fsrm
Virtualization:                          AMD-V
Hypervisor vendor:                       Microsoft
Virtualization type:                     full
L1d cache:                               64 KiB (2 instances)
L1i cache:                               64 KiB (2 instances)
L2 cache:                                2 MiB (2 instances)
L3 cache:                                32 MiB (1 instance)
NUMA node(s):                            1
NUMA node0 CPU(s):                       0-3
Vulnerability Gather data sampling:      Not affected
Vulnerability Ghostwrite:                Not affected
Vulnerability Indirect target selection: Not affected
Vulnerability Itlb multihit:             Not affected
Vulnerability L1tf:                      Not affected
Vulnerability Mds:                       Not affected
Vulnerability Meltdown:                  Not affected
Vulnerability Mmio stale data:           Not affected
Vulnerability Old microcode:             Not affected
Vulnerability Reg file data sampling:    Not affected
Vulnerability Retbleed:                  Not affected
Vulnerability Spec rstack overflow:      Vulnerable: Safe RET, no microcode
Vulnerability Spec store bypass:         Vulnerable
Vulnerability Spectre v1:                Mitigation; usercopy/swapgs barriers and __user pointer sanitization
Vulnerability Spectre v2:                Mitigation; Retpolines; STIBP disabled; RSB filling; PBRSB-eIBRS Not affected; BHI Not affected
Vulnerability Srbds:                     Not affected
Vulnerability Tsa:                       Vulnerable: No microcode
Vulnerability Tsx async abort:           Not affected
Vulnerability Vmscape:                   Not affected
```

## resources.log

```text
	Command being timed: "/home/runner/work/turso/turso/target/debug/deps/integration_tests-e809222c3dd489f0 dense_ivf_recall_probe --ignored --nocapture --test-threads=1"
	User time (seconds): 9.75
	System time (seconds): 0.72
	Percent of CPU this job got: 88%
	Elapsed (wall clock) time (h:mm:ss or m:ss): 0:11.86
	Average shared text size (kbytes): 0
	Average unshared data size (kbytes): 0
	Average stack size (kbytes): 0
	Average total size (kbytes): 0
	Maximum resident set size (kbytes): 84972
	Average resident set size (kbytes): 0
	Major (requiring I/O) page faults: 0
	Minor (reclaiming a frame) page faults: 92336
	Voluntary context switches: 354
	Involuntary context switches: 72
	Swaps: 0
	File system inputs: 0
	File system outputs: 615856
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
test index_method::dense_ivf::dense_ivf_write_fault ... 2026-11-06T15:41:24.385919Z ERROR turso_core::statement: Error while draining pending IO during statement reset: I/O error (queued_io): other error
2026-11-06T15:41:24.385955Z ERROR turso_core::statement: Statement reset failed during best-effort cleanup: I/O error (queued_io): other error
ok

test result: ok. 7 passed; 0 failed; 1 ignored; 0 measured; 1323 filtered out; finished in 0.95s

```
