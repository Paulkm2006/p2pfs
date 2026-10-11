# Controlled scheduler comparison

## Predeclared protocol

This protocol and its gated test are committed before collecting any performance results. It compares the existing `Config.FixedScheduler` baseline with the unchanged adaptive scheduler. It is a controlled application-rate fixture on real local UDP, not a WAN, network-emulation, packet-loss, or cross-NAT benchmark.

- Each scenario has five paired rounds. Odd rounds run fixed then adaptive; even rounds run adaptive then fixed. There are no discarded warmup rounds and no selective reruns.
- Every run reconstructs a deterministic 100 MiB (104,857,600-byte) sparse-zero source with small nonzero markers, using the existing 4+2 Reed–Solomon codec. All bytes are actually encoded, transmitted over encrypted UDP, reconstructed, and SHA-256 verified. There is no compression and no whole-file memory buffer.
- Three source peers each publish the identical source and hold all six shard indices. The same three source UUIDs, shard placement, and configurations remain in place throughout each scenario.
- In the heterogeneous scenario, the lexicographically first source UUID is predeclared as degraded with an aggregate upload limit of 8 MiB/s (8,388,608 bytes/s). The other two peers are unlimited. Source transports are recreated with persisted UUIDs and their intended rates before publication. This explicitly stresses the fixed policy's preference for the first peer when choosing the final shard; it is not a random or representative sample of heterogeneous networks.
- In the uniform scenario, all three peers have the same unlimited upload configuration. No OS network settings are changed in either scenario.
- Both policies use concurrency 3. Both currently spread their first three choices across the three peers. Their fourth-shard choice can differ after the adaptive policy learns the initial transfer rates.
- Every run uses the same downloader UUID with a fresh transport, empty shard cache, empty learned scores, and pristine task history. Source encoding and downloader registration are outside the timed interval. A 200 ms settle interval follows every run.
- Elapsed time starts immediately before `StartDownload` and ends when the task is observed complete, polled every 10 ms. It includes discovery, connection establishment, actual shard transfer, cache verification, reconstruction, persistence, and the application's final integrity check. An additional independent streamed SHA-256 check is performed outside the measured interval.
- Every successful run must have exactly four successful direct transfers of 26,214,400 bytes and a payload count of 104,857,600 bytes. The test checks that exactly three complete source holders are discoverable before every run. A run has a 90-second deadline; failures are retained and reported.
- Report every raw elapsed time, completed counts, minimum, maximum, median, median absolute deviation (MAD), and application payload bytes. The primary effect is `100 × (1 − adaptive median / fixed median)`. The heterogeneous goal is at least 10% improvement. The uniform goal is no more than 10% regression. Goals are reported honestly; environmental timing alone is not made a default CI pass/fail assertion.
- The run uses the existing explicit loopback-interface fixture when this cloud container denies host interface enumeration. Real UDP sockets, ICE, DTLS, and SCTP remain in use. Neither successful network simulation nor the formal prescribed hardware environment is implied.

## Reproduction

```sh
export PATH=/workspace/shared/toolchains/go/bin:$PATH
export GOMODCACHE=/workspace/shared/gomodcache
export GOPATH=/workspace/shared/gopath
export GOCACHE=/workspace/shared/gocache
go test -c -o /tmp/p2pfs-scheduler.test ./internal/client
P2PFS_SCHEDULER_EXPERIMENT=1 /tmp/p2pfs-scheduler.test \
  -test.run '^TestSchedulerControlledComparison$' -test.v -test.timeout=20m
```

Without `P2PFS_SCHEDULER_EXPERIMENT=1`, ordinary test runs skip this resource comparison. Each JSON `SCHEDULER_RESULT` record includes scenario, pair number, run position, policy, elapsed time, completion state, payload bytes, and individual transfer records. Fixture logs identify the source UUIDs and actual configured rates without exposing identity recovery secrets.

## Results

The first full measurement run on 2026-10-11 completed **20/20 downloads**. Both predeclared local-fixture goals passed:

- Heterogeneous: fixed median **9.231941824 s**, adaptive median **5.649136876 s**; **38.808790% faster**, exceeding the 10% target.
- Uniform: fixed median **2.430562477 s**, adaptive median **2.438228128 s**; **0.315386% slower**, within the permitted 10% regression.

These are controlled-fixture results for NFR-004. They do not establish the same effect in a WAN, on the prescribed 2-core/2-GiB host, for arbitrary source placement, or under loss, reordering, duplication, or NAT failures. The predeclared degraded first UUID deliberately creates a case where measured source selection should help. The implementation and scenario were not changed after observing timings. No rounds were discarded or rerun.

### Execution and environment

- Protocol/test commit: `3319c1358bb1c778b5cb0802f2ebe7388309a609`, based on backend commit `3eb3b35`.
- UTC interval: 2026-10-11 05:57:30.731957 to 05:59:19.013429. The Go test reported 108.27 seconds in total, including fixture setup and cleanup; heterogeneous 79.01 seconds and uniform 29.26 seconds.
- Go 1.27.2, Linux amd64, kernel 6.18.44, reported CPU Intel Xeon Platinum 8573C, nine visible logical CPUs, `GOMAXPROCS=9`. `/proc/meminfo` reported 10,206,504 KiB total memory. These are guest observations, not a controlled hardware allocation.
- Local overlay filesystem, 32 GiB reported capacity and approximately 28 GiB free before the run. Sources, shards, reconstructed files, coordinator, and all peers were in the same cloud container and test process. The system page cache was not flushed; source data was already read during publication, and run order was alternated.
- Real loopback UDP with the existing test-only explicit-interface adapter. Host enumeration returned `netlinkrib: operation not permitted`. There was no netem or OS network modification and no relay in measured transfers.
- The test binary was compiled before measurement, without the race detector. Python `resource.getrusage(RUSAGE_CHILDREN)` around only the binary reported 47,148 KiB peak RSS, 126.395045 CPU-user seconds, and 48.929793 CPU-system seconds. This includes all test peers and the coordinator but no desktop GUI; it is supplementary context, not a GUI memory acceptance result.
- No deliberate competing load was part of the experiment. This was a shared cloud environment, so unrelated host scheduling and background noise were not controlled.
- The command in Reproduction exited 0. Ordinary gated-skip compilation passed before the timing run. The fixture/test was not altered after that run.

### Immutable file and source configuration

- Source size: 104,857,600 bytes (100 MiB).
- Source SHA-256: `1dce696a5930f3e5005ef8bf0b40ed58a9bf788130a9ebeb4f204d0369233b8e`.
- Manifest ID: `babaaa5b55a61f721ea1463b8f40a5c7e831d530eb4609004054f172539ff092`.
- Each source held indices `[0,1,2,3,4,5]`. Each run received indices `[0,1,2,3]`, with four transfers of 26,214,400 bytes. Every output passed the independent full-size streamed SHA-256 check.

| Scenario | Source rank | Source UUID | Upload cap, bytes/s |
| --- | ---: | --- | ---: |
| Heterogeneous | 0 | `70010788-aab1-41d1-8779-f0a8c0ed2916` | 8388608 |
| Heterogeneous | 1 | `843e3c1d-3143-4848-9e77-e8c450eb2e20` | 0 (unlimited) |
| Heterogeneous | 2 | `acb91dee-30f0-4b3d-8736-0ce490fd2724` | 0 (unlimited) |
| Uniform | 0 | `a7377e95-b0bf-46a2-9979-fb1a2effefb1` | 0 (unlimited) |
| Uniform | 1 | `ae772b2b-15e1-479c-ad00-5728c449ee9f` | 0 (unlimited) |
| Uniform | 2 | `eded8e50-d9f9-4903-bb8c-7ce853cffe03` | 0 (unlimited) |

The downloader UUID was `619d4bd8-87eb-433c-9395-e4a6549878b2` throughout the heterogeneous scenario and `9ed3af50-d834-42cb-97e8-2451a58a5ebe` throughout the uniform scenario. Source and downloader identities were constant within every pair and every scenario; independent scenarios used independent coordinators and identities.

### Raw paired elapsed times

Positive paired improvement means adaptive was faster. The primary reported effect uses the ratio of the two policy medians, as declared before the run; it is not the median of the paired percentages.

| Scenario | Round | Order | Fixed seconds | Adaptive seconds | Paired improvement |
| --- | ---: | --- | ---: | ---: | ---: |
| Heterogeneous | 1 | fixed → adaptive | 9.240410022 | 5.649136876 | +38.864868% |
| Heterogeneous | 2 | adaptive → fixed | 9.225184600 | 5.677239588 | +38.459339% |
| Heterogeneous | 3 | fixed → adaptive | 9.034596423 | 5.650629365 | +37.455653% |
| Heterogeneous | 4 | adaptive → fixed | 9.269704407 | 5.648291391 | +39.067190% |
| Heterogeneous | 5 | fixed → adaptive | 9.231941824 | 5.634807065 | +38.964010% |
| Uniform | 1 | fixed → adaptive | 2.424350753 | 2.230372810 | +8.001233% |
| Uniform | 2 | adaptive → fixed | 2.425180560 | 2.438228128 | -0.538004% |
| Uniform | 3 | fixed → adaptive | 2.720844071 | 2.454113406 | +9.803232% |
| Uniform | 4 | adaptive → fixed | 2.430562477 | 2.432500289 | -0.079727% |
| Uniform | 5 | fixed → adaptive | 2.669017379 | 2.440746052 | +8.552635% |

Every row represents two completed, verified downloads. Each individual run transferred 104,857,600 application payload bytes; each scenario/policy group transferred 524,288,000 bytes, and all 20 runs transferred **2,097,152,000 bytes** over 80 successful direct shard transfers. There were no failed or retried shard transfers. These counters exclude framing, encryption, signaling, and transport retransmissions; they are not total wire-byte counters.

### Dispersion and goal calculation

All values below are seconds. MAD is the median absolute deviation from that group's median. Nine decimal places preserve the emitted measurements, but polling granularity is 10 ms and should not be confused with nanosecond measurement accuracy.

| Scenario | Policy | Completed | Median | Minimum | Maximum | MAD |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Heterogeneous | fixed | 5/5 | 9.231941824 | 9.034596423 | 9.269704407 | 0.008468198 |
| Heterogeneous | adaptive | 5/5 | 5.649136876 | 5.634807065 | 5.677239588 | 0.001492489 |
| Uniform | fixed | 5/5 | 2.430562477 | 2.424350753 | 2.720844071 | 0.006211724 |
| Uniform | adaptive | 5/5 | 2.438228128 | 2.230372810 | 2.454113406 | 0.005727839 |

- Heterogeneous: `100 × (1 − 5.649136876 / 9.231941824) = 38.808790%`, so the ≥10% goal passed.
- Uniform: `100 × (2.438228128 / 2.430562477 − 1) = 0.315386%` regression, so the ≤10% regression goal passed.
- As secondary descriptive statistics, the median paired improvement was 38.864868% for heterogeneous sources and 8.001233% for uniform sources. The latter differs from the primary metric because fixed-policy uniform times had two slower observations; those observations are retained above. The preregistered primary metric remains a 0.315386% uniform regression.
- Five pairs give limited evidence about variability and no broad statistical or population-level guarantee. The small uniform median difference is below the completion-polling interval and is best interpreted as no material observed regression in this run.

### Raw transfer durations and final source choices

The first three indices always used source ranks 0, 1, and 2 respectively. The table retains the reported per-transfer durations, including connection establishment and verification, in index order `[0,1,2,3]`. Each duration is the application's integer-millisecond field; it is not a network-only latency measurement. The fourth index is where the policy can use the initial rate measurements.

| Scenario | Round | Policy | Durations for indices 0,1,2,3, ms | Fourth-shard source rank |
| --- | ---: | --- | --- | ---: |
| Heterogeneous | 1 | fixed | 4236, 837, 843, 4345 | 0 |
| Heterogeneous | 1 | adaptive | 4232, 841, 847, 758 | 1 |
| Heterogeneous | 2 | adaptive | 4245, 899, 885, 757 | 2 |
| Heterogeneous | 2 | fixed | 4252, 888, 902, 4318 | 0 |
| Heterogeneous | 3 | fixed | 4186, 766, 956, 4200 | 0 |
| Heterogeneous | 3 | adaptive | 4264, 825, 832, 732 | 1 |
| Heterogeneous | 4 | adaptive | 4215, 868, 866, 773 | 2 |
| Heterogeneous | 4 | fixed | 4308, 818, 817, 4302 | 0 |
| Heterogeneous | 5 | fixed | 4239, 800, 971, 4331 | 0 |
| Heterogeneous | 5 | adaptive | 4245, 847, 832, 730 | 2 |
| Uniform | 1 | fixed | 963, 958, 943, 744 | 0 |
| Uniform | 1 | adaptive | 940, 947, 939, 552 | 2 |
| Uniform | 2 | adaptive | 917, 931, 924, 741 | 0 |
| Uniform | 2 | fixed | 950, 950, 951, 720 | 0 |
| Uniform | 3 | fixed | 930, 947, 945, 1006 | 0 |
| Uniform | 3 | adaptive | 939, 924, 934, 759 | 1 |
| Uniform | 4 | adaptive | 931, 943, 936, 719 | 0 |
| Uniform | 4 | fixed | 967, 966, 962, 714 | 0 |
| Uniform | 5 | fixed | 1196, 988, 1004, 816 | 0 |
| Uniform | 5 | adaptive | 973, 967, 971, 688 | 1 |

In all five heterogeneous pairs, the fixed policy fetched the fourth shard from the predeclared limited source, while adaptive chose an unlimited source. Both policies still waited for the slower source in the first three-shard batch. This observed final-source change explains the controlled improvement; it does not prove arbitrary slow-path replacement within ten seconds.

The original full console output, including JSON records with UUIDs and start timestamps, has SHA-256 `f6101417cd6c97a968fcc329487c995df11a45fd64bd5062a70804481ff6e566`. Its transfer timestamps use the process's UTC+08:00 local timezone; the execution interval above is normalized to UTC.
