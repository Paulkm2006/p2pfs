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

Pending collection under the committed protocol above.
