# Step 9 - Final benchmarks

## What we are building
Six measurements, all with **real broker processes** (not in-process, not Docker) and the real CLI, written by `scripts/bench-phase7.sh`. Raw files (one CSV with one row per measured request, one text file per run) are in `docs/benchmarks/`; `docs/benchmarks/RESULTS.txt` is the whole log.

## Why it is designed this way
A benchmark number is only useful with its recipe. Every run: one fresh cluster, 4 producer threads each with its own connection, **batches of 100 records** (one `sendBatch` call = one measured request), the first 2,000 records are warm-up and excluded, the read-back verification must pass ("every partition contiguous"), and errors and retries are counted. Latency is **per request of up to 100 records** (not per record) measured with our own histogram (within about 3%).

**Machine:** Apple M4, 10 cores, 16 GiB RAM, macOS 27.0.1, Java 21.0.12, one local SSD. **All three brokers and the clients run on this one machine and share its one SSD.** This matters more than anything else below.

## The numbers (all measured; one run per cell unless stated)

### A and B - 3 brokers, 4 producers, closed loop

| Raft fsync | value | partitions | records/s | MB/s | p50 | p90 | p99 | p99.9 |
|---|---|---|---|---|---|---|---|---|
| **ON** | 100 B | 3 | 3,491 | 0.48 | 114.7 ms | 135.2 ms | 151.6 ms | 151.6 ms |
| **ON** | 100 B | 12 | 1,031 | 0.14 | 385.0 ms | 417.8 ms | 458.8 ms | 484.0 ms |
| **ON** | 1 KB | 3 | 3,841 | 4.08 | 106.5 ms | 124.9 ms | 151.6 ms | 170.6 ms |
| **ON** | 1 KB | 12 | 1,222 | 1.30 | 335.9 ms | 409.6 ms | 466.9 ms | 475.4 ms |
| off (`--unsafe-no-raft-fsync`) | 100 B | 3 | 126,680 | 17.48 | 2.56 ms | 5.25 ms | 8.96 ms | 9.82 ms |
| off | 100 B | 12 | 100,772 | 13.91 | 3.78 ms | 4.48 ms | 7.30 ms | 16.29 ms |
| off | 1 KB | 3 | 101,485 | 107.78 | 3.65 ms | 5.12 ms | 7.17 ms | 14.62 ms |
| off | 1 KB | 12 | 73,236 | 77.78 | 5.12 ms | 6.78 ms | 11.26 ms | 12.31 ms |

Every run: 0 failed requests, 12-16 retried requests (the first requests race the leader elections of the new partitions), read-back verified.

### C - one broker versus three (100 B, 3 partitions, 4 producers)

| | Raft fsync ON | fsync off |
|---|---|---|
| 1 broker | 9,845 records/s (p50 41.0 ms, p99 51.2 ms) | 272,292 records/s (p50 1.28 ms, p99 3.71 ms) |
| 3 brokers | 3,491 records/s (p50 114.7 ms, p99 151.6 ms) | 126,680 records/s (p50 2.56 ms, p99 8.96 ms) |
| 3 brokers vs 1 | 2.8x slower | 2.1x slower |

### D - fixed-rate latency (the latency knee). 3 brokers, fsync ON, single-record requests, 4 producers

Measured max with a closed loop and `--batch=1`: **117 records/s** (p50 33.8 ms, p99 54.3 ms). Then fixed-rate runs, latency measured from the **intended** send time:

| load | rate | records measured | p50 | p90 | p99 | p99.9 | max |
|---|---|---|---|---|---|---|---|
| 25% of max | 29/s | 522 | 29.7 ms | 37.9 ms | 44.0 ms | 48.9 ms | 48.9 ms |
| 50% | 58/s | 1,044 | 30.2 ms | 41.0 ms | 49.2 ms | 57.1 ms | 57.1 ms |
| 75% | 87/s | 1,566 | 28.7 ms | 36.9 ms | **143.4 ms** | **221.2 ms** | 233.6 ms |

The median does not move (about 30 ms) but the **tail jumps threefold between 50% and 75%**: that is the knee. A closed-loop run would never show it, because it only ever offers as much load as the system can take.

### E - failover: 20 x `kill -9` of the partition leader (3 brokers, fsync ON)
Time from the kill to the first successful produce that **started after** the kill (a client that retries through the outage):

| Raft timings | min | median | p90 | max | mean |
|---|---|---|---|---|---|
| default (election 300-600 ms, heartbeat 100 ms) | 583 ms | **770 ms** | 979 ms | 1,598 ms | 823 ms |
| election 150-300 ms, heartbeat 50 ms | 365 ms | **559 ms** | 569 ms | 574 ms | 551 ms |

### F - restart catch-up: a broker that missed 100,000 records (3 partitions, 100 B)
The broker was killed, 100,000 records were produced without it, then it was restarted. Time from the restart command until its admin port says ready **and** its partitions hold all records: **2,792 ms, 2,212 ms, 2,569 ms** (3 repetitions; this includes starting the JVM).

## What the numbers say (honest reading)
* **fsync is the cost.** With Raft fsync on, throughput is 30-35x lower and latency 40x higher than without. A single-record produce takes about 34 ms: the leader fsyncs before it sends, each follower fsyncs before it answers, and the leader then commits. **On this machine all three brokers share ONE SSD**, so one acknowledged produce is three fsyncs on the same device; the closed-loop maximum of 117 produces/s is about 350 fsyncs/s, which is the disk, not the code. On three separate machines with their own disks the replicas fsync in parallel and the number would look different. I did not measure that.
* **12 partitions are slower than 3, not faster**, in this setup. `EmberProducer.sendBatch` splits a batch by partition and sends **one request per partition, one after the other**; a batch of 100 random keys touches all 12 partitions, so one `sendBatch` is 12 sequential commits (385 ms / 12 = 32 ms each; with 3 partitions 115 ms / 3 = 38 ms each). More partitions would help with a producer that pipelines requests; ours does not. A real finding, not fixed here.
* **Replication costs 2-3x** (C): it is not "free durability". Three copies need two extra network hops and two extra fsyncs per commit.
* **Failover is dominated by the election timeout.** The median of 770 ms with the default timings (300-600 ms) falls to 559 ms with 150-300 ms. The rest is the client noticing the dead connection and its retry back-off. A failover is one election, not a cluster-wide event.
* **The 100 B and 1 KB runs with fsync on are almost the same** in records/s, because the commit latency, not the bytes, is the limit; only without fsync does 1 KB cost more (101k vs 127k records/s).

## Alternatives I rejected and why
* Benchmarking inside one JVM: it would hide the network, the OS page cache and the process boundaries.
* Reporting the best of several runs: the table shows one run per cell, and where I repeated (F) I show all runs.
* Larger batches to make the numbers look better: the batch size is stated (100) and the single-record case (D) is measured separately.

## Tradeoffs and honest limits
One laptop, one disk, a few minutes per run, one repetition per cell (so differences under about 10-15% are not meaningful). The failover times include the client library's own retry policy. Nothing was tuned: default flags except where stated.

## How it is tested
By running `scripts/bench-phase7.sh` (about 25 minutes). The tools behind it are unit-tested: `LatencyHistogramTest`, `OpenLoopScheduleTest` (with a manual clock), `BenchCliTest`, `FailoverBenchTest`.
