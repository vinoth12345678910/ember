# Step 9 - Real processes, `kill -9`, and benchmarks

## What we are building
`scripts/run-cluster.sh` / `stop-cluster.sh` (start and stop N real shaded broker processes with separate data dirs and ports), `scripts/demo-failover.sh` (the whole `kill -9` demo with the real CLI), and `scripts/bench-phase5.sh` (benchmarks A and B).

## Why it is designed this way
In-process tests share a JVM, a clock and the OS page cache. A `kill -9` of a real process is the honest crash: no shutdown hook, no flush, sockets reset by the kernel. Running the real CLI against real processes also tests things unit tests never touch: the shaded jar, the flag parsing, the process lifecycle, reconnect behaviour of separate processes.

The demo checks the promise end to end: it records every record the CLI printed an acknowledgement for (`partition=P offset=O`), kills the leader of partition 0 while a producer loop runs, then consumes everything and verifies that **every acknowledged value is present**.

## Alternatives I rejected and why
* Docker for the demo: not needed for Phase 5 and slower to iterate; plain processes show the same failure.
* `kill -15`: graceful, so it proves nothing about crash safety (the demo ends with `SIGTERM` only to show the clean-shutdown path).

## What goes wrong if you get it wrong
A demo that reports "recovered" when the first request merely *connected* would hide a stall; the measurement is the time to the first **acknowledged** produce to the killed leader's partition.

## Tradeoffs and honest limits
All processes run on one laptop, one disk; fsync cost therefore reflects this SSD only. Each CLI invocation starts its own JVM and takes about 0.07 s per call in the demo, so per-call times there say little about Ember itself; the bench numbers (one JVM, many records) are the meaningful ones.

## How it is tested
By running it. Results (Apple M4, 10 cores, 16 GiB, macOS 27.0.1, Java 21.0.12, local SSD; three broker processes plus the CLI on the same machine):

**Demo** (`scripts/demo-failover.sh`): 200/200 keyed records acknowledged; a producer loop of 301 calls during `kill -9` of the leader of partition 0: **301 ok, 0 failed**; first acknowledged produce to partition 0 **0.60 s** after the kill; consumed 501 records, **0 acknowledged records missing**; after restart all three brokers report identical end offsets (50 / 151 / 300 for the three partitions); SIGTERM shutdown: 3 x "Ember broker shut down cleanly".

**Benchmark A** (CLI `bench`, 4 partitions, 4 producers, 100-byte values, 50,000 records, batch 500, one run each, read-back verified):

| Setup | records/s | MB/s |
|---|---|---|
| 1 broker with Raft, fsync ON | 22,804 | 3.15 |
| 3 brokers, fsync ON | 10,858 | 1.50 |
| 3 brokers, `--unsafe-no-raft-fsync` | 42,063 | 5.80 |

Each run reports "retried requests during produce: 12": the first requests after topic creation race the leader elections of the new partition groups. One run per setup, so treat differences below ~20% as noise.

**Benchmark B** (failover, 1-partition topic, fsync ON, default Raft timings 300-600 ms election, 5 cycles of `kill -9` leader then restart): time to first acknowledged produce after the kill: 0.82, 0.83, 0.84, 0.84, 1.65 s -> **min 0.82 s, median 0.84 s, max 1.65 s**. (The 1.65 s cycle is a case where the client's first retry hit a broker that was still electing.)
