# Step 9 - Real processes, `kill -9`, and measurements

## What we are building
`scripts/demo-groups.sh` (the narrated demo), `scripts/bench-phase6.sh` (measurements A-D, `ONLY=AC` runs a subset) and `scripts/lib-groups.sh` (helpers). Everything uses **real shaded jars**: three broker processes, and consumers that are real `consume --group` processes. New CLI tools exist only to make the measurements honest and precise:
`produce --count=N` (one producer, N keyed records), `consume --print-time`, `group watch` (a 50 ms observer that prints a timestamped line whenever generation / members / owned partitions / committed offsets change), `group commit-probe` (commits every 20 ms and prints each outcome), `group commit-bench`, `consume-bench`.

## Why it is designed this way
A process killed with `kill -9` gets no shutdown hook: no commit, no leave, sockets reset by the kernel. That is the honest crash. Timing from inside the JVM that is being killed is impossible, so a **separate observer process** with its own clock reads the state of the group every 50 ms, and consumers print a wall-clock timestamp with every record (`--print-time`).

* (A) **Consumer kill -9 -> reassignment.** The burst of new records is produced *after* the kill, so records for the dead consumer's partitions cannot be delivered until the partitions move. "Reassigned" = first observer line with 2 members owning all 6 partitions. "Consumption resumed" = first printed record from the dead consumer's old partitions at a surviving consumer.
* (B) **Coordinator kill -9 -> commits.** A probe consumer (own group) commits every 20 ms. We report the first successful commit that *started* after the kill, and the longest gap between two successful commits.
* (C) **Read throughput**: one group consumer versus the manual `EmberConsumer` over 200,000 x 100 B records in a 6-partition topic on 3 brokers; the clock starts after the first batch (join and offset fetch excluded) and stops *before* `close()`.
* (D) **OFFSET_COMMIT latency** with Raft fsync ON and with `--unsafe-no-raft-fsync`: 2,000 synchronous commits.

## Alternatives I rejected and why
* Timing with `sleep` + `date` inside shell loops: JVM start-up (about 0.3 s per CLI call) would swamp the numbers. The observers are long-running JVMs.
* Killing consumers with SIGTERM: they would commit and leave, which hides the session timeout we want to measure.

## What goes wrong if you get it wrong (and did)
Three mistakes of my own tooling were found by looking at suspicious numbers (see the bug notes): the burst produced before the kill (nothing to resume), the throughput clock including `close()` (two fsynced Raft writes inflating the "cost of auto-commit"), and a first monitor in the chaos test. All were fixed at the root.

## Tradeoffs and honest limits
One laptop, all processes on one machine, one disk. Machine: **Apple M4, 10 cores, 16 GiB RAM, macOS 27.0.1, Java 21.0.12**, local SSD. The read-throughput window is only 0.11-0.15 s per run, so treat differences below ~5% as noise. The first run of B was twice as slow as the other four and was not investigated.

## How it is tested
By running it. Results:

**Demo** (`scripts/demo-groups.sh`): 3,000 keyed records on 6 partitions; 3 consumer processes own 2 partitions each; all 3,000 distinct values delivered, lag 0. `kill -9` of consumer c3 while 600 records arrive: the two survivors take 3 partitions each (generation 3 -> 4), lag reaches 0, 3,600 of 3,600 distinct values delivered. `kill -9` of the coordinator broker (node 2) with 300 more records: a new coordinator (node 3) takes over, the group stays at generation 4 with the same members, lag 0, 3,900 of 3,900 delivered. After a graceful stop, a **fresh consumer in the same group read 0 records**; after 10 more records it read exactly those 10.

**A - consumer kill -9** (session timeout 3000 ms, heartbeat 1000 ms, coordinator expiry tick 750 ms, 5 runs):

| | min | median | max |
|---|---|---|---|
| partitions reassigned (observer) | 2,180 ms | 2,577 ms | 2,953 ms |
| first record from the dead consumer's partitions delivered to a survivor | 2,601 ms | 2,713 ms | 3,669 ms |

Reassignment takes less than the full session timeout because the last heartbeat arrived up to one heartbeat interval before the kill.

**B - coordinator broker kill -9** (default Raft timings: election 300-600 ms, heartbeat 100 ms; 5 runs):

| | min | median | max |
|---|---|---|---|
| first commit started after the kill that succeeded | 740 ms | 760 ms | 1,570 ms |
| longest gap between two successful commits | 758 ms | 760 ms | 1,564 ms |

**C - reading 200,000 records** (3 repetitions, steady state):

| Consumer | records/s |
|---|---|
| manual `EmberConsumer` | 1.40-1.45 million |
| group consumer, auto-commit every 5 s (no commit fires inside the window) | 1.32-1.35 million |
| group consumer, no commit | 1.33-1.34 million |
| group consumer, auto-commit every 100 ms (a few commits inside the window) | 1.25 million |

The group consumer is a little slower than the manual one mainly because its poll budget (`maxPollBytes`, 1 MiB per poll in total) is smaller than the manual consumer's 1 MiB per partition. Committing every 100 ms costs about 6%.

**D - OFFSET_COMMIT latency** (2,000 synchronous commits, 3 brokers):

| Raft fsync | p50 | p90 | p99 | max | commits/s |
|---|---|---|---|---|---|
| ON (default) | 8.34 ms | 11.89 ms | 13.26 ms | 17.05 ms | 107 |
| OFF (`--unsafe-no-raft-fsync`) | 0.26 ms | 0.45 ms | 0.74 ms | 1.04 ms | 3,432 |

An offset commit is a fsynced, replicated Raft write, so it costs one disk flush on a majority. That is why consumers commit at intervals and not per record.
