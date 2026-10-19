# Phase 3 benchmarks: what partitions buy you (and what they do not)

Every number below was measured by running the real shaded jars (`ember-broker-0.1.0-SNAPSHOT-all.jar` and `ember-client-0.1.0-SNAPSHOT-all.jar`) with the CLI `bench` command. Nothing is estimated.

## Machine and method
* Apple M4, 10 cores, 16 GB RAM, macOS (Darwin 27), internal SSD, APFS, JDK 21.0.12.
* Broker and client ran on the **same machine** over loopback and compete for the same CPU.
* Every run started a **fresh broker with an empty data directory**; the topic is created by the bench with `--partitions=N`.
* Records: 100-byte values, 10-byte random keys (`key-123456`), batches of 500, `P` producer threads each with its **own** `EmberProducer` and connection. Producer count equals partition count in every run (1/1, 4/4, 8/8).
* "MB/s" counts whole encoded records (28-byte header + key + value), the same way in both directions.
* After producing, one `EmberConsumer` reads back exactly what the run wrote and the bench **verifies** the count and that every partition's offsets are contiguous. All runs printed `verify: OK`.
* The "read" figure includes the client decoding every record and checking its CRC, and the data is still in the OS page cache: it measures the network/zero-copy path, not the disk.

Commands (one run each):
```
java -jar ember-broker/target/ember-broker-0.1.0-SNAPSHOT-all.jar --port=19099 --data-dir=<fresh dir> [broker flags]
java -jar ember-client/target/ember-client-0.1.0-SNAPSHOT-all.jar bench --port=19099 --topic=bench \
     --partitions=N --producers=N --records=R --value-bytes=100 --batch=500
```

## A) `--flush-messages=1 --flush-ms=never` (fsync after every message), 20,000 records

| partitions x producers | produce records/s | produce MB/s | read records/s | read MB/s | produce time |
|---|---|---|---|---|---|
| 1 x 1 | 241 | 0.03 | 254,864 | 35.17 | 82.84 s |
| 4 x 4 | 292 | 0.04 | 284,554 | 39.27 | 68.50 s |
| 8 x 8 | 287 | 0.04 | 281,983 | 38.91 | 69.66 s |

## B) default flush (`--flush-ms=1000`), 200,000 records (3 repetitions each)

| partitions x producers | produce records/s (run 1 / 2 / 3) | produce MB/s (run 1 / 2 / 3) | read records/s (run 1) | read MB/s (run 1) |
|---|---|---|---|---|
| 1 x 1 | 238,957 / 236,383 / 250,564 | 32.98 / 32.62 / 34.58 | 972,484 | 134.20 |
| 4 x 4 | 334,442 / 356,529 / 359,091 | 46.15 / 49.20 / 49.55 | 978,195 | 134.99 |
| 8 x 8 | 290,319 / 306,734 / 329,143 | 40.06 / 42.33 / 45.42 | 948,081 | 130.84 |

(Repetitions 2 and 3 were produce-only comparisons; run-to-run noise on this laptop is roughly +-10%.)

## A side experiment: how many fsyncs per second can this disk do?
To explain A, a tiny stand-alone program (not part of Ember) had N threads each write 128 bytes to **its own file** and call `FileChannel.force(false)` (the same call the log uses), 300 times per thread:

| threads | fsyncs/s |
|---|---|
| 1 | 271 |
| 4 | 341 |
| 8 | 567 |

## What the numbers show

**Partitions removed the lock bottleneck, but in A the disk is the limit.** With fsync after every message, one producer on one partition manages 241 records/s - one record per ~4 ms, the cost of a physical fsync on this machine (Phase 2 measured the same ~4 ms per fsync). Every partition is its own `Log` with its own lock, so an fsync on partition 0 does not stop partition 1 from appending; that property is tested for correctness (`fsyncPerMessageOnFourPartitionsWithFourWritersLosesNothing`). Yet 4 and 8 partitions only reached 292 and 287 records/s: **+20%, not 4x or 8x**. The side experiment explains why: this SSD does not execute independent fsyncs fully in parallel - four threads fsyncing four different files got only 1.26x the fsyncs/s of one thread, eight threads 2.1x. The device, not Ember's locks, sets the ceiling.

**Why A scales even less than the raw disk (287 vs 567 fsyncs/s at 8).** Two Ember-specific reasons that follow from the design (we reason about them; we did not isolate them with further experiments):
1. Each bench producer uses *random keys*, so every producer sends records to *every* partition. Producers therefore queue on the same partition locks, and a lock is held for the whole fsync-per-message batch. Producers that each owned their own partition would overlap better.
2. A producer has **one connection with one request in flight** and sends its partitions one after another, so one producer can never have more than one fsync running.

**In B (fsync once a second) partitions help the CPU side.** 1 -> 4 partitions/producers raised produce throughput from ~240k to ~345k records/s (+40-50%, well outside the +-10% noise). 8 was not better than 4 (290-329k): the machine has 10 cores shared by 8 producer threads, the broker's I/O and handler threads and the JVMs' GC, and all of it competes on one box over loopback. Beyond a point more partitions/producers just add contention. We did not profile it further, so "CPU/scheduling on a single machine" is the likely limit, not a measured one.

**Reading is the same for any partition count (~950k-980k records/s, ~130-135 MB/s).** The consumer is a single connection that reads the partitions one at a time, so extra partitions do not speed it up; the zero-copy path plus client-side CRC checking is the limit. Reading in parallel (one consumer per partition) is future work (groups, Phase 6).

**Compared with Phase 2** (`1 partition, 1 producer, null keys`: ~320k records/s): this Phase 3 run with 1 x 1 is slower (~240k). The Phase 3 bench does more client work per record (it formats a random key with `String.format`, hashes it with murmur2 and groups by partition), and its records are 10 bytes bigger. We did not separate these effects, so treat the two benches as not directly comparable.

## Limits to remember
* One machine, loopback, one disk: no network, no replication.
* The fsync-per-message runs use 20,000 records because 200,000 would take about 12 minutes.
* One run per cell in A and run 1 for read figures; B produce figures were repeated three times.
