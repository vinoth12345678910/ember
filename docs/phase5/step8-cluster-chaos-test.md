# Step 8 - The cluster chaos test

## What we are building
`ClusterChaosTest`: 3 real brokers, 2 producer threads writing keyed sequence numbers (with retries), and a **chaos thread** that every 0.5-1.5 s randomly crashes or restarts ONE broker, isolates a broker from its peers, or cuts a single link, then heals - never more than one broker unavailable at a time, so a majority always exists. At the end everything is healed, restarted, and the audit runs. A short default variant (10 s) and a `slow` variant (60 s, Maven profile `cluster-long`). The random schedule is seeded and the seed is printed on failure.

## Why it is designed this way
**Hand-written scenarios test what you thought of; chaos tests what you did not.** Phase 4's simulator did this for the pure Raft core with *virtual* time. This test does it for the whole broker with *real* sockets, files, threads and timeouts, where bugs live in glue code: a connection that is never re-opened, a future that is never completed, a lock held across a network call, a lost wake-up of a timer.

**"Never more than one broker unavailable"** keeps the system within its promise. The test is not trying to prove liveness under majority loss (a separate test does that); it asks: under a stream of single failures, does every acknowledged record survive, in order, and do all replicas converge?

**The audit** (after healing, restarting everybody and waiting for catch-up):
1. every value the producers were *told* was stored is in the log;
2. per producer and per key, the acknowledged sequence numbers appear in increasing order (a key always lives in one partition);
3. per-partition offsets are contiguous from 0;
4. all replicas' data logs are byte-identical;
5. nothing is corrupt (every record is readable and CRC-verified when read).
Values from sends that failed may or may not be there; every attempt uses a fresh sequence number, so duplicates can only come from the client's internal retry of one attempt (at-least-once), which the audit tolerates.

**Seeded schedule, real-time execution.** The sequence of chaos actions comes from `Random(seed)`, but thread timing is real, so a rerun is *similar*, not identical. The seed plus the printed event log of actions makes a failure reproducible in spirit; the Raft-level exact replay is the simulator's job.

## Alternatives I rejected and why
* Killing OS processes at random: that is Step 9's demo; in-process chaos is faster to iterate on.
* More than one broker down: a majority-less cluster is *supposed* to stop; that is tested separately.
* Asserting latency: a loaded laptop would make the test flaky for the wrong reason.

## What goes wrong if you get it wrong
A chaos test that quietly tolerates data loss ("some records missing, fine") is worse than none. The audit therefore treats a missing acknowledged record as an immediate failure with the full cluster state printed.

## Tradeoffs and honest limits
In-process crashes do not lose the OS page cache, so power-loss torn writes are not covered here (file-level fuzz tests and the Phase 4 simulator cover them). Timing assumptions (election 150-300 ms) are tight, so a heavily loaded machine can stretch recovery; waits therefore have generous timeouts.

## How it is tested
It is the test. Step 8 of the plan also requires running the default variant 5 times in a row and the slow variant at least 3 times with different seeds; results are in the final report.
