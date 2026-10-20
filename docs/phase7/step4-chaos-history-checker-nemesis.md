# Step 4 - ember-chaos: history, checker, nemesis, runners

## What we are building
A new module, `ember-chaos` (a tool, not a library; `ember-client` does not depend on it), with four parts:

1. **History.** Every client operation is recorded as two events, *invoke* and *complete*, with an outcome: **OK** (acknowledged), **FAIL** (definitely not applied) or **INFO** (unknown: a timeout or a lost connection; it may or may not exist). Operations: PRODUCE (unique values `p<id>-<seq>`), READ, GROUP_POLL, COMMIT; plus NEMESIS events (fault start/stop) and REPLICA events (state of each replica at the end). The history is written as one JSON line per event, so a run that failed can be re-checked offline: `--check-only=FILE`.
2. **Checker.** A *pure function*: history in, list of violations out. Nine properties (P1-P9, below).
3. **Nemesis.** A seeded schedule of faults: crash and restart a broker, pause a broker (a freeze like `SIGSTOP`), isolate a broker, cut one link, slow fsync, drop or delay peer messages, and stop a consumer's heartbeats. At most one broker-level fault is active at a time, so a majority always exists; a final heal restores everything.
4. **Runners.** `ChaosMain --mode=inprocess|external`. In-process it starts real brokers in the JVM and attacks them through the fault hooks of step 3. External it talks to running brokers and applies faults through a `FaultDriver`: `ProcessFaultDriver` (PID files, `kill -9/-STOP/-CONT`) or `DockerFaultDriver` (`docker kill/start/pause/unpause`, `docker network disconnect/connect`). Output: a summary table, and exit code 0 only if the checker found nothing.

## Why it is designed this way
**An exam marker who does not trust the student.** The brokers' own tests say "I passed". The checker only looks at what *clients were told* and what they *read*, and compares. Jepsen-style: record a history under faults, then check it against a model.

The nine properties:

| | Property | What it catches |
|---|---|---|
| P1 | no lost acknowledged write | an OK produce missing from the final read at its acknowledged offset (same key and value) |
| P2 | no divergence | two reads (from anyone, any time) returning different data for the same offset; a delivery that disagrees with a read |
| P3 | no phantoms | a value nobody sent, or a value from a produce that definitely FAILED |
| P4 | offset uniqueness | two OK produces with the same (partition, offset); one producer's acknowledged offsets going backwards |
| P5 | per-key order | the acknowledged values of one producer and key appearing in a different order in the log (or in two partitions) |
| P6 | durable reads never shrink | a later read by the same reader reporting a shorter log than an earlier one saw |
| P7 | group delivery | an acknowledged record that never reached the group; a delivery nobody produced; the duplicate rate is counted and printed |
| P8 | commits | a commit beyond the log end; a commit with a stale generation that was ACKNOWLEDGED (a zombie) |
| P9 | replica agreement | data logs, committed Raft-log prefixes (term and bytes), `__groups` state and topic state identical on all replicas |

**Unknown is not failure.** An INFO produce (timeout) may exist in the log or not; both are fine. A FAIL produce must never appear. The checker is careful about this because an over-strict checker cries wolf and gets ignored.

**The checker is mutation-tested.** A checker that passes everything is worthless. So `CheckerMutationTest` takes a known-good history, injects exactly ONE bug per test (drop an acknowledged record, two values for one offset, a duplicate offset, an invented value, swapped keyed records, a read that shrinks, an accepted stale-generation commit, a replica that differs by one byte, an undelivered record, a commit beyond the end, a divergent Raft log or group state, a failed write that became visible) and requires the exact violation code; the untouched history must report nothing.

**One fault at a time.** Faults are applied to a real majority-preserving schedule. A separate scenario (`runMajorityLoss`) kills two of three brokers on purpose and requires that *nothing* is acknowledged during the outage and that progress resumes after healing.

## Alternatives I rejected and why
* Asserting inside the workload ("this write must be there"): it ties correctness checks to timing and cannot be re-run on a recorded failure. Separating recording from checking lets one history be examined by a better checker later.
* A relaxed checker for "known" anomalies: the rule is that a violation means a bug in Ember or in the checker (proved by a test), never an exception list.
* Killing OS processes for every test run: external mode exists for that (step 7); in-process is 10x faster to iterate.

## What goes wrong if you get it wrong (and did)
The first alarm of the checker was **my own recording**: `commitSync()` returns normally without contacting the coordinator when there is nothing to commit, and the workload recorded every normal return as an acknowledged commit, which produced a false "zombie commit". The fix was at the source: `commitSync()` now returns whether a commit was really sent. The second finding was a **real Phase 6 gap**: consumers that subscribe right after a topic is created could receive an empty assignment for ever, because the coordinator's local topic list lagged; the coordinator now re-derives assignments on its tick. A third, found by reading code while designing the workload: a **group consumer could skip records** when a later partition's fetch failed after an earlier one succeeded.

## Tradeoffs and honest limits
* It is a test, not a proof. It can only see violations that this history makes visible; the seed reproduces the fault *schedule*, not the exact timing; a bug that needs a rare interleaving may need many runs.
* P9's Raft-log comparison needs the data directories (in-process runs open them after a graceful stop; external runs compare data logs and state through the admin API only).
* Reads are checked only for what the *leader* served; follower reads do not exist.
* A produce retried inside the client library is one operation here; its outcome is INFO (not FAIL) if any retry happened, because an earlier attempt may have been applied.

## How it is tested
`CheckerPropertiesTest` (each property on small handwritten good and bad histories, 14 tests), `CheckerMutationTest` (16 tests; the whole mutation story), history-file round trips, and `ChaosRunTest`: three fixed seeds x 20 s with all eight fault types and zero violations required, the majority-loss scenario, the command line tool (exit codes 0 / 1 / 2, offline re-check, a corrupted history exits 1), plus three 120-second runs tagged `slow` (Maven profile `cluster-long`).
