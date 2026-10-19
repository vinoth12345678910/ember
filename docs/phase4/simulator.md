# The Raft simulator: how it works and how to use it

Package `com.ember.raft.sim` (in `src/main`, so Phase 5 can reuse it for cluster-level chaos tests). It depends on nothing but `ember-raft`'s core.

## The pieces
| Class | Job |
|---|---|
| `SimCluster` | the world: virtual clock, event queue, fake network, nodes, crash/restart, trace |
| `SimConfig` | everything that defines a run: seed, node count, latency range, drop/duplicate probability, forced election timeouts, injected bug |
| `InvariantChecker` | the safety laws, evaluated after every event |
| `Trace` | readable log (ring buffer of the last lines + rolling hash of all lines + optional full recording) |
| `RandomScenario` | seed -> chaos phase -> healing phase -> liveness assertion |
| `Scenarios` | named scenarios for people (election, leader-crash, partition, figure8) |
| `SimMain` | the command line tool for watching and replaying |

## How time works
There is no real clock. `SimCluster.step()` picks the next thing that happens: the earliest queued message delivery (ties: lowest sequence number first) or the earliest node timer (`RaftNode.nextTimerMs()`, ties: lowest node id; queued events go before timers at the same instant). It sets the virtual clock to that time and runs exactly that one event. `runFor(ms)` and `runUntil(predicate, maxMs)` are loops around `step()`. After each event the checker looks at the one node the event touched.

## The network
A sent message gets a random latency (default 5-40 ms, so messages overtake each other), may be dropped (`dropProbability`) or duplicated (`duplicateProbability`, the copy gets its own latency). At *delivery* time it is lost if the link is blocked (partition/isolate) or the destination is dead. `setLinkLatency(from, to, ms)` fixes a link's latency for scripted scenarios.

## Crashes
* `crash(n)`: the node is discarded and `storage.crash()` throws away every unsynced write.
* `crashOnNextSend(n[, rebootAfterMs])`: the node loses power right after its next message leaves (the message is still delivered), optionally rebooting a few ms later. This is what exposes "replied before syncing" bugs.
* `setRebootAfterVoteProbability(p)`: chaos knob; a node that just granted a vote loses power with probability p and reboots 1-30 ms later.

## Run one seed (watch it)
```
mvn -q -pl ember-raft compile
java -cp ember-raft/target/classes com.ember.raft.sim.SimMain --seed=42 --nodes=5 --scenario=leader-crash
java -cp ember-raft/target/classes com.ember.raft.sim.SimMain --seed=7  --nodes=3 --scenario=election --proposals=3 --duration-ms=3000
java -cp ember-raft/target/classes com.ember.raft.sim.SimMain --scenario=figure8
java -cp ember-raft/target/classes com.ember.raft.sim.SimMain --scenario=figure8 --bug=COMMIT_OLD_TERM_BY_COUNTING
java -cp ember-raft/target/classes com.ember.raft.sim.SimMain --scenario=random --seed=164 --nodes=3 --bug=FORGET_VOTED_FOR_ON_RESTART --quiet
```
Flags: `--seed=N --nodes=3|5 --duration-ms=N --scenario=random|election|leader-crash|partition|figure8 --proposals=K --bug=NAME --quiet`. After the trace you get a summary: virtual time, trace hash, leader history (term -> node), role/term/log/commit per node, message statistics, and `invariants: OK` or the violation (SimMain then exits with status 1). (`mvn package` also builds `ember-raft-0.1.0-SNAPSHOT-all.jar`, so `java -jar` works as well.)

## Reading a trace
```
t=00183 n2 election timeout -> CANDIDATE term=1      <- n2's timer fired at virtual time 183 ms
t=00183 n2 CANDIDATE term=1                          <- role change reported to the listener
t=00183 n2 -> n1 RequestVote(term=1,lastIdx=0,lastTerm=0)
t=00202 n1 -> n2 RequestVoteResponse(term=1,granted=true)
t=00216 n2 LEADER term=1
t=00216 n2 -> n1 AppendEntries(term=1,prev=0/0,n=1,commit=0)   <- prev=index/term, n=entries carried
t=00224 n1 -> n2 AppendEntriesResponse(term=1,ok match=1)       <- or "fail hint=conflictIndex/conflictTerm"
t=00251 n2 commit idx=1                                         <- onCommitted on n2
LOST n2 -> n3 ... (link down)    DROP n2 -> n3    DUP n2 -> n3    n3 crash -> DEAD (unsynced writes lost)
PARTITION [1,2] [3,4,5]    ISOLATE n4    HEAL    n3 restart
```
A line is written when a node sends (not when it receives); a delivery shows up as the receiver's follow-up lines. `LOST` lines are messages that arrived at a blocked link.

## Replay a failing seed
1. A failing test prints the seed, node count, the violation and the last 60 trace lines, and writes the whole run to `ember-raft/target/raft-sim/seed-<seed>.trace`, whose second line is the exact replay command. (The harness re-runs the seed with full recording; determinism guarantees it fails identically.)
2. Replay: `SimMain --scenario=random --seed=<seed> --nodes=<3 or 5> [--bug=NAME]` (seed even -> 3 nodes, odd -> 5 nodes in the tests).
3. Only that seed in Maven: `mvn -q -pl ember-raft test -Dtest=RandomizedSafetyTest -Draft.seeds=1 -Draft.seedStart=<seed>`.
4. Debugger: set a breakpoint in `RandomScenario.run` and step; nothing is timing dependent.

## Run more seeds
```
mvn -q -pl ember-raft test                                         # default: 300 seeds
mvn -q -pl ember-raft test -Draft.seeds=2000 -Draft.seedStart=500  # a bigger range
mvn -pl ember-raft test -Praft-long                                # 5,000 seeds + the tests tagged "slow" (2,000 more)
```

## Determinism rules (why a seed is a test case)
One thread; no real time; `java.util.Random(seed)` only, with separate streams for each node, the network and the scenario; nodes iterated in id order; events ordered by `(time, sequence)`; enums never hashed by identity. Two runs of one seed give the same trace hash (`RandomizedSafetyTest.theSameSeedGivesTheSameRunAndTheSameTraceHash`).

## Limits
All nodes share one perfect clock (no clock drift); disk faults are only "unsynced writes are lost" (no torn or corrupt writes); no membership changes, snapshots or client sessions (non-goals of this phase).
