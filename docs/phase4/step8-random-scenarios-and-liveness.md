# Step 8 - Random chaos, liveness, and thousands of seeds

## What we are building
`RandomScenario`: from a single seed it builds a cluster and attacks it for ~30 virtual seconds with random proposals, crashes (any number, even a majority), restarts, partitions, heals, changing drop/duplicate rates and isolations. Then a **healing phase** (everything repaired) and a **liveness** assertion. `RandomizedSafetyTest` runs it for 300 seeds by default (configurable), with every safety checker active after every event. Failing seeds write a full trace file and can be replayed.

## The Raft rules involved
* Safety (Figure 3) must hold **always**, whatever the failures: the checkers run after each event, during the chaos too.
* Liveness (5.2, 5.6): Raft makes progress when a majority of servers is up and can talk to each other and timing assumptions hold: `broadcastTime << electionTimeout << MTBF`. After the healing phase the timing assumptions are true again, so the cluster must have exactly one leader, identical logs and identical applied sequences.

## Why it is designed this way
**Safety and liveness are different promises.** Safety: "nothing bad ever happens" (two leaders, divergent committed entries). It must hold even when 4 of 5 nodes are dead. Liveness: "something good eventually happens" (a leader is elected, commands get committed). It can only be promised when the network and nodes are healthy long enough. That is why the scenario has two phases: a hostile chaos phase where only safety is checked, and a calm phase where liveness is checked as well. If liveness failed in the calm phase it would mean a *stuck* state: a livelock (endless elections), a node that never catches up, a leader that never commits - bugs that safety checkers cannot see.

**Seeds make randomness a test case.** Everything random in a run (node timeouts, network latency/drops, the scenario's choice of action) comes from streams derived from one `long`. Failing at seed 4711 is as good as a handwritten test: you can run exactly that schedule again, with the full trace, under a debugger.

**Weighted random actions** (propose 40%, crash 10%, restart 15%, bipartition 8%, heal 10%, change drop/duplicate rates 7%, isolate 5%, nothing 5%) at random 50-600 ms intervals. Half of the crashes are "power loss right after the node's next message leaves" - the only way to expose a node that acknowledged something before making it durable.

**Healing phase.** Heal the network, restart every node, drop = duplicate = 0, run 15 s, propose 5 unique commands to whichever node is leader (retrying for up to 5 s each), run 5 s, then assert: exactly one leader, all nodes alive, identical logs, equal commit indexes covering the 5 commands, each of the 5 commands exactly once in every node's applied sequence, identical applied sequences.

**Failing seeds are re-run to produce a trace.** Keeping every trace line of every run would cost memory; since runs are deterministic, the test harness simply runs the failing seed a second time with full recording and writes `target/raft-sim/seed-<seed>.trace`.

## What goes wrong if you get it wrong
* A generator that never crashes a majority, never partitions, never reorders: the interesting bugs (split brain, lost committed entries) are never reached. This is checked in Step 9: each injected bug must be found within 3,000 seeds, otherwise the generator is too gentle.
* Asserting liveness during chaos: spurious failures that teach nothing.
* Shared random streams: adding one trace line or one extra node would shift every later random decision and make old seeds meaningless. Streams are separate per node, network, and scenario.

## Alternatives I rejected and why
* Formal model checking (TLA+): more rigorous but a different skill and tool; randomized simulation tests the real Java code.
* Jepsen-style tests on real processes: realistic but slow and not repeatable.
* Fuzzing individual messages: the interesting bugs involve interleavings of many events, not malformed single messages.

## Tradeoffs and honest limits
Random testing can show bugs, never prove their absence; more seeds make rare interleavings more likely to be tried. The scenario cannot model clock drift or disk corruption. Liveness is asserted only after a generous calm period (20 s of virtual time).

## How it is tested
`RandomizedSafetyTest`: 300 seeds by default alternating 3 and 5 nodes (`-Draft.seeds=N`, `-Draft.seedStart=K`; profile `raft-long` runs 5,000). Same seed twice gives the identical trace hash. With `Bug.NONE` there must be zero violations and zero liveness failures.
