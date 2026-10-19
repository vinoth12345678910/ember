# Step 10 - The long run, the numbers, and the documentation

## What we are building
One big run with the `raft-long` Maven profile (5,000 seeds plus a 2,000-seed "slow" test), a measurement of how fast the simulator is, and the documents that make the work reusable: the rules-to-code map, the simulator guide, the bug-injection results, and the Phase 5 plan.

## The Raft rules involved
All of them: the long run is the widest net we cast over Figure 2 and Figure 3. It checks that none of the five safety properties breaks and that the cluster recovers (liveness) in thousands of different failure histories.

## Why it is designed this way
**More seeds = more of the rare corners.** A bug that needs "crash a voter within 30 ms of its vote while two candidates are racing" shows up in 1 seed out of ~65 with our generator; a bug that needs three coincidences might show up once in 10,000. Cheap seeds (about a millisecond and a half each) make it affordable to run many, but not on every `mvn test`: the default run uses 300 seeds (a few seconds) and the profile `raft-long` is for before a release or after touching the core.

**Measure speed, because speed buys confidence.** If a seed cost one second, 5,000 seeds would be unaffordable. We report events per second and seeds per second; an "event" is one processed step of the simulator (a message delivery, a timer tick, a crash or a restart).

**Documentation is part of the deliverable.** Raft is easy to get *almost* right. The rules-to-code table lets a reviewer jump from a sentence of Figure 2 to the exact method and the exact test; the bug-injection document records which law each wrong variant breaks and how many seeds it took, so a future change that weakens the checkers is noticed (the numbers should not get worse without a reason); the Phase 5 plan says how this single-threaded core will be wrapped in a real runtime without breaking the properties tested here.

## What goes wrong if you get it wrong
* Reporting "0 violations" without saying how many seeds ran and that the checkers were proven able to fail: a meaningless claim.
* Letting the long profile rot: if nobody ever runs it, it will stop passing unnoticed.

## Alternatives I rejected and why
* Making 5,000 seeds the default: it would slow every build for little extra signal day to day.
* Running seeds in parallel threads: the simulator is single-threaded by design, but whole seeds are independent, so parallel seed runners are a possible later optimization; it was not needed (about 700 seeds per second on one thread).

## Tradeoffs and honest limits
Events per second depends on how much tracing is done (each event builds trace text). Faster numbers are possible by recording less, but the trace is what makes failures understandable.

## How it is tested
The long run itself: `mvn -pl ember-raft test -Praft-long`. Results are recorded in the final report and in `bug-injection.md` / `simulator.md`.
