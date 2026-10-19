# Bug injection: do the checkers really see?

Each `Bug` is one small, clearly commented `if (config.injectedBug() == Bug.X)` branch in `RaftNode`. `BugDetectionTest` runs up to 3,000 random seeds per bug (seed `s` uses 3 nodes if `s` is even, 5 if odd) and records which checker fired. These numbers come from the real runs (`mvn -pl ember-raft test -Dtest=BugDetectionTest`); the simulator is deterministic, so they are reproducible exactly. "Seeds needed" = the position of the first failing seed (seed 0 = 1 seed).

| Injected bug | What is wrong | Law broken | First caught by | Seeds needed | Violations over 3,000 seeds |
|---|---|---|---|---|---|
| `VOTE_WITHOUT_LOG_CHECK` | grants votes without comparing logs (5.4.1) | Leader Completeness | LeaderCompleteness | **1** (seed 0) | LeaderCompleteness 2,815; StateMachineSafety 1 |
| `TRUNCATE_ON_EVERY_APPEND` | follower truncates even when entries match (R12) | Log Matching / commit safety | CommitIndexBounded (a follower dropped entries it knew were committed) | **4** (seed 3) | CommitIndexBounded 315; LeaderCompleteness 2; liveness failures 1,530 (the cluster cannot make progress) |
| `ACK_BEFORE_SYNC` | replies before `sync()` (outbox reversed) | Election Safety (lost votes), Leader Completeness (lost acked entries) | ElectionSafety | **165** (seed 164) | ElectionSafety 42; LeaderCompleteness 2; StateMachineSafety 1 |
| `FORGET_VOTED_FOR_ON_RESTART` | votedFor cleared on restart | Election Safety | ElectionSafety | **165** (seed 164) | ElectionSafety 46 |
| `COMMIT_OLD_TERM_BY_COUNTING` | commits an old-term entry once a majority stores it (Figure 8) | Leader Completeness | not found by random seeds in 3,000; **always caught by the scripted Figure 8 schedule** | 0 of 3,000 random; scripted: 1 run | LeaderCompleteness (scripted) |
| `NONE` (control) | nothing | - | nothing | - | **0 violations, 0 liveness failures** over the same 3,000 seeds (and over 10,300 further seeds) |

Notes:
* `TRUNCATE_ON_EVERY_APPEND` is found first by a *sanity* checker rather than one of the five properties; `LeaderCompleteness` (one of the five) also fires in 2 seeds, which is what the test requires ("at least one seed of a sensible kind").
* `FORGET_VOTED_FOR_ON_RESTART` and `ACK_BEFORE_SYNC` first needed the random generator to get nastier (below).
* `COMMIT_OLD_TERM_BY_COUNTING` needs a very specific interleaving (Figure 8). The test accepts the scripted schedule as the catch, as allowed.

## How the generator had to improve (the real story)
First version of the chaos generator (crash, restart, bipartition, heal, drop/duplicate, isolate, proposals, plus "crash right after the node's next message leaves"):

* `ACK_BEFORE_SYNC`: found after 261 seeds (needed the "crash right after sending" action, which exists only for this bug: with crashes only *between* events, replying before syncing is unobservable, because nothing can happen inside one call).
* `FORGET_VOTED_FOR_ON_RESTART`: **NOT found in 3,000 seeds.** Why: a forgotten vote only matters if a rival candidate of the *same term* asks the rebooted node before the node learns anything new, i.e. within the 5-40 ms a message spends on the wire; but the generator acts every 50-600 ms and a restart was always a separate, later action, so the window could not be hit.

Changes made (the assertion was not weakened; the generator got nastier):
1. a third kind of crash: **quick reboot** (crash now, restart 1-40 ms later) so messages already in flight reach the new incarnation;
2. "crash right after sending" may now also come with a quick reboot (1-40 ms);
3. a chaos level **"power loss right after granting a vote"** (0-40%, changed together with the drop/duplicate rates) with a 1-30 ms reboot, because the dangerous moment for a lost vote is exactly when the vote has just left the node.

After these changes `FORGET_VOTED_FOR_ON_RESTART` is found at seed 164 and `ACK_BEFORE_SYNC` at seed 164 as well (earlier: 261), and the correct code still produced zero violations over thousands of seeds, which is what shows the new faults are realistic (a correct Raft must survive them).
