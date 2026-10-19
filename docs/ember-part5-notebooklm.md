# Ember Part 5 - Raft Consensus in Isolation (Phase 4)

## One-minute pitch

Ember so far has one broker. If its machine dies, the data and the service are gone. The classic cure is several brokers that keep identical copies of the log and agree on who is in charge, even when machines crash and networks lie. The agreement algorithm we chose is **Raft**.

Raft is famously easy to read and famously easy to get subtly wrong. So Phase 4 does not touch the broker at all. We built Raft as a **pure state machine**: no threads, no sockets, no clocks, no files. It reacts to events (a timer fired, a message arrived, a client proposed a command) and says what to send. Because it is pure, we could build a **simulator** around it: a virtual clock, a fake network that delays, drops, duplicates and reorders messages, nodes that crash and lose everything not yet synced to disk. The simulator replays thousands of random failure histories and, after every single event, checks the five safety laws of Raft from the paper.

Then we asked the uncomfortable question: *would those checkers actually notice a bug?* We broke Raft on purpose in five classic ways and checked that the simulator catches each one, and we showed that the correct code produces zero violations over thousands of seeds. 425 tests pass in the repository; the Raft module has 166 (plus one 2,000-seed "slow" test), and the simulator runs about 720 whole seeds and about 4.7 million events per second.

---

## Step 0 - Preflight

**Why.** Never build on a broken base. We read the README and the modules and ran the full build: green, 259 tests in four modules, and no Raft code anywhere yet.

## Step 1 - Types and the storage contract

**Why a pure core.** Think of a chess engine: you give it a board and a move, it gives you the new board. It never watches a clock or opens a socket. If Raft is built the same way, we can feed it any history we like, including the horrible ones, and replay a failure exactly.

**Why storage has `sync()`.** A disk write that returned is not necessarily on the disk; it may live in memory until an fsync. Our fake disk keeps two copies: **pending** (what the node sees) and **durable** (what survives a crash). `crash()` throws pending away. Analogy: you write a note on a whiteboard (pending), and only photocopying it into the filing cabinet (sync) makes it survive a power cut.

**Why term and vote are saved together.** If a crash could save one without the other, a node could end up in a new term with an old vote and vote twice.

**Why `LogEntry` compares by content.** Java arrays compare by identity, so two identical entries would look different and every log comparison in the checkers would be wrong.

## Step 2 - The node skeleton and the outbox

**Why the caller drives the node.** The node never sleeps. It says "wake me at time X" (`nextTimerMs`) and the caller (a test, the simulator, later a real event loop) does. Not thread-safe on purpose: locks would make behaviour depend on thread timing, and failures could no longer be replayed.

**The outbox rule: first write it down, then tell people.** Every entry point does three things in order: change state and queue messages; sync the disk; only then send messages. Analogy: a bank teller who closes the ledger page before handing you the receipt. If you hand out the receipt first and the lights go out, the deposit is gone but you hold proof.

**Randomized election timeouts.** If every node timed out at the same moment, everybody would vote for themselves and nobody would win, forever. Randomizing the waiting time (150 to 300 ms) makes one node usually go first.

## Step 3 - Leader election

**Terms are Raft's clock.** A term is like the number of a presidential term. A message from an older term is from a former president and is ignored; a message from a newer term tells me I am out of date, so I update my number and step down, immediately. That single rule untangles a huge number of confusing situations.

**One vote per term, on disk.** If the vote lived only in memory, a node could vote for A, crash, restart, and vote for B in the same term. Both could win. That is "two leaders in one term", the worst thing that can happen.

**The election restriction.** A candidate whose log is behind cannot win. Because everything that was committed is on a majority, and any two majorities overlap in at least one node, the winner is guaranteed to contain all committed entries.

**The no-op.** A brand-new leader cannot know which old entries were committed, so it appends an empty entry of its own term; once that is on a majority, everything before it is committed too.

## Step 4 - The simulator core

**Why simulate.** The bugs of distributed systems hide in rare orders of events: "the follower crashes just after answering, while a delayed message from the old leader arrives". On real machines this happens once a month and cannot be reproduced. In a simulator it becomes a search: run many random schedules and replay any failing one from its seed.

**Virtual time.** Our clock is just a number. The simulator jumps to the next thing that happens, so 55 seconds of cluster life take about a millisecond and a half of real time. Analogy: a film editor cutting out all the waiting.

**Determinism.** One thread, one seeded random generator per node and one for the network, events ordered by (time, sequence number). The same seed always gives the exact same history, so a failing seed is a test case you can keep forever.

**A crash can happen in the middle of a call.** To catch a node that answers before saving, the simulator can cut the power right after a node sends a message. With correct code the state was already saved, so nothing is lost.

## Step 5 - Election Safety, and election scenarios

**A checker is a small independent witness.** Election Safety: at most one leader per term. The checker keeps a notebook "term -> leader" and never erases it, not even when a leader crashes. If a second leader appears for the same term, the run stops with the seed and the last 60 lines of the trace.

**Scenarios with exact outcomes.** Forced timeouts let us say "node 1 wins term 1" and prove it. We even engineered a **split vote** on purpose with four nodes and hand-picked network delays: two candidates, each with exactly half the votes, nobody wins, and the next round resolves it.

## Step 6 - Log replication

**The consistency check.** Every AppendEntries message says: "the entry just before these has index P and term T. Accept only if you have exactly that." That chain of checks is why two logs that agree at one entry agree on everything before it. Analogy: a chain where each link records the number and colour of the previous link.

**Never truncate on a match.** Networks duplicate and delay messages. A late message about entries 3 to 5 must not delete entries 6 to 9 that the follower already has. We only delete when the same position holds a different term.

**Commit only current-term entries by counting (Figure 8).** Counting replicas is only safe for entries from the leader's own term. An old-term entry on a majority can still be overwritten by a later leader. The fix: commit a current-term entry and let everything before it become committed indirectly.

**Faster backtracking.** When a follower is far behind or has a different tail, the plain algorithm steps back one entry per round trip. The follower can instead say where its conflicting term begins. In a test with a 250-entry leader log and a 150-entry divergent follower tail, plain stepping needs about 250 round trips; the hint needs 3.

## Step 7 - The other checkers, and Figure 8

**Four more laws.** Leader Append-Only (a leader never rewrites its own log), Log Matching, Leader Completeness (a new leader holds every committed entry) and State Machine Safety (nobody applies different commands at the same index). To check Log Matching cheaply the checker keeps, for every log position, a hash of everything before it, so it only compares what changed.

**Figure 8, on purpose.** We rebuilt the famous trap from the paper with five pre-set logs, fixed timeouts and fixed network delays: an entry from an old term reaches a majority, the leader dies, and another node with a newer-term entry wins and overwrites it. With the correct rule, nothing was ever called committed, so nothing is lost. With the "commit by counting" mistake switched on, the checker reports that a committed entry vanished.

## Step 8 - Random chaos and liveness

**Two promises.** Safety means nothing bad ever happens, even when four of five nodes are dead. Liveness means something good eventually happens, which can only be promised when things are healthy for a while. So every run has a hostile phase (about 30 virtual seconds of random proposals, crashes, restarts, splits, heals, packet loss, duplicates, isolations) where only safety is checked, and then a healing phase where we also demand: exactly one leader, identical logs, identical applied commands, and our five final commands each applied exactly once.

## Step 9 - Do the checkers really see? Bug injection

**A test suite that has never failed proves nothing.** So we broke Raft on purpose, five ways, each one line of code, and counted how many random seeds it took to notice.

**The honest part.** One bug, forgetting the vote after a restart, was NOT found in 3,000 seeds. The reason was in our generator: a forgotten vote only matters if a rival candidate asks within the few milliseconds a message spends on the wire, but our chaos acted every 50 to 600 ms and restarts were always later separate actions. We made the generator nastier (quick reboots; power loss right after a node grants a vote) rather than weakening any check. Then it was found.

## Step 10 - The long run and the documents

5,000 seeds with the long profile plus a 2,000-seed slow test: 7,000 seeds, zero violations, zero liveness failures. The documents: a table from every rule of Figure 2 to the method that implements it and the test that proves it; a simulator guide; the bug-injection results; and a plan for how a real runtime will wrap this core.

## Step 11 - Final verification

`mvn clean verify` was green. `SimMain --seed=42 --nodes=5 --scenario=leader-crash` showed a real life story: node 2 won term 1 and replicated three entries, then was killed; node 1 and node 3 both campaigned in term 2 and split the votes (node 4 voted for node 1, node 5 for node 3), and node 5 won term 3. The figure8 scenario with the commit-by-counting bug printed "INVARIANT VIOLATED: LeaderCompleteness" with the seed and the trace.

---

## The numbers

**Tests.** ember-raft: 166 tests by default (106 node-level: storage 11, entries 4, config 5, skeleton 8, election rules 36, replication rules 35, backtracking 6, smoke 1; 51 simulator: cluster 14, election-safety checker 6, other checkers 15, election scenarios 7, scripted scenarios incl. Figure 8 9; 3 randomized-safety tests; 6 bug-detection tests). One more test tagged slow runs only with the long profile. Whole repository: 55 storage + 64 protocol + 16 client + 124 broker + 166 raft = **425**.

**Seeds.** Every normal build: 300 random seeds. Long profile: 5,000 + 2,000 seeds. Extra ranges run by hand: 2,000 and 3,000 seeds. Bug hunts: 3,000 seeds per bug per run. Violations with the correct code: **0**; liveness failures: **0**.

**Speed.** Long run: about 4.7 million simulated events per second and about 720 seeds per second (a seed is about 55 virtual seconds, roughly 6,500 events). The default 300 seeds take about a second; the slowest test is the bug-detection hunt at about 21 seconds.

**Bug-detection table (seeds needed to catch each bug).**

| Injected bug | First caught by | Seeds needed |
|---|---|---|
| vote without log check | LeaderCompleteness | 1 |
| truncate on every append | CommitIndexBounded (LeaderCompleteness in 2 seeds) | 4 |
| acknowledge before sync | ElectionSafety (also LeaderCompleteness, StateMachineSafety) | 165 |
| forget vote on restart | ElectionSafety | 165 (not found at all before the generator was improved) |
| commit old term by counting | scripted Figure 8 (not found by 3,000 random seeds) | always, in the scripted schedule |

---

## Bugs and surprises we hit

1. **The simulator found no bug in the Raft core itself.** Honest result: all rule tests and thousands of random seeds were green, with the correct code, from the first full run. The bugs we hit were in our tests and in our generator.
2. **A checker caught a bug in our own test data.** Our "divergent log" scenario pre-set a node with two entries of term 2 and then elected another node leader of term 2 as well. The Log Matching checker stopped it at once: two different entries with the same index and term. Only one leader exists per term, so the setup was impossible. We fixed the scenario (the new leader runs in term 3), not the checker.
3. **We misread our own rule R13.** A test sent a heartbeat with leaderCommit 2 and expected the follower to commit 2, but the rule caps the commit index at the last entry carried by the message (prevLogIndex plus entries), here 0. The code was right and the test was wrong; the lesson is exactly why R13 exists.
4. **Replying before syncing is invisible if crashes only happen between events.** We spotted this while designing the simulator: inside one call nothing can happen, so "reply first, sync afterwards" cannot be observed. That is why the simulator can cut the power in the middle of a call.
5. **A bug survived 3,000 seeds.** Forgetting the vote on restart was not found until the generator learned quick reboots and "power loss right after granting a vote". Before that it was undetectable in principle: the restart always came after the vote messages had already been dropped.
6. **A sanity check beat the big laws to a bug.** Truncating on every append is first caught by "commit index must not exceed the log length", because a follower throws away entries it already knows are committed. The five big laws also fire, in fewer seeds. We changed the test to demand at least one seed of a listed kind, not that the first catch is listed.
7. **Message order in the trace.** The first traces showed the messages of an election before the line "n2 became CANDIDATE". We changed the node to deliver listener callbacks before messages; a cosmetic change that makes the trace readable.
8. **Suspiciously good results made us look harder.** 2,000 clean seeds on the first try felt too good, so we looked at a trace (term 60 reached, seven different leaders in 40 seconds) to confirm the chaos was real, and then proved the checkers with the bug-injection step.

---

## Glossary

- **Raft**: a consensus algorithm: several servers agree on one ordered log of commands.
- **Term**: Raft's logical clock; each election starts a new term.
- **Leader / follower / candidate**: the three roles of a server.
- **Log entry**: one command with the term in which a leader created it.
- **Commit**: an entry is committed when it is safe: it will never be lost. Only then may it be applied.
- **Majority (quorum)**: more than half of the servers; any two majorities share a server.
- **Election restriction**: a server only votes for a candidate whose log is at least as up to date.
- **No-op entry**: an empty entry a new leader appends so it can commit something from its own term.
- **Outbox pattern**: save to disk first, send messages afterwards.
- **Sync / fsync**: force written data onto the disk; unsynced data is lost in a crash.
- **Simulator**: a program that runs the whole cluster with a fake clock and fake network.
- **Seed**: the number from which all randomness of a run is derived; the same seed repeats the same run.
- **Invariant / safety property**: something that must always be true.
- **Liveness**: something good must eventually happen (a leader exists, commands commit).
- **Split vote**: two candidates each get too few votes; nobody wins that term.
- **Figure 8**: the paper's example showing why old-term entries are not committed by counting.
- **Mutation testing / bug injection**: breaking the code on purpose to see if the tests notice.

## Suggested NotebookLM prompts

**Video overview:** "Make a video overview of Ember Phase 4 for a student who has built a single-node broker and has never implemented consensus. Follow the steps in order and give the WHY before the WHAT with the analogies in the document (the whiteboard and filing cabinet for sync, the bank teller closing the ledger before the receipt, terms as presidential terms, the film editor cutting out the waiting). Spend extra time on (1) the outbox rule, (2) why only current-term entries are committed by counting and the Figure 8 scenario, (3) how the simulator makes bugs reproducible, and (4) the bug-injection table, especially the bug that was not found until the generator became nastier. End with the real surprises we hit."

**Quiz:** "Write a 12-question quiz on Ember Phase 4: 3 on elections (terms, one vote per term, the election restriction), 3 on replication (the prevLogIndex check, never truncating on a match, the commit rule and Figure 8), 3 on the simulator (virtual time, determinism and seeds, why a crash must be able to happen mid-call), and 3 on testing the tests (what each safety checker watches, what bug injection proves, why a bug survived 3,000 seeds). Include answers with a one-sentence explanation each."
