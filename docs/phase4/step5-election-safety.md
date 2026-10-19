# Step 5 - Checking Election Safety, and election scenarios

## What we are building
The first two checkers of the `InvariantChecker` (it runs after **every** simulated event):
* **ElectionSafety**: at most one leader per term, across the whole run - including leaders that crashed since.
* **Term monotonicity**: a node's `currentTerm` never decreases, not even across crash and restart.

And scripted election scenarios: a clean 3-node election followed by a calm period without spurious elections, leader crash and re-election, an engineered split vote, and a partitioned leader.

## The Raft rules involved
* **Election Safety** (Figure 3, Section 5.2): "at most one leader can be elected in a given term." It follows from R5 (one vote per term, persisted) and R6 (majority) - two majorities of the same cluster always share a node, and that node voted only once.
* Terms only grow (5.1): a server that sees a larger term adopts it (R1) and never goes back.

## Why it is designed this way
**A checker is a tiny, independent re-statement of the law.** It must not reuse the code under test. ElectionSafety here is a `Map<term, leaderId>` filled from the listener callbacks: when a node announces "I am LEADER in term T" and the map already holds a *different* node for T, the run fails with the seed and the last 60 trace lines. The map is never cleared when a leader crashes - a crashed leader of term 4 still "owns" term 4, otherwise a bug that lets a second node become leader of term 4 after the first crashed would slip through.

**Why term monotonicity is a *safety* check and not a nicety.** If a node's term could go backwards (because the new term was not persisted), then after a restart it could grant a vote in a term in which it already voted: that is how two leaders appear. The checker compares the node's term after every event with the highest term it ever showed. It looks at the durable storage view, so a restart that loses an unsynced term bump is caught the moment the node reappears.

**Per-event checking.** If we only checked at the end of a run, a violation could be "repaired" by later events (an overwritten log looks fine afterwards). Checking after every event pins the violation to the exact event and the trace line that caused it.

**Scenarios with forced timeouts.** To assert exact outcomes (term 1, node 2 is leader) the scenarios set `min == max` election timeouts per node (`SimConfig.withElectionTimeout`). The split vote needs two candidates at the same instant *and* voters that split; we engineer that with per-link latencies (deterministic) rather than hoping for a lucky seed.

## What goes wrong if you get it wrong
* ElectionSafety based on the *current* roles only: a leader that already crashed is invisible.
* Terms compared against the node's in-memory field while storage says otherwise: a lost term bump is invisible.
* Checking only the final state of a run.

## Alternatives I rejected and why
* Checking invariants from inside `RaftNode` (assertions): then a bug in the node could also break its own assertion; independent observers are stronger.
* Sampling every 100 ms: a violation that lasts one event is missed.

## Tradeoffs and honest limits
The checker sees leaders through the listener callbacks, which are only delivered after `sync()` (the outbox). A node that becomes leader and crashes before its callback is never recorded, but it also never told anyone it was leader (its first AppendEntries are sent after the callback), so it cannot have influenced anything.

## How it is tested
Unit tests feed the checker contradictory events by hand (two leaders in one term must throw; a lowered term must throw). Scenarios: stable 3-node election (term 1, one leader, 10 s of calm without a new election), 200 election-only seeds (exactly one leader, no term churn), leader crash and re-election in a higher term, the engineered split vote that resolves in a later term, and a partitioned leader whose majority side elects a successor.
