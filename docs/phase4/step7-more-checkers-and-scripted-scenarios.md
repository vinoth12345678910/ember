# Step 7 - The remaining safety checkers, and scripted scenarios (including Figure 8)

## What we are building
The other four properties of Figure 3 plus sanity checks, all evaluated after every event:
* **LeaderAppendOnly** - a leader never overwrites or deletes entries in its own log.
* **LogMatching** - if two logs contain an entry with the same index and term, the logs are identical up to that index.
* **LeaderCompleteness** - every entry committed so far is present in the log of every node that becomes leader.
* **StateMachineSafety** - no two nodes ever apply different entries at the same index.
* Sanity: `commitIndex` never decreases within a node's life and is at most its `lastIndex`; `onCommitted` arrives in index order without gaps.

Then deterministic scripted scenarios with exact expected outcomes: replication, leader crash, minority partition, divergent follower log, duplicate/reorder storm, "a crash loses only unsynced writes", and **Figure 8**.

## The Raft rules involved
Figure 3 / Section 5.4: the five safety properties and their proofs. The properties are consequences of R5 (election restriction), R11-R12 (log matching and conflict handling), R15 (commit rule). If any of those rules is implemented wrongly, one of these checkers should fire.

## Why it is designed this way
**Checkers know nothing about the algorithm.** They observe only what any outsider could: roles announced by the listener, committed entries delivered to the state machine, and the nodes' logs. If the checker used `RaftNode`'s internals it could share the node's mistakes.

**Efficient LogMatching with an incremental prefix hash.** Comparing every pair of whole logs after every event would be quadratic. Instead the checker keeps for each node a *mirror* of its log plus, for every index, a hash of the whole prefix up to that index. After an event it finds the first index where the node's log differs from its mirror (logs only change by truncating the suffix and appending, so scan backwards from the end), updates the mirror, and compares just the changed entries against the other nodes: "same term at this index but a different prefix hash" is a violation. Cost per event is proportional to what changed. A log is a chain of (index, term, command); the prefix hash plays the role of a Merkle-chain tip.

**LeaderAppendOnly via the same mirror.** If a node that was already leader before this event has a changed entry at an index within its old length, it overwrote something. A node that *just* became leader is exempt for the event in which it became leader (its first change is the appended no-op).

**LeaderCompleteness at election time.** The global list of committed entries (first value seen per index) is compared with the new leader's log at the moment it announces leadership. This is the property that the election restriction (R5) exists to guarantee.

**StateMachineSafety is the property users care about.** "Two replicas applied different commands at index 5" is the data-corruption bug; every other property is a means of preventing it.

**Why scripted scenarios in addition to random ones.** Random runs find surprises; scripted runs *explain*. They fix exact timeouts, latencies and pre-populated logs so a reader can follow the story and assert exact outcomes. **Figure 8** is the most famous trap in Raft (Section 5.4.2): an entry replicated on a majority but of an old term can still be overwritten. Reproducing it on purpose, once with correct code (nothing bad happens) and once with `Bug.COMMIT_OLD_TERM_BY_COUNTING` (the checkers report a violation), shows both that the rule matters and that the checker can see its violation.

## What goes wrong if you get it wrong
* A LogMatching check that compares only the entry itself (not the prefix) misses logs that diverge earlier.
* A LeaderCompleteness check that only looks at entries committed by the *current* leader misses the whole point.
* A checker that stops at the first mismatch silently without the trace is almost useless: reports carry the seed and the last 60 lines.

## Alternatives I rejected and why
* Full-log comparison after every event: O(events x log length x nodes).
* Comparing only at the end of a run: faults "heal" themselves and escape detection.
* Using hashes of the entire log only: that would not tell *where* logs diverged.

## Tradeoffs and honest limits
The prefix hash is a 64-bit mix, so a collision could in theory hide a violation (probability ~2^-64 per comparison). The checker verifies Log Matching for the nodes' *current* logs; it does not recheck old histories of crashed nodes beyond their durable log. LeaderCompleteness is checked at election time only (that is its definition).

## How it is tested
Each checker is fed a hand-made violation and must throw (proof it can fail). The scripted scenarios run with all checkers active and assert exact outcomes. Figure 8: the correct implementation survives, `Bug.COMMIT_OLD_TERM_BY_COUNTING` produces an `InvariantViolation`.
