# Step 6 - Log replication, commit, apply, propose (and faster backtracking)

## What we are building
The second half of Raft: the leader replicates its log to followers with `AppendEntries`, followers check and repair their logs, the leader decides what is **committed**, everyone **applies** committed entries in order, and clients can `propose` commands. Then, as a separate sub-step, the **accelerated log backtracking** hint.

## The Raft rules involved (paper references)
* **R9** (Figure 2 AppendEntries receiver 1): reply false if `term < currentTerm`.
* **R10** (5.2): a valid AppendEntries from the current leader resets the election deadline and tells us who the leader is.
* **R11** (Figure 2 receiver 2, 5.3 Log Matching): reply false if the log has no entry at `prevLogIndex` whose term matches `prevLogTerm`.
* **R12** (Figure 2 receiver 3-4): if an existing entry conflicts with a new one (same index, different term) delete it and all that follow; append entries not already in the log. **Entries that already match are left alone.**
* **R13** (Figure 2 receiver 5): if `leaderCommit > commitIndex`, set `commitIndex = min(leaderCommit, index of last new entry)`.
* **R14** (Figure 2 Leaders): on success update `nextIndex`/`matchIndex`; on failure decrement `nextIndex` and retry (with the hint: jump).
* **R15** (5.3, 5.4.2, Figure 8): commit index N if a majority has `matchIndex >= N` **and `log[N].term == currentTerm`**.
* **R17** (5.3): a client command is appended to the leader's log and replicated.
* **R3** (Figure 2 All Servers): `commitIndex > lastApplied` -> increment `lastApplied`, apply `log[lastApplied]`.
* Accelerated backtracking: end of Section 5.3 (the paper says it is optional and that it is not clear it is needed in practice; we implement the simple version described in the Raft thesis / course notes).

## Why it is designed this way
**Why followers check `prevLogIndex/prevLogTerm`.** Each AppendEntries says: "my entry just before these has index P and term T; only accept these if you have exactly that." By induction this guarantees *Log Matching*: if two logs have an entry with the same index and term, all the entries before it are identical. It is like a chain of hashes in a blockchain, without the hashes: the (index, term) of the previous link is enough because one leader creates at most one entry per index per term.

**R12: never truncate on a match.** Networks duplicate and delay messages. An old AppendEntries carrying entries 3-5 can arrive after the follower already holds entries 3-9 (all correct). If the follower "made room" by truncating from index 3 it would delete committed entries 6-9. So we only delete when an entry at the same index has a *different term*, and append only what is missing. (`Bug.TRUNCATE_ON_EVERY_APPEND` does the wrong thing; Step 9 shows the simulator catches it.)

**R13 caps the commit index at the last entry *of this message*.** A follower may hold stale, even conflicting, entries past what the leader just sent. `leaderCommit` talks about the leader's log; the follower only knows it agrees with the leader up to `prevLogIndex + entries.size()`. Committing beyond that could mark an entry committed that is not the leader's.

**R15: why only current-term entries are committed by counting (Figure 8).** Suppose an old leader replicated entry X (term 2) on a majority and then crashed before committing it. A new leader (term 4) can find X on a majority and be tempted to call it committed. But a *different* server holding an entry of term 3 at that index may still win a later election (its last term, 3, beats 2) and overwrite X - X was "committed" and then disappeared. The fix: a leader only counts replicas for entries of **its own term**; once such an entry is on a majority, everything before it is committed *indirectly* (any future leader must contain it). That is why every new leader appends a no-op (Step 3) - it gives the leader a current-term entry to commit.

**`matchIndex` in responses.** The follower reports "I verified your log up to index M". The leader sets `matchIndex[peer] = max(old, M)` so delayed, duplicated and reordered responses can never move it backwards, and no request/response correlation is needed.

**Commit by sorting.** To find the highest index stored on a majority the leader sorts the `matchIndex` values (its own counts as `lastIndex`) and takes the one at position `n - majority`. No loops over candidate indexes.

**Apply loop (R3) with one callback per index.** `onCommitted` is called once per index in order with no gaps; the listener is called after `sync()` (outbox). After a restart a node re-applies from index 1 - Raft does not persist `lastApplied`; the state machine is rebuilt (snapshots would shorten this; out of scope).

**Accelerated backtracking.** Plain Raft retries one index at a time: if a follower is 1,000 entries behind or divergent, that is 1,000 round trips. The follower returns a hint: if its log is too short, `conflictIndex = lastIndex+1`; if the term at `prevLogIndex` differs, it returns that term and the first index where that term starts. The leader then jumps `nextIndex` back to just past its own last entry of that term, or to `conflictIndex` if it has none: 2-3 round trips instead of thousands.

## What goes wrong if you get it wrong
* Commit old-term entries by counting -> a committed entry can vanish (Figure 8) - `Bug.COMMIT_OLD_TERM_BY_COUNTING`.
* Truncate on every append -> acknowledged entries can disappear after a delayed message.
* `matchIndex = response.matchIndex` without `max`: a late response rolls the leader's view back.
* Apply before sync / apply twice / apply out of order: duplicate or reordered state machine updates.
* `commitIndex = leaderCommit` without the cap: marking uncommitted, divergent entries committed.

## Alternatives I rejected and why
* Retrying with `nextIndex--` only: correct but slow (kept as a switch inside tests, to measure the difference).
* Optimistic `nextIndex` advancement (pipelining AppendEntries without waiting for acks): faster but adds states; not needed here.
* Keeping per-request bookkeeping on the leader: unnecessary thanks to `matchIndex` in responses.

## Tradeoffs and honest limits
A leader re-sends entries from `nextIndex` for every `propose` until acked, so under a burst many messages contain overlapping entries (harmless but wasteful); `maxEntriesPerAppend` bounds each message. The apply loop runs inside `receive`; a real state machine that is slow would need to be applied on another thread (Phase 5 decision). `lastApplied` restarts at 0 after a crash.

## How it is tested
One node-level test per rule (R3, R9-R15, R17): replies and hints, matching entries are never truncated by a duplicate/delayed message, conflicting entries are replaced, `leaderCommit` capped by the last new entry, old-term entries not committed by counting but committed once a current-term entry sits above them on a majority, single-node commit, `propose` results, applied once and in order. A round-trip comparison between decrement-by-one and the hint on a long divergent log.
