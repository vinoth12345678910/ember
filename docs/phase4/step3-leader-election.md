# Step 3 - Leader election

## What we are building
Complete leader election inside `RaftNode`: becoming a candidate, asking for votes (`RequestVote`), granting votes, counting a majority, becoming leader (with a **no-op** entry), sending heartbeats, and stepping down when a higher term shows up. The `AppendEntries` receiver only does what election needs (term check, reset the timer, remember the leader, candidates step down); the log-consistency part comes in Step 6.

## The Raft rules involved (paper references)
* **R1** (Figure 2 "Rules for Servers: All Servers"): any message with term T > currentTerm -> set currentTerm = T, clear votedFor, become follower - *then* process the message.
* **R2** (Figure 2, RequestVote receiver 1 / AppendEntries receiver 1): reject requests with term < currentTerm and tell the sender our term; ignore stale responses.
* **R4** (5.2, Figure 2 "Candidates"): on election timeout: increment term, vote for self, reset the election timer (new random value), send RequestVote to all others.
* **R5** (5.2, 5.4.1, Figure 2 RequestVote receiver 2): grant the vote if we have not voted for someone else in this term **and** the candidate's log is at least as up to date as ours (higher last term wins; equal last terms: longer or equal log wins). The vote is made durable before we answer.
* **R6** (5.2): a candidate with votes from a majority (itself included) becomes leader; each node counts once per term.
* **R7** (5.2): a candidate that receives AppendEntries from a leader whose term is at least its own steps down.
* **R8** (5.3, Figure 2 "Leaders", 5.4.2/8): on election the leader initializes `nextIndex[] = lastIndex+1`, `matchIndex[] = 0`, appends a **no-op** of its term, sends AppendEntries to everyone at once and then repeatedly during idle periods (heartbeats).
* **R16**: a leader that sees a higher term steps down; a leader has no election timer.
* **R18**: after a restart a node is a follower with a fresh deadline (term, vote, log persist).

## Why it is designed this way
**Terms are Raft's logical clock.** Think of a presidential term number. A message from an older term is from a "past president" and is ignored; a message from a newer term means *I* am out of date, so I update my number and step down - always, immediately (R1). That one rule resolves almost every confusing situation (a deposed leader waking up from a long pause, a partitioned node rejoining) without special cases.

**One vote per term, remembered on disk.** If the vote were only in memory, a node could crash after voting for A, restart, and vote for B in the same term: A and B could both collect a majority and both lead term 7. So the vote is durable *before* the "yes" leaves the node (the outbox from Step 2).

**The election restriction (R5, 5.4.1).** A candidate cannot win unless its log is at least as up to date as a majority's. Since every committed entry is on a majority, and any two majorities overlap, the winner is guaranteed to contain every committed entry. This is the heart of "Leader Completeness".

**Reset the election deadline only when a vote is *granted*.** If every RequestVote reset the timer, a node with a stale log (or a partitioned one that keeps incrementing its term) could ask for votes over and over and prevent everyone else from ever timing out - a livelock. Only granting a vote, or hearing from a legitimate leader (R10), is proof that someone is making progress.

**The no-op at election (8).** A new leader cannot know which old-term entries are committed. Appending an entry of its *own* term and getting it on a majority commits everything before it (Step 6, Figure 8). Without it, a leader of an idle cluster could never commit the previous leader's last entries.

**Count each voter once.** Messages are duplicated by networks. A `TreeSet` of voter ids makes the count idempotent.

## What goes wrong if you get it wrong
* No R1 on responses: a deposed leader keeps believing it leads, and clients write to a ghost.
* Vote not persisted: two leaders in one term (ElectionSafety violated); that is `Bug.FORGET_VOTED_FOR_ON_RESTART`.
* Skip the log comparison: a candidate missing committed entries wins and overwrites them (`Bug.VOTE_WITHOUT_LOG_CHECK`).
* Equal timeouts without randomization: endless split votes.
* Heartbeat interval longer than the election timeout: followers constantly start elections.

## Alternatives I rejected and why
* PreVote / CheckQuorum (extra protections against disruptive nodes): explicitly out of scope; they add rules and the paper's core is enough for this phase.
* Resetting the timer on every received message: see above.
* A separate "leader election" class: the rules share the node's state (term, vote, role) so splitting them would only pass the same fields around.

## Tradeoffs and honest limits
A node returning from a partition with a huge term forces the healthy leader to step down (R1), causing a needless election. That is a known property of plain Raft (PreVote fixes it). A leader's heartbeats are plain AppendEntries, so they also carry entries when a follower is behind.

## How it is tested
One node-level test per rule (names start with the rule number), using the manual harness: vote once per term even when two candidates ask, the same candidate asking twice, up-to-date comparison in all four cases, persistence before response, deadline reset only on grant, duplicate votes counted once, stale responses ignored, candidate and leader step-down, heartbeat cadence exactly `heartbeatIntervalMs`, a leader never starts an election, single-node cluster elects itself, restart keeps vote and prevents a double vote.
