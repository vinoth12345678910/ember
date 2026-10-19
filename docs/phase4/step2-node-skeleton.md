# Step 2 - The RaftNode skeleton: timers, the outbox, and restart

## What we are building
The empty shell of `RaftNode`: it starts as a follower, owns an **election deadline**, reports the next time it needs attention (`nextTimerMs()`), handles the start of an election (`tick()` after the deadline), queues messages and callbacks, and releases them only after `storage.sync()`. Plus a manual test harness (recording transport, recording storage, a clock you move by hand).

## The Raft rules involved
* 5.2: "If a follower receives no communication over a period of time called the election timeout, it begins an election." Timeouts are **randomized** (150-300 ms here) so servers rarely time out together.
* Figure 2, Rules for Servers / Followers: convert to candidate when the election timeout elapses without hearing from a leader or granting a vote (R4; full election is Step 3).
* Figure 2, persistent state: the vote and term must be on stable storage **before responding to RPCs** - this is the *outbox* rule below.
* R18: a restarted node keeps `currentTerm`, `votedFor`, `log`; it forgets role, `commitIndex`, `lastApplied`, leader state and timers.

## Why it is designed this way
**A node is a single-threaded state machine that the *caller* drives.** The caller (a test, the simulator, later a real network loop) calls `tick(now)`, `receive(from, msg, now)`, `propose(cmd, now)`. The node never sleeps and never looks at a clock; it tells the caller when it next needs a tick with `nextTimerMs()`. The caller must serialize calls - the node is **deliberately not thread-safe**: locking would make behaviour depend on thread timing, and then a failing schedule could not be replayed. Phase 5 will give each Raft group one thread (an event loop), like Node.js.

**The outbox pattern (the most important idea of this step).** Every public entry point does three things in this order:
1. handle the event: change state in memory and *queue* the messages and callbacks it causes;
2. `storage.sync()` if anything persistent changed;
3. only then send the queued messages and deliver the callbacks.

Analogy: a bank teller who writes your deposit in the ledger *and closes the ledger page* before handing you the receipt. If the lights go out right after the receipt, the deposit is still there. A node that sends "yes, I vote for you" *before* making the vote durable can crash, forget the vote, and vote for someone else in the same term: two leaders. A follower that says "I stored your entry" before storing it durably can crash and the leader will have counted a replica that no longer exists. `Bug.ACK_BEFORE_SYNC` does steps 2 and 3 in the wrong order so Step 9 can prove our simulator notices.

**`nextTimerMs()` makes the simulator event-driven.** Instead of ticking every millisecond, the simulator asks every node "when do you next need me?" and jumps the virtual clock there. That is why it can simulate 45 seconds of cluster life in milliseconds of real time.

**Randomized timeouts use a `java.util.Random` handed in by the caller.** Same seed, same timeouts, same run.

**Reset deadline only when something *legitimate* happened** (R5, R10 in later steps): resetting it on every message would let a deposed or partitioned node keep followers from ever electing a new leader.

## What goes wrong if you get it wrong
* Send before sync: the crash window described above; the bug survives every normal test and only a crash-injecting simulator finds it.
* Same timeout on all nodes (no randomization): everybody becomes a candidate at the same instant, everybody votes for themselves, nobody wins, repeat forever (a *split vote* livelock).
* A timeout drawn outside [min, max]: either spurious elections (too short) or slow recovery (too long).

## Alternatives I rejected and why
* A real timer thread inside the node: not replayable.
* Callbacks fired immediately when state changes: the listener could observe (and the outside world react to) state that is not yet durable.
* Passing a `Clock` object: a plain `long nowMs` parameter is simpler and cannot hide a real clock.

## Tradeoffs and honest limits
The outbox delays every message until after the fsync of the *whole* event. A real runtime will batch many events per fsync (group commit) to amortize its cost (Phase 5 notes). Delivering callbacks after the messages means a listener cannot influence what is sent in the same call; that is intended.

## How it is tested
Node-level tests with a manual harness: election timeouts always inside [min, max] over 1,000 draws; `nextTimerMs()` equals the deadline; a message is never sent before `storage.sync()` ran (a recording storage and transport check this on every send); a restarted node keeps term, vote and log but resets role and commit state.
