# Step 1 - Raft core extensions: maxAppendBytes and CheckQuorum

## What we are building
Two small, safe additions to the pure Raft core (`ember-raft`), both needed by a real broker:
1. `RaftConfig.maxAppendBytes` (default 1 MiB): an `AppendEntries` carries entries until the next one would push it past the byte cap or past `maxEntriesPerAppend`; it always carries at least one entry if any is pending.
2. `RaftConfig.checkQuorum` (default **false** in the core, the broker turns it on): a leader that has not heard from a majority (itself included) within `electionTimeoutMaxMs` steps down to follower **without changing its term** and forgets that it is leader.

## Why it is designed this way
**Byte cap.** Phase 4 limited a message by *number* of entries. In a broker an entry is a produce batch that can be megabytes; 64 of them would be a 500 MB network frame that blows every frame limit and stalls the connection. Counting bytes keeps every Raft message under a size the network layer accepts. "Always at least one entry" matters: otherwise a single entry bigger than the cap could never be replicated and the follower would be stuck forever (the same rule as Phase 2's FETCH: the first record is always returned).

**CheckQuorum.** Plain Raft has a nasty property that Phase 4's tests demonstrated: a leader cut off from the others keeps believing it is the leader (nobody tells it otherwise) and accepts client commands that can never commit. Clients talking to it only learn the truth when their requests time out. CheckQuorum makes the leader notice by itself: it records when each peer last answered; if fewer than a majority (counting itself) answered recently, it steps down. Think of a manager who stops signing orders when the phone line to the head office has been dead for a while. The term is **not** changed because stepping down is a local observation, not a new election; the node keeps its vote record and simply becomes a follower again. A fresh leader gets a grace period (its "last heard" times start at election time), otherwise it would resign immediately.

**Default off in the core, on in the broker.** The 5,000-seed Phase 4 expectations were built without it; the simulator now runs it as a coin flip per seed (half of the seeds have it on), and a dedicated 1,000-seed run forces it on.

## Alternatives I rejected and why
* Stepping down *and* bumping the term: needlessly disruptive; other nodes would see a spurious higher term.
* Leader leases based on clocks: need synchronized clocks; CheckQuorum needs only a local timer.
* Splitting a big entry across several messages: complex; real brokers instead cap the produce request size.

## What goes wrong if you get it wrong
* Without the byte cap: an oversized AppendEntries is rejected by the frame decoder, the follower never catches up, and the leader retries forever.
* CheckQuorum counting only followers (forgetting itself): a 3-node leader with one live follower would step down although it has a majority.
* CheckQuorum checked against the election *minimum*: a leader would resign between two heartbeats under normal jitter.

## Tradeoffs and honest limits
A leader may resign because of a short network hiccup longer than `electionTimeoutMaxMs`; that costs one election. The byte cap counts command bytes plus a fixed per-entry overhead, not the exact wire size.

## How it is tested
Node level: byte cap respected, at least one oversized entry still sent, the cap and the entry-count cap interact; leader keeps leading while answers arrive, steps down after silence, keeps its term, clears its leader id, a lone single-node leader never steps down, off by default. Simulator: an isolated leader steps down (and does not with the option off); all five safety checkers stay silent over 1,000 random seeds with CheckQuorum forced on; liveness after healing still holds; the random scenario flips a coin per seed; all Phase 4 tests and mutation tests stay green.
