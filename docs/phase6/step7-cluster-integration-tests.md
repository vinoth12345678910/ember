# Step 7 - Cluster integration tests for consumer groups

## What we are building
`GroupScenariosTest`: real brokers, real sockets, the real client, one scenario per promise of the phase, plus a small support class (`GroupMember`: a consumer running in its own thread that records every record it received).

## Why it is designed this way
The promise of a consumer group is easy to state and easy to break in the details: *every partition has exactly one owner, nothing is skipped, and offsets survive the death of consumers and brokers.* So each test ends in the same two audits: (1) **ownership**: in the final generation every partition is owned by exactly one member, and what each consumer believes it owns equals what the coordinator says; (2) **delivery**: every record that was produced was delivered at least once (duplicates are allowed after a crash or rebalance; skipping is never allowed).

Scenarios: three consumers on six partitions; a fourth joins; one leaves; a consumer crashes (heartbeat stops without leaving) and its uncommitted records are delivered again; a **zombie** consumer is fenced and cannot overwrite the new owner's offsets; the coordinator broker crashes while consumers run; the whole cluster restarts; a committed offset falls below the log start after retention; the same rebalance in single-broker mode; replicated group state is identical on all brokers.

Session timeouts are short (seconds) but never fixed sleeps: every wait polls a condition with a timeout and prints the state of the cluster if it fails.

## Alternatives I rejected and why
* Asserting exact generation numbers after concurrent joins: the number depends on arrival order. The tests assert the final *properties* (ownership, delivery) instead.
* Mocking the coordinator: the interesting bugs live between the client threads, the coordinator timer and Raft.

## What goes wrong if you get it wrong
A test that only checks "the consumer got N records" passes even if two consumers read the same partition. The ownership audit compares the coordinator's view with each consumer's own view, which catches a consumer that kept consuming a partition it no longer owns.

## Tradeoffs and honest limits
Real-time timings (seconds per scenario). In-process crashes do not lose the OS page cache. The coordinator-failover test asserts that member ids are unchanged and nothing is lost; it does not demand an unchanged generation, because a heavily loaded machine may legitimately let a session run out.

## How it is tested
These are the tests. Step 8 adds a randomized chaos run over the same pieces.
