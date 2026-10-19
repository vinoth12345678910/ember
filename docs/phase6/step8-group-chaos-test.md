# Step 8 - The consumer-group chaos test

## What we are building
`GroupChaosTest`: three real brokers, a topic with six partitions, two producers writing globally unique values (with retries), three group consumers that record every record they receive, and a chaos thread that every 0.5-1.5 s does one of: crash and later restart a broker, kill a consumer (no leave) and start a replacement, pause a consumer's heartbeats for longer than its session, or cut a link between two brokers. At most one broker is unavailable at any time. A 10-second variant runs in the default build; a 60-second variant is tagged `slow` (Maven profile `cluster-long`). The schedule is seeded and the seed is printed on failure.

## Why it is designed this way
Hand-written scenarios test the failures we thought of. Groups add many interleavings: a rebalance during a commit, a coordinator failover during a join, a zombie that wakes up in the middle of someone else's generation. Random schedules find the rest.

At the end everything is healed, the group is allowed to settle, the consumers drain until the lag is zero, and then we audit:

1. **No acknowledged record is skipped.** Every value a producer was told was stored was delivered at least once to some member of the group.
2. **Commits stay inside the log.** Sampled while the chaos runs: the committed offset of a partition never exceeds that partition's end offset (the end offset is read *after* the committed offset, since it only grows).
3. **No unexplained duplicates.** A member never receives the same record twice without having rejoined in between. Duplicates across members or across rejoins are the expected price of at-least-once; their rate is counted and printed.
4. **Ownership.** In the final generation every partition has exactly one owner, and each consumer's own view equals the coordinator's.
5. **Replicas agree.** The `__groups` state is identical on all brokers.

## Alternatives I rejected and why
* Asserting "no duplicates after a commit": a slow member can legitimately deliver records it fetched before a rebalance. Only the rejoin-free duplicate is a bug.
* Asserting that committed offsets never go backwards: a deposed coordinator that has not noticed yet may serve an older state (documented); asserting it would make the test fail for a reason that is not a bug.

## What goes wrong if you get it wrong
A chaos test that tolerates a missing record is worse than none; check 1 fails immediately and prints the cluster and group state with the seed.

## Tradeoffs and honest limits
Real time, so a rerun with the same seed repeats the fault schedule but not the exact interleavings. In-process crashes keep the OS page cache.

## How it is tested
It is the test. Required runs: the default variant 5 times in a row and the slow variant at least 3 times with different seeds.
