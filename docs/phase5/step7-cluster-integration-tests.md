# Step 7 - Cluster integration tests

## What we are building
The proof that the design works with real brokers, real sockets and real client code: a `TestCluster` fixture (N brokers in one JVM; stop / crash / restart one while keeping its data directory and port; isolate / heal a broker; read any replica's data log directly) and the end-to-end scenarios: topic creation through any broker, keyed ordering, leader failover under continuous production, crashed broker catching up, minority isolation, majority loss, whole-cluster restart, controller crash during CREATE_TOPIC, a 5-broker availability smoke test, and a durability test with fsync **on**.

## Why it is designed this way
**Test the promise, not the code path.** The promise is: *an acknowledged record is never lost or reordered, whatever single failure happens.* So every failure test ends with the same audit: collect what the producer was told ("acknowledged"), read every replica's data log, and check (1) every acknowledged record is present, (2) in the order of acknowledgement, (3) duplicates exist only if the client retried (at-least-once), (4) after healing all replicas are identical.

**Real in-process brokers with `bind()` then `start(addresses)`.** Ephemeral ports (no clashes) and real Netty / Raft / file code; `crash()` kills a broker without goodbyes; the "block this peer" hook cuts network links at the socket level. A restarted broker reuses its port so the others reconnect.

**fsync off by default in fixtures, on in dedicated tests.** The default fixtures use `--unsafe-no-raft-fsync` so dozens of cluster scenarios run in seconds; one test per risk area runs with fsync on (an in-process "crash" cannot lose OS cache, so it proves the code paths work with real fsync; the `kill -9` demo in Step 9 and the file-level torn-tail fuzz tests prove the rest).

**Unique values, one audit function.** Values carry a sequence number so "present, in order, no unexpected duplicates" is a simple check.

## Alternatives I rejected and why
* Mocking the network: would not test Netty framing, reconnects or back-pressure.
* One JVM per broker in tests: slow and hard to debug; the real-process demo comes in Step 9.
* Sleeping instead of polling: flaky and slow. Every wait polls a condition with a timeout and prints the cluster state when it fails.

## What goes wrong if you get it wrong
* Checking only that "something was acknowledged": a lost acknowledged record would go unnoticed.
* Forbidding all duplicates: at-least-once retries legitimately duplicate; forbidding them makes tests flaky for the wrong reason.
* Asserting immediately after healing: replicas need a moment to converge; the audit waits for catch-up, then compares byte for byte.

## Tradeoffs and honest limits
Real-time Raft timings (150-300 ms elections) make each scenario take seconds. Failures injected inside one JVM share a clock and the OS page cache, so power loss is not modelled (the Phase 4 simulator does that for the Raft core).

## How it is tested
These *are* the tests; Step 8 adds a randomized chaos test on the same fixture.
