# Ember Part 6 - Replicated Partitions (Phase 5)

## One-minute pitch

Until now Ember was one broker with a very careful disk, and a very careful Raft in a laboratory. If the machine died, the service died. Phase 5 plugs Raft into the broker. Now three (or five) brokers keep identical copies of every partition, and a produce is answered "OK" only after a **majority** of them has written it down. Kill the leader with `kill -9` in the middle of a producer loop and a new leader is serving about 0.8 seconds later, with every acknowledged record still there. Restart the dead broker and it catches up by itself.

The big idea: **every partition is its own Raft group**, plus one more group for the topic list. Each broker keeps two logs per partition: the Raft log (the signed minutes of every decision, the truth) and the data log (the published newspaper that consumers read, filled only from committed decisions).

---

## Step 0 - Preflight
**Why.** Never build on a broken base. The Phase 4 build was green; the Raft core had no wiring to any socket.

## Step 1 - Two small Raft extensions
**Why.** A real network has two problems the simulator hid. (1) One `AppendEntries` could carry a huge backlog; we added `maxAppendBytes` (always at least one entry, so progress is never blocked). (2) A leader cut off from everybody would keep believing it is the leader. **CheckQuorum** makes a leader step down, *without* changing the term, if it has not heard from a majority within an election timeout. Analogy: a captain who has not heard from the crew for a while stops giving orders.

## Step 2 - Protocol
**Why.** Brokers must talk to each other, and clients must find leaders. New: `RAFT_MESSAGE` (one-way, no reply), METADATA v2 (brokers, controller, leader per partition), and error codes 9-13 (NOT_LEADER, TIMED_OUT, LEADER_NOT_AVAILABLE, NOT_CONTROLLER, OVERLOADED). `REQUEST_TIMED_OUT` is deliberately called *ambiguous*: the record may or may not exist.

## Step 3 - FileRaftStorage
**Why.** Raft's promises hold only if term, vote and log are on disk before any reply. The state (term + vote) is one 16-byte file replaced atomically (write temp, fsync, rename, fsync the directory). The log is a file of checksummed frames; on open we cut at the first bad frame, like the Phase 1 recovery. A corrupt state file is *refused*, never guessed. Analogy: a notary who will not stamp a page until the ink is dry.

## Step 4 - GroupRuntime and the state machines
**Why.** `RaftNode` is single-threaded by design; the broker is multi-threaded. `GroupRuntime` wraps one node in a mailbox: anything touching the node becomes a task in a queue that one worker drains at a time. `propose()` returns a future that completes when the entry is **applied** and its **term still matches**; if leadership changed, it fails with NotLeader instead of lying.

**Idempotent apply.** After a restart we replay the Raft log into the data log, but part of the data log may already exist. For each batch we compare "where replay thinks we are" with "what the data log already holds": skip, append all, or append only the missing tail. We crashed at every byte of a file to prove it.

## Step 5 - Broker integration
**Why.** `ReplicaManager` owns the topic-list group and one group per partition on every broker. PRODUCE = propose, wait for apply, map errors. FETCH is served only by a leader that has applied an entry of its own term (so it knows everything committed). Single-broker mode is just a cluster of one on the same code path, so the whole old test suite exercises the Raft path too.

## Step 6 - The cluster client
**Why.** A client must follow leaders. `ClusterClient` keeps a leader cache, asks any broker for METADATA v2, redirects topic creation to the controller, retries retriable errors with backoff and refreshes metadata. A nonexistent partition fails fast instead of being retried forever.

## Step 7 - Cluster integration tests
**Why.** Test the promise, not the code path: *an acknowledged record is never lost or reordered.* Twelve scenarios: topic created through a follower, byte-exact replicas, keyed ordering (50 keys x 200), leader failover under continuous produce, crashed broker catches up, isolated leader steps down, majority loss (nothing acknowledged), whole-cluster restart, controller crash during CREATE_TOPIC, five brokers tolerate two failures but not three, fsync-ON crash of every broker, and a broker whose data log was deleted rebuilds it from the Raft log.

## Step 8 - Chaos test
**Why.** Hand-written tests find what you thought of. A seeded random schedule of crashes, isolations and cut links, with two producers running, finds the rest. At the end every acknowledged record must be present, per-key order kept, offsets contiguous, replicas byte-identical. The default run lasts 10 s; the slow one 60 s.

## Step 9 - Real processes
**Why.** In-process "crashes" keep the OS cache and the clock. `kill -9` of a real process is honest. Scripts start three shaded jars; the demo kills the partition-0 leader during a producer loop.

## Step 10-11 - Docs and final verification
**Why.** The failure matrix in `docs/phase5/design.md` states what the client sees in each failure, including "the record may or may not exist".

---

## Bugs and surprises we hit (all real)

1. **Followers waited the full 5 s on every request.** `BrokerApis` decided "am I a single broker?" from the *flags*, while the test cluster passed the real addresses later at `start()`. Followers thought they were alone and waited for their own election. A test that should take seconds took 40 s and then hung. We found it with `jstack` (a thread parked in `waitForLeaderIfAlone`). Fix: decide from the actual cluster addresses. Lesson: when a wait seems "just slow", take a thread dump.
2. **A client that trusted one broker.** A restarted broker had not replayed the topic list yet, so it answered "unknown topic" or "no leader". Three integration tests failed. Fix: the client asks brokers until the layout of the topic is complete, and merges the answers (a known leader wins over "unknown").
3. **Fail-fast broke tests.** To avoid retrying a nonexistent partition forever we added an early check, which then broke two CLI tests that expected a specific error. Fixed properly instead of weakening the tests.
4. **`LogRecord` equality.** `LogRecord` holds a `byte[]`, and arrays compare by identity, so two identical records looked different in a test. We compare content summaries.
5. **The deadlock found only with real processes.** Raft messages from other brokers were handled on the same thread pool as client requests. A PRODUCE waits (blocked) for its commit, and Netty pins a connection to one thread: the acknowledgement the produce waited for could be queued behind it. Symptoms in the real run: sometimes a produce took 1.4 s, and leaders occasionally resigned because CheckQuorum saw no replies for 4-6 s (we printed the ages to prove it). With one handler thread it is a certain deadlock. Fix: `PeerMessageHandler` handles Raft messages on the I/O event loop, only dropping them into the right mailbox. A regression test (`PeerTrafficIsolationTest`) fails without the fix (even topic creation deadlocks with one handler thread) and passes with it. After the fix: kill-to-recovery 0.60 s in the demo. Lesson: a blocking wait inside a thread that also carries the answer is a classic deadlock.
6. **Heartbeat config validation.** The easy way to build a `RaftConfig` with faster timings failed validation (heartbeat 50 ms vs election minimum 50 ms). We used the full constructor.
7. **A tooling surprise.** macOS `sed` treated an "any digits" pattern as matching empty strings and produced a garbage bootstrap list, so the first benchmark script printed nothing. We build the list with a loop.
8. **Mutation numbers moved.** After CheckQuorum became part of the random Phase 4 scenarios, the bug-detection table changed (for example the sync-order bug is found at seed 66 instead of 164). We re-measured and updated the doc instead of leaving stale numbers.

## Real numbers

* Tests: storage 55, raft 182, protocol 83, replication 42, client 22, broker 154 = **538**; `mvn clean verify` 2 min 46 s. With `-Pcluster-long`: 539, 3 min 49 s.
* Cluster chaos: default (10 s) **5 runs in a row** passed; slow (60 s) **3 runs** passed with different seeds, 152,000-169,000 acknowledged records each, 49-55 faults each, zero missing records, replicas identical.
* Demo (3 real processes, `kill -9` of the partition-0 leader): 301/301 produce calls OK, 0 acknowledged records missing among 501, first acknowledged produce to the dead leader's partition **0.60 s** after the kill, all brokers equal after restart.
* Failover over 5 cycles: **0.82 / 0.84 / 1.65 s** (min / median / max).
* Throughput (4 partitions, 4 producers, 100 B, 50,000 records, one run each): 1 broker 22,804 rec/s; 3 brokers fsync ON 10,858 rec/s; 3 brokers no Raft fsync 42,063 rec/s.
* Machine: Apple M4, 10 cores, 16 GiB, macOS, Java 21, local SSD, everything on one machine.

## Glossary
* **Raft group** - the set of replicas that agree on one log.
* **Leader / follower** - the replica that accepts writes / the ones that copy it.
* **Commit** - stored on a majority; it will not be lost.
* **Apply** - executing a committed entry (here: writing it into the data log).
* **Majority (quorum)** - more than half of the brokers.
* **CheckQuorum** - a leader that hears from no majority steps down.
* **Data log vs Raft log** - what consumers read vs the durable record of decisions.
* **Idempotent apply** - applying the same entry twice gives the same result.
* **Controller** - the leader of the topic-list group; creates topics.
* **At-least-once** - a record is never lost, but may be delivered twice.
* **Ambiguous error** - timeout where the record may or may not exist.

## Suggested NotebookLM prompts
1. *Video overview:* "Explain Ember Phase 5 for a student: why each partition is a Raft group, the two logs (published newspaper vs signed minutes), what happens to one produce request step by step, and the real deadlock bug where Raft messages shared a thread pool with blocked produce requests. Use the kill -9 demo as the climax."
2. *Quiz:* "Write 12 questions with answers from this document: when is a produce acknowledged, what is CheckQuorum, why does apply have to be idempotent, what can a client see when the leader crashes after commit but before the answer, and why is exactly-once not guaranteed. Mix multiple choice and short answers, and add two questions about the bugs we hit."
