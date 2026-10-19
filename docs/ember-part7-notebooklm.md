# Ember Part 7 - Consumer Groups (Phase 6)

## One-minute pitch

Until now a consumer in Ember was alone: you told it which partitions to read and it remembered its place only in memory. If it died, nobody knew where it had stopped. Phase 6 turns consumers into a **team**. Several consumers join a **group**; the broker divides the partitions among them; when somebody joins, leaves or crashes, the broker re-divides the work by itself; and every consumer saves its place (a **committed offset**) in a replicated log, so that after any crash the work continues from the last saved place.

The hard part is not dividing partitions. It is stopping a **zombie**: a consumer that was paused, got replaced, and wakes up believing it still owns its partitions. Every message therefore carries a **generation**, the version number of the seating chart, and the replicated log refuses anything written from an old chart. The honest price is **at-least-once** delivery: after a crash, some records are delivered again, and the window is bounded by the commit interval.

---

## Step 0 - Preflight
**Why.** Never build on a broken base. The Phase 5 build was green (538 tests) and the real-process failover demo had worked.

## Step 1 - Protocol
**Why.** The group needs seven conversations: *who is the head waiter* (FIND_COORDINATOR), *I want to join* (JOIN_GROUP), *I am alive* (HEARTBEAT), *I am leaving* (LEAVE_GROUP), *save my place* (OFFSET_COMMIT), *where is my place* (OFFSET_FETCH) and *show me the whole team* (DESCRIBE_GROUP). Seven new error codes tell the consumer what to do: retry elsewhere (NOT_COORDINATOR), rejoin (UNKNOWN_MEMBER_ID, REBALANCE_IN_PROGRESS), you are fenced (ILLEGAL_GENERATION), or you made a mistake (INVALID_SESSION_TIMEOUT, PARTITION_NOT_ASSIGNED). Same strict rules as before: check lengths first, bound every list, reject trailing bytes.

## Step 2 - The state machine and the assignor
**Why two small commands.** Everything that must survive a crash is a Raft entry. There are only two kinds: *install this complete seating chart* (SET_MEMBERSHIP) and *save these places* (OFFSET_COMMIT).

**Why the leader computes and the state machine only installs.** Every broker replays the log, today and in five years, and must reach exactly the same state. If `apply()` had to work out the seating itself it would need the current topic list (kept in another Raft group, at a different position on each broker) and a new random member id. Two brokers would silently disagree about who owns partition 3. So the coordinator computes everything first, and the command says "here is the complete new chart, built on generation N". If someone else changed the chart first, the answer is STALE_GENERATION, nothing changes, and the coordinator recomputes. Analogy: a head waiter who hands the restaurant a finished seating chart on paper; the restaurant only checks that it replaces the version it expected.

**Fencing in `apply`.** An offset commit is accepted only if the member exists, the generation is current, and the member owns those partitions. The check is inside `apply`, so the Raft log orders a commit and a rebalance that happen at the same time; the loser is simply rejected.

**The assignor.** Range assignment: members sorted by id get contiguous ranges, balanced to within one. Simple and testable (2,000 random inputs). Its known weakness: adding one member moves many partitions (it is not "sticky").

## Step 3 - The coordinator
**Why two kinds of state.** The seating chart and the saved places are written in ink (Raft). "When did each waiter last wave at the head waiter" is not: it changes every few seconds per consumer, and putting it in the log would fill the log with entries worth nothing a minute later. Heartbeats live only in the leader's memory. When a new head waiter takes over he starts everybody's clock fresh: a full session of grace, so a broker failure does not throw every consumer out. The price: a dead consumer is noticed up to one extra session later after a failover.

**Idempotent rejoin.** A consumer told "the chart changed" rejoins; if its subscription is unchanged the coordinator just returns the current chart. Without this, every rejoin would change the chart and everybody would rejoin again, for ever (a rebalance storm).

**Manual clock.** The coordinator reads time from a function we can control, so its tests (22 of them) run in microseconds and never sleep.

## Step 4 - Broker integration
**Why.** One more Raft group, `__groups`, on every broker, created beside `__meta` and empty if the directory does not exist (so Phase 5 data keeps working). The coordinator is simply whoever leads it. FIND_COORDINATOR can be asked of any broker.

## Step 5 - The consumer
**Why a separate heartbeat thread.** If beats came only from `poll()`, a consumer busy with one slow batch would look dead. The thread uses its own connections and only sets a flag; `poll()` does the rejoin. The price: a stuck user thread keeps the membership alive, so after `maxPollIntervalMs` without a poll the consumer leaves.

**Process, then commit.** Positions advance only for records `poll()` returned; auto-commit commits the previous poll's positions.

## Step 6 - The CLI
**Why.** A group is invisible without a window. `group describe` shows the generation, who owns which partition, the committed offset, the log end and the **lag**: the one number an operator watches.

## Step 7 - Scenario tests
**Why.** Each promise has a test: three consumers split six partitions; a fourth joins; one leaves; one crashes and its uncommitted records are delivered again; a zombie is fenced and the new owner's offsets stay untouched; the coordinator broker crashes and the members keep their ids; the whole cluster restarts; retention removes the committed offset.

## Step 8 - Chaos
**Why.** A random schedule of broker crashes, cut links, killed consumers and consumers frozen beyond their session, under constant production. At the end every acknowledged record must have been delivered at least once, committed offsets must never pass the end of the log, every partition must have exactly one owner, and all brokers must agree.

## Step 9 - Real processes
**Why.** A real `kill -9` gives no goodbye. Separate observer processes with their own clocks time the recovery.

---

## Bugs and surprises we hit (all real)

1. **The "next free number" trap.** Two Phase 5 tests asserted that API key 11 and error code 14 were *unknown*. Adding the group APIs made them fail. They tested a boundary, not a feature; we moved the boundary (18 and 21).
2. **A tick that could not grow.** `tickIntervalMs()` (how often the coordinator checks for expired sessions) started from the configured minimum session, so it could never exceed that minimum divided by four. Found by writing the test that says "a quarter of the shortest session in use".
3. **`--max` committed records nobody had seen.** `consume --group --max=20` fetched 30 records, printed 20, and `close()` committed position 30: the ten unprinted records would have been skipped forever. The consumer had no way to say "I only processed up to here". Fix: `seek()`, and the CLI rewinds to the first unprinted record. Caught by the lag assertion in the CLI test. This is the at-least-once rule biting in the most ordinary place.
4. **A test that guessed.** The zombie test assumed 10 records per partition. Keys spread unevenly (28 and 12), and the test waited forever for a total that never occurred. The test now compares with what the new owner actually received.
5. **A test monitor that raised false alarms.** The first chaos monitor failed 5 of 8 runs with "committed offset beyond the log end". The "end offset" it used came from a lagging follower when a partition leader was unreachable. Every failing run had passed the strict end-of-run check, which is how we knew the product was right. Fix: use only *ready leaders*, and require a suspected violation to persist for a second. Lesson: when a check fails, find out whether the product or the check is wrong, and prove it.
6. **A benchmark that timed its own goodbye.** The read-throughput tool stopped its clock after `close()`, which does a final commit and a leave (two fsynced Raft writes). It made auto-commit look 6-8% slower even when no commit had fired in the window. We noticed because the gap was identical whatever the run order. Fix: stop the clock before `close()`.
7. **A benchmark with nothing to measure.** The "consumption resumed" measurement produced its records before the kill, so the doomed consumer had already read them. Records now arrive after the kill.
8. **An unexplained slow test.** Once in four full runs the fsync-ON cluster test took 108 s instead of 2 s. It did not reproduce in the next two runs or when run alone. We did not find a cause and say so; a thread dump during a stall is the next step if it returns.

## Real numbers

* **Tests:** storage 55, raft 182, protocol 97, replication 90, client 22, broker 175 = **621**. `mvn clean verify` takes **3 min 24 s** (the two 60-second chaos runs are tagged `slow`); with `-Pcluster-long` they are included (broker module: 177 tests, 5 min 33 s).
* **Group chaos test:** the 10-second variant passed **5 times in a row** (seeds 354470591, 2371327370, 343746769, 1958353464, 2749179368) with 5,700-6,500 acknowledged records each; duplicate rate 4.9-15.3%. The 60-second variant passed **3 times** with different seeds (3591989813, 2040061411, 3544844221): 28,000-31,000 acknowledged records, 53-56 faults each, duplicate rate 7.5-9.7%, nothing lost. Two seeds that had failed because of the monitor flaw passed after the fix.
* **Real-process demo (3 broker processes, 3 consumer processes):** 3,900 of 3,900 distinct records delivered across a `kill -9` of a consumer and a `kill -9` of the coordinator broker; a fresh consumer in the same group re-read 0 committed records.
* **(A) Consumer `kill -9`, session timeout 3 s:** partitions reassigned after a median of 2.58 s (min 2.18, max 2.95); first record from the dead consumer's partitions delivered to a survivor after a median of 2.71 s (min 2.60, max 3.67).
* **(B) Coordinator broker `kill -9`:** next successful commit after a median of 0.76 s (min 0.74, max 1.57; the first run was slower and was not investigated).
* **(C) Reading 200,000 records:** manual consumer 1.40-1.45 million records/s; group consumer 1.33 million (auto-commit every 5 s), 1.25 million with commits every 100 ms. The read windows are only ~0.15 s, so differences below about 5% are noise.
* **(D) One OFFSET_COMMIT:** median 8.3 ms with Raft fsync (p99 13.3 ms); 0.26 ms without (p99 0.74 ms).
* **Machine:** Apple M4, 10 cores, 16 GiB RAM, macOS, Java 21, local SSD; all processes on one machine.

## Glossary
* **Consumer group** - consumers that share the work of reading topics.
* **Coordinator** - the broker (leader of `__groups`) that manages all groups.
* **Generation** - the version number of the seating chart; it grows with every change.
* **Rebalance** - dividing the partitions again after a change.
* **Session timeout** - how long the coordinator waits for a heartbeat before removing a member.
* **Heartbeat** - "I am alive" message; kept in memory, never in the log.
* **Committed offset** - the saved place of a group in a partition.
* **Lag** - log end minus committed offset: how far behind the group is.
* **Fencing** - refusing a request that carries an old generation.
* **Zombie** - a consumer that was replaced but does not know it yet.
* **At-least-once** - nothing is skipped; some records may be delivered twice.
* **Idempotent rejoin** - rejoining with the same subscription changes nothing.
* **Eager rebalance** - everybody stops and takes the new assignment.

## Suggested NotebookLM prompts
1. *Video overview:* "Make a video overview of Ember Phase 6 for a student who has built a replicated log but never a consumer group. Use the restaurant analogy (waiters, tables, a seating chart with a version number). Explain why heartbeats are not in the Raft log, why the leader computes the assignment and the state machine only installs it, and tell the zombie story step by step. End with the two bugs where a check or a benchmark was wrong and how we proved it."
2. *Quiz:* "Write 12 questions with answers from this document. Include: why a rejoin with the same subscription does not start a new generation; what happens to a commit that was in flight during a rebalance; where duplicates come from and what bounds them; what a new coordinator does about heartbeat times; why `consume --max` once lost records; and what the failure matrix says for a consumer paused longer than its session. Mix multiple choice and short answers."
