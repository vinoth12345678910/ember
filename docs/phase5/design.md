# Phase 5 design: replicated partitions

This document explains how a record travels through an Ember cluster, why the pieces are shaped the way they are, and exactly what is (and is not) guaranteed.

## 1. The life of one record

```
 client                 broker 1 (partition leader)                  brokers 2, 3 (followers)
   |  PRODUCE(topic,p,records)            |                                     |
   |------------------------------------->|                                     |
   |                                      | 1. am I the Raft leader of t-p?     |
   |                                      |    no -> NOT_LEADER_OR_FOLLOWER     |
   |                                      | 2. build ONE entry:                 |
   |                                      |    PRODUCE_BATCH(leader's clock,    |
   |                                      |                  records)           |
   |                                      | 3. propose(): append to the RAFT    |
   |                                      |    LOG, fsync (outbox rule)         |
   |                                      |--- AppendEntries(entry) ----------->| 4. append to own RAFT LOG,
   |                                      |                                     |    fsync, THEN reply
   |                                      |<-- AppendEntriesResponse(ok) -------|
   |                                      | 5. majority (2 of 3) has it:        |
   |                                      |    COMMIT. commitIndex advances     |
   |                                      | 6. APPLY: write the records into    |--- (next heartbeat tells them
   |                                      |    the DATA LOG, base offset =      |     the new commitIndex)
   |                                      |    running offset                   | 7. they apply too: same entry,
   |                                      | 7. complete the waiting future      |    same order -> identical
   |<-------- ProduceResponse(baseOffset)-|       (term must match!)            |    data log, same offsets
   |                                      |                                     |
   |  FETCH(topic,p,offset)               |                                     |
   |------------------------------------->| 8. leader only; reads the DATA LOG  |
   |<-- records (zero-copy from the file) |    which only holds applied entries |
```

The acknowledgement (step 7) happens only after commit **and** apply on the leader, so a consumer who fetches after the producer got its answer always finds the record.

## 2. Why two logs, and what it costs

Each replica has two logs for each partition:

| | Raft log | Data log |
|---|---|---|
| Written | when an entry is proposed / replicated | when an entry is **committed and applied** |
| Durable | fsynced before any reply (the truth) | not fsynced by default (`--data-flush=never`) |
| Contents | every entry, including no-ops and uncommitted tail | only committed records |
| Read by | Raft only | consumers (zero-copy fetch) |
| Can be lost? | no (that would be data loss) | yes: it is rebuilt from the Raft log |

Analogy: the **Raft log** is the newsroom's signed minutes of every decision, kept safe; the **data log** is the published newspaper readers see. If the newspaper's last pages are lost in a fire, the newsroom reprints them from the minutes.

Benefits: consumers can never see uncommitted data; the high watermark is just the end of the data log; zero-copy fetch is untouched; truncating the Raft log (a new leader overwriting a stale tail) never touches data readers have seen. **Costs:** every record is written twice (once in each log) and stored twice on disk; the Raft log never shrinks (no snapshots, a documented non-goal) so restart replays everything. **Alternative:** unify them as production systems do (the Raft log *is* the partition log and a "commit index" marks how far readers may read). That halves the writes but entangles Raft's truncation with the storage engine's index, retention and zero-copy paths; we chose clarity for a learning project.

## 3. Idempotent apply: surviving a crash in the middle of a batch

After a restart a node replays committed entries from index 1 and keeps `runningOffset` = the number of records contained in the entries applied so far (it restarts at 0). The data log, however, may already hold part of that history. Rule for a batch of `n` records:

* `runningOffset + n <= dataLog.nextOffset()` -> already there: skip;
* `runningOffset == dataLog.nextOffset()` -> append all `n`;
* `runningOffset < dataLog.nextOffset() < runningOffset + n` -> append only the missing tail (records from position `nextOffset - runningOffset`);
* `runningOffset > nextOffset` -> impossible unless the data log lost more than a torn tail: fail loudly.

**Worked example.** Batches A (3 records), B (4), C (2), i.e. 9 records. The process dies while B is being written; Phase 1 recovery cuts the torn record, leaving 5 records (A whole, B's first two). Replay: A: `0+3 <= 5` skip, `running=3`. B: `3 < 5 < 7` -> append the last `7-5 = 2` records, `running=7`. C: `7 == 7` -> append both, `running=9`. The data log holds exactly records 0..8, identical to every other replica; no duplicates, no gaps. `IdempotentApplyTest` crashes at *every* byte of the file to prove it. Data-log retention removes old segments but never changes `nextOffset`, so the arithmetic stays valid.

## 4. Why Raft's majority replaces Kafka's ISR

| | Kafka (ISR) | Ember (Raft) |
|---|---|---|
| Acknowledge when | all *in-sync* replicas have it (`acks=all`) | a **majority** of all replicas has it |
| Slow replica | dropped from the ISR after a timeout; the ISR shrinks | simply not needed for the majority; nothing to shrink |
| Leader election | the controller picks any ISR member | only a candidate whose log is at least as up to date as a majority's can win |
| Tolerated failures | up to N-1 with an ISR of N (if `min.insync.replicas` allows) | floor((N-1)/2) |
| Failure mode | if the ISR shrinks to the leader alone and it dies, you must choose between unavailability and an *unclean* election that can lose acknowledged data | never loses acknowledged data while a majority of logs survives; stops instead (CAP: consistency over availability) |
| Extra machinery | separate metadata consensus (ZooKeeper / KRaft), ISR bookkeeping, high watermark exchange | the one Raft algorithm, already tested by a simulator |

Honest trade-off: Raft pays a majority round trip on every write and tolerates fewer failures for the same number of replicas; Kafka's ISR can be faster and more available but needs more moving parts and lets you choose to lose data. For a learning project and for "no acknowledged record is ever lost", the single simple rule wins.

## 5. Failure matrix

| Situation | What happens | What the client sees |
|---|---|---|
| Leader crashes **before** it proposed the record | nothing was stored | connection error / `NOT_LEADER...`; the client refreshes metadata and retries: no duplicate |
| Leader crashes **after propose, before commit** | the entry may be on a minority or a majority of Raft logs. If a majority has it, the new leader commits it; otherwise the new leader overwrites it | the call fails (`NOT_LEADER...`, timeout or connection error); the record may or may not exist. A retry **may duplicate** (at-least-once) |
| Leader crashes **after commit, before the ack** | the record is committed and will survive | the call fails although the record exists; a retry **duplicates** it |
| Follower crashes | majority still exists; the follower later restarts, replays its Raft log and catches up from the leader | nothing (maybe slightly slower) |
| Leader isolated from the others | CheckQuorum makes it step down within about one election timeout; pending produces fail with `NOT_LEADER...`; the majority elects a new leader | `NOT_LEADER_OR_FOLLOWER`, then success at the new leader. Its uncommitted proposals are overwritten after healing |
| Majority lost (e.g. 2 of 3 down) | no commits possible; leader steps down (CheckQuorum) | retriable errors / `REQUEST_TIMED_OUT`; **nothing is acknowledged**. Progress resumes when a majority returns |
| Controller (META leader) crashes | the other brokers elect a new META leader; topic metadata is replicated everywhere | `CREATE_TOPIC` gets `NOT_CONTROLLER` / connection error and is retried at the new controller; if the first attempt had committed, `TOPIC_ALREADY_EXISTS` |
| Broker restart | Raft state (term, vote, log) restored from disk; META log replayed -> topics and partition groups recreated; data logs repaired by idempotent apply | brief unavailability of the partitions it led |
| Data log torn tail (power loss) | Phase 1 recovery cuts the torn record; replay re-appends exactly the missing tail | nothing |
| Raft log torn tail | `FileRaftStorage` truncates to the last valid entry; the entries lost were not yet acknowledged to anybody (replies are sent only after fsync) or are re-fetched from the leader | nothing |
| Raft log corrupt in the **middle** | truncated at the first bad entry: this replica "forgets" more than it acknowledged. Safe only while a majority of other replicas still has the data | nothing, but see limitations |

## 6. Exact guarantees and non-guarantees

**Guaranteed** (assuming at most a minority of brokers fail permanently and Raft logs are fsynced):
1. A PRODUCE that returned success is stored on a majority of Raft logs and applied on the leader. It is never lost and never reordered relative to other acknowledged records of the same partition, across any number of leader changes.
2. Offsets are identical on every replica; every replica's data log converges to a byte-identical copy.
3. FETCH returns only committed data (each leader serves reads only after it has applied an entry of its own term).
4. Per-partition order; same key => same partition => ordered (Phase 3 rules still hold). No order across partitions.
5. A topic exists exactly once with a fixed partition count, decided by the META log.

**Not guaranteed:**
* **No exactly-once.** Retries after a timeout, a lost connection, or `NOT_LEADER...` returned after the record was proposed can duplicate a record. (Idempotent producer: Phase 8.)
* **Stale end-of-log from a deposed leader.** A leader that lost leadership but has not noticed yet can still answer FETCH from its data log: possibly an end offset that is behind the new leader, but never uncommitted or lost data. There are no linearizable reads (no ReadIndex).
* **Metadata can lag** by a moment on followers (a freshly created topic may briefly be unknown to them); clients retry.
* Replicas may differ slightly in `logStartOffset` (each runs retention with its own clock).
* Availability requires a majority; a minority partition cannot make progress.
* `--unsafe-no-raft-fsync` voids the durability promise.
* Peers are trusted: no authentication or encryption between brokers.

## 7. Known limitations and the plan

* Replication factor = cluster size; no partition placement, no adding or removing brokers, no Raft membership change. *(future)*
* No Raft snapshots: the Raft log grows without bound and restart replays it. *(a later phase)*
* One request in flight per client connection; no follower fetching; no long polling.
* **Phase 6**: consumer groups and committed offsets (offsets will be stored in a replicated `__consumer_offsets`-like group using this same machinery).
* **Phase 7**: back-pressure, quotas, observability.
* **Phase 8**: idempotent producer (producer id + sequence numbers deduplicated inside the Raft-applied state machine) - removes the duplicate caveat.
