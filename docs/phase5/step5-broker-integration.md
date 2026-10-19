# Step 5 - Broker integration: a cluster of brokers

## What we are building
The broker becomes one member of a cluster:
* `BrokerConfig`: `--cluster=1@host:port,2@host:port,...`, Raft timings, produce timeout, overload limit, peer queue size, `--unsafe-no-raft-fsync`, `--data-flush`. No `--cluster` = a cluster of one, **same code path**.
* `ReplicaManager`: owns the **META group** (the cluster's topic list) and one Raft group per topic partition, creating them when a `CREATE_TOPIC` entry is applied.
* `PeerNetwork`: one outbound Netty connection per peer broker, a bounded queue per peer, reconnect with back-off; inbound `RAFT_MESSAGE` frames are dropped into the right group's mailbox without blocking.
* `BrokerApis` rewritten: PRODUCE (propose and wait for apply), FETCH (leader only, committed data only), METADATA v0/v1/**v2**, CREATE_TOPIC (controller only).
* `BrokerServer`: two-phase start (`bind()` gives the real port, `start(addresses)` joins the cluster), `crash()` and a "block this peer" hook for tests.

## Why it is designed this way
**Every broker holds every partition (replication factor = cluster size).** No broker "assignment": a partition's Raft group has all brokers as voters. That removes partition placement, reassignment and ISR bookkeeping (all out of scope). The cost: capacity does not grow by adding brokers, and a 5-broker cluster stores 5 copies. Real systems pick a replica subset per partition; here it keeps the lesson about *replication*, not placement.

**Why Raft's majority replaces Kafka's ISR.** Kafka keeps an *in-sync replica set* per partition and acknowledges when all ISR members have the record; the set shrinks when a replica lags and a controller coordinates elections among ISR members. It needs a separate consensus system (ZooKeeper/KRaft) to decide the ISR and the leader. Raft folds all of this into one rule: acknowledge when a **majority** has the entry, elect only a candidate whose log is at least as up to date as a majority's. A slow replica simply does not count; nothing needs shrinking or expanding. The trade: with 3 brokers you tolerate 1 failure and always pay for a majority round trip (Kafka can tolerate N-1 failures with `acks=all` and ISR=N at the price of waiting for the slowest ISR member, and can lose data in "unclean" elections).

**The controller is simply the leader of the META group.** Topic creation goes through its Raft log, so every broker applies `CREATE_TOPIC` in the same order and agrees on the topic list. Auto-creation on first PRODUCE therefore works only on the controller; elsewhere clients get `UNKNOWN_TOPIC_OR_PARTITION` and create topics explicitly through the controller (`NOT_CONTROLLER` + METADATA v2's `controllerId` tell them where).

**PRODUCE is acknowledged after commit AND apply.** The leader stamps the batch with its clock, proposes one `PRODUCE_BATCH` entry, and replies when the entry has been applied on this broker with the term it was proposed in. FETCH only reads the data log, which only contains applied entries, so consumers can never see uncommitted data.

**A new leader serves fetches only after it has applied an entry of its own term** (its no-op). Until then its data log may lag behind what the previous leader committed; serving it would show an end offset that is too small. During that short window FETCH says `LEADER_NOT_AVAILABLE`.

**Peers are one-way, bounded and never block.** Raft assumes lost, duplicated and reordered messages, so a full queue drops the *newest* message (counted) and a dead connection simply loses its queue; heartbeats re-synchronize. A group thread never waits for the network. A hook can block traffic with a given peer at the real-socket level to simulate partitions in tests.

**Two-phase start.** Ephemeral ports (tests!) are only known after binding, but a broker must know its peers' addresses before joining. So `bind()` opens the port first; `start(addresses)` then builds the replication machinery.

**Single-broker mode** is a cluster of one: the same Raft code commits immediately. `start()` waits until the META group has elected the broker so that the first request does not race the election; a PRODUCE or FETCH to a freshly created partition waits (bounded by the produce timeout) for the partition's own election. In a real cluster these cases return a retriable error instead.

**Legacy data directories are refused**: Phase 2/3 data has no Raft log, so adopting it would invent history. The broker fails fast with a clear message.

## Alternatives I rejected and why
* ISR-style replication with a separate failure detector: needs more machinery than the Raft core we already tested to death.
* Letting FETCH read uncommitted data from followers or leaders ("read your own writes faster"): the guarantee "consumers only see committed data" is worth more.
* Blocking Netty I/O threads on commit: the handler runs on a separate executor group (as since Phase 2).
* A global lock instead of per-group mailboxes: partitions must not block each other.

## What goes wrong if you get it wrong
* Acknowledging at commit but before apply: the client could be acknowledged and then not find its record on the leader.
* Auto-creating on non-controllers: two brokers create the same topic concurrently with different partition counts.
* Serving FETCH from a fresh leader before its no-op is applied: a consumer sees the end of the log go *backwards* relative to the old leader.
* Blocking inside a group thread on the network: one dead peer freezes every partition.

## Tradeoffs and honest limits
No snapshots: the Raft log grows and a restart replays it. No authentication between peers. A deposed leader that has not yet noticed can serve a stale end of log (never uncommitted data). Replicas may differ slightly in `logStartOffset` (each runs retention with its own clock). Handler threads block while waiting for the commit (up to the produce timeout).

## How it is tested
The old single-broker integration tests run unchanged against the new code path (fixtures only gain fast Raft timings and `--unsafe-no-raft-fsync`), plus `BrokerApis` / config / legacy-directory unit tests. Cluster behaviour is tested in Step 7.
