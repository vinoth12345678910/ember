# Step 4 - Broker integration

## What we are building
* A new Raft group `__groups` (partition 0) on every broker, next to `__meta`, with all brokers as voters. Its state machine is the `GroupStateMachine`. The **coordinator** for all groups is the current leader of `__groups`.
* `ReplicaManager` owns it and a `GroupCoordinator`; a ticker calls `expireSessions()` every `tickIntervalMs()`.
* Seven handlers in `BrokerApis` (FIND_COORDINATOR, JOIN_GROUP, HEARTBEAT, LEAVE_GROUP, OFFSET_COMMIT, OFFSET_FETCH, DESCRIBE_GROUP) and their routing in `RequestHandler`.
* Config: `--group-min-session-ms` (1000) and `--group-max-session-ms` (60000).
* A test hook: `ReplicaManager.groupState().snapshot()` to compare the replicated state across brokers.

## Why it is designed this way
**One more Raft group, same machinery.** Offsets and memberships must survive broker crashes, so they live in a Raft log exactly like topic metadata and partitions do. We reuse `GroupRuntime`, `FileRaftStorage`, `PeerNetwork` and the `RAFT_MESSAGE` envelope; the only new things are the state machine and the coordinator. A new group name needed no protocol change because RAFT_MESSAGE already carries (topic, partition) of the group.

**Why a single group for all consumer groups.** Simplicity: one leader = one coordinator = one place to ask. Kafka shards consumer groups over the partitions of `__consumer_offsets` (hash of the group id) so that different brokers coordinate different groups. That is the scale-out path; here every group-related request goes to one broker.

**FIND_COORDINATOR is answered by any broker** from its own Raft view (it only needs to know who leads `__groups`), so a client with just a bootstrap list can find the coordinator whichever broker it asks first. Group requests that reach a non-leader are answered `NOT_COORDINATOR`, and the client asks again.

**Single-broker mode** is the same path: a cluster of one wins its own election; group requests wait (bounded) for it like PRODUCE does.

**Old data directories** keep working: if `__raft/__groups-0` does not exist it is created empty.

## Alternatives I rejected and why
* Keeping offsets in a normal topic (`__consumer_offsets` as a data log): would reuse the produce path but gives no compare-and-set on the generation, so the zombie fencing could not be linearized with membership changes.
* Running the expiry timer inside the Raft group's serial context: it would block that context while waiting for its own proposal to commit (a deadlock of the same family as the Phase 5 peer-thread bug).
* A new request pool for group APIs: group requests block their handler thread while a proposal commits, just like PRODUCE. The Phase 5 rule applies: Raft peer traffic is handled on the I/O thread (`PeerMessageHandler`), so a blocked handler can never starve the acknowledgements it waits for.

## What goes wrong if you get it wrong
* Expiry computed from a stale term: a mass kick-out after a failover (the coordinator resets its heartbeat memory when the Raft term changes).
* A `__groups` message arriving for an unknown group is dropped silently; so the group must be created before the broker joins the cluster (it is, in the constructor).
* Reusing a topic name starting with `__` for users: reserved, rejected by CREATE_TOPIC.

## Tradeoffs and honest limits
* Every group request is served by one broker (the sharding path is documented, not built).
* The group state lives in memory and is rebuilt by replaying `__groups` at startup; the log is never compacted (Phase 8), so a busy group makes the log grow forever. Each OFFSET_COMMIT is one fsynced Raft entry: that is the price of surviving broker crashes (measured in step 9).
* A DESCRIBE or OFFSET_FETCH answered by a deposed coordinator that has not noticed yet may be slightly stale.

## How it is tested
`GroupApisOverTheWireTest` with raw frames: the full lifecycle against a single broker, session timeout validation, FIND_COORDINATOR on every broker of a 3-broker cluster, NOT_COORDINATOR on followers, replication of the group state to all brokers, real-time session expiry, and a coordinator crash that leaves members and offsets intact. All earlier tests still pass with the extra Raft group.
