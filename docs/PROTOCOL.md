# Ember wire protocol (v0; METADATA v1 and CREATE_TOPIC from Phase 3; METADATA v2, RAFT_MESSAGE and error codes 9-13 from Phase 5)

TCP, binary, request/response. All integers are **big-endian** and signed.

| Type   | Size    | Meaning |
|--------|---------|---------|
| INT16  | 2 bytes | signed |
| INT32  | 4 bytes | signed |
| INT64  | 8 bytes | signed |
| STRING | 2 + n   | INT16 length n (0..32767) then n bytes of UTF-8 |

Topic name rule: 1..128 characters from `[a-zA-Z0-9._-]`.

## Frames

### Request frame

| Field | Type | Notes |
|-------|------|-------|
| frameLength   | INT32 | number of bytes that FOLLOW this field |
| apiKey        | INT16 | 0 = PRODUCE, 1 = FETCH, 2 = METADATA, 3 = CREATE_TOPIC, 10 = RAFT_MESSAGE (broker to broker) |
| apiVersion    | INT16 | 0 for every API; METADATA also accepts 1 and 2 (see Compatibility) |
| correlationId | INT32 | chosen by the client, echoed in the response |
| body          | ...   | depends on apiKey |

### Response frame

| Field | Type | Notes |
|-------|------|-------|
| frameLength   | INT32 | number of bytes that FOLLOW this field |
| correlationId | INT32 | echo of the request's |
| errorCode     | INT16 | 0 = no error |
| body          | ...   | **absent when errorCode != 0**: the frame ends right after errorCode |

The response does not say which API it answers; the client remembers (it has one request in flight).

## PRODUCE (apiKey 0)

Request body:

| Field | Type | Notes |
|-------|------|-------|
| topic | STRING | |
| partition | INT32 | must exist: 0 <= partition < the topic's partitionCount |
| recordCount | INT32 | >= 1 |
| records | repeated recordCount times: | |
| - keyLength | INT32 | -1 = null key, otherwise >= 0 |
| - key | bytes | absent when keyLength = -1 |
| - valueLength | INT32 | >= 0 |
| - value | bytes | |

Response body:

| Field | Type | Notes |
|-------|------|-------|
| topic | STRING | |
| partition | INT32 | |
| baseOffset | INT64 | offset of the FIRST record; the rest are baseOffset+1, +2, ... |
| recordCount | INT32 | |

## FETCH (apiKey 1)

Request body:

| Field | Type | Notes |
|-------|------|-------|
| topic | STRING | |
| partition | INT32 | must exist: 0 <= partition < the topic's partitionCount |
| fetchOffset | INT64 | first offset wanted |
| maxBytes | INT32 | >= 1; the broker caps it at its maxFetchBytes |

Response body:

| Field | Type | Notes |
|-------|------|-------|
| topic | STRING | |
| partition | INT32 | |
| logStartOffset | INT64 | oldest offset still on the broker |
| highWatermark | INT64 | Phase 2: equals the log end offset (next offset to be assigned). **Phase 5 will change this to the replicated commit point.** |
| recordsLength | INT32 | number of record bytes that follow (may be 0) |
| records | bytes | zero or more COMPLETE `LogRecord` encodings, byte-for-byte identical to the log files on disk, contiguous, starting exactly at fetchOffset |

### LogRecord encoding (the records field and the on-disk format)

| Field | Type | Notes |
|-------|------|-------|
| length | INT32 | bytes AFTER this field |
| crc | INT32 | CRC32 of everything after the crc field |
| offset | INT64 | |
| timestamp | INT64 | assigned by the broker |
| keyLength | INT32 | -1 = null key |
| key | bytes | |
| value | bytes | whatever remains of `length` |

## METADATA (apiKey 2)

Request body:

| Field | Type | Notes |
|-------|------|-------|
| topicCount | INT32 | 0 means "all topics" |
| topics | topicCount x STRING | |

Response body:

| Field | Type | Notes |
|-------|------|-------|
| nodeId | INT32 | |
| host | STRING | the advertised host |
| port | INT32 | |
| topicCount | INT32 | |
| topics | repeated: STRING topic, INT32 partitionCount (always 1), then per partition: INT32 partition (0), INT64 logStartOffset, INT64 logEndOffset | sorted by topic name; unknown requested topics are omitted |

## METADATA version 1 (apiKey 2, apiVersion 1)

Request body: identical to v0.

Response body (the ONLY difference from v0 is the added `leaderNodeId` before `logStartOffset`):

| Field | Type | Notes |
|-------|------|-------|
| nodeId | INT32 | |
| host | STRING | the advertised host |
| port | INT32 | |
| topicCount | INT32 | |
| topics | repeated: STRING topic, INT32 partitionCount, then per partition: INT32 partition, **INT32 leaderNodeId**, INT64 logStartOffset, INT64 logEndOffset | topics sorted by name, partitions ascending; unknown requested topics are omitted |

In Phase 3 the leader is always this broker's `nodeId`. It exists now because replication (Phase 5) needs it.

## CREATE_TOPIC (apiKey 3, version 0)

Request body:

| Field | Type | Notes |
|-------|------|-------|
| topic | STRING | valid topic name, else INVALID_REQUEST |
| partitionCount | INT32 | 1 <= partitionCount <= broker `maxPartitionsPerTopic` (default 256), else INVALID_REQUEST |

Response body:

| Field | Type | Notes |
|-------|------|-------|
| topic | STRING | |
| partitionCount | INT32 | |

If the topic already exists the response is errorCode TOPIC_ALREADY_EXISTS (no body), even when the requested count differs. Exactly one of several concurrent creators of the same topic succeeds.

## METADATA version 2 (apiKey 2, apiVersion 2) - Phase 5

Request body: identical to v0. Response body:

| Field | Type | Notes |
|-------|------|-------|
| brokerCount | INT32 | |
| brokers | repeated: INT32 nodeId, STRING host, INT32 port | every broker of the cluster |
| controllerId | INT32 | the leader of the META group; -1 if unknown |
| topicCount | INT32 | |
| topics | repeated: STRING name, INT32 partitionCount, then per partition: INT32 partition, INT32 leaderId (-1 unknown), INT32 replicaCount, replicaCount x INT32 nodeId, INT64 logStartOffset, INT64 logEndOffset | topics sorted by name, partitions ascending, replica ids ascending. Offsets are the ANSWERING broker's applied (committed) view: a follower can be slightly behind |

In Phase 5 every partition is replicated on every broker, so `replicas` is always the whole cluster. METADATA v0 and v1 are still answered (v1: the leader is the real Raft leader or -1).

## RAFT_MESSAGE (apiKey 10, version 0) - Phase 5, broker to broker

**One-way: no response frame is ever sent.** `correlationId` is ignored. One outbound TCP connection per peer carries the messages of ALL Raft groups (the META group and every partition). Peers are **trusted** (there is no authentication or encryption in this phase: run the cluster on a private network). A message for a group the receiver does not know yet is dropped silently (the sender's Raft retries). Peer frames may be up to `maxFrameBytes + 1 MiB` (a produce batch rides inside an entry); client frames stay limited to `maxFrameBytes`.

| Field | Type | Notes |
|-------|------|-------|
| groupTopic | STRING | `__meta` for the cluster metadata group, otherwise a topic name |
| groupPartition | INT32 | >= 0 |
| fromNodeId | INT32 | |
| messageType | INT8 | 1 RequestVote, 2 RequestVoteResponse, 3 AppendEntries, 4 AppendEntriesResponse |
| payload | | see below |

| messageType | payload |
|---|---|
| 1 RequestVote | INT64 term, INT32 candidateId, INT64 lastLogIndex, INT64 lastLogTerm |
| 2 RequestVoteResponse | INT64 term, INT8 voteGranted (0/1) |
| 3 AppendEntries | INT64 term, INT32 leaderId, INT64 prevLogIndex, INT64 prevLogTerm, INT64 leaderCommit, INT32 entryCount, entries: (INT64 term, INT8 type 0=NOOP 1=COMMAND, INT32 length, length bytes) |
| 4 AppendEntriesResponse | INT64 term, INT8 success (0/1), INT64 matchIndex, INT64 conflictIndex, INT64 conflictTerm |

Same strict rules as every codec: lengths and counts are validated against the bytes actually remaining before anything is allocated; booleans must be 0 or 1; terms and indexes must not be negative; unknown message or entry types and trailing bytes are errors.

## Consumer group APIs (apiKeys 11-17, version 0) - Phase 6

All use the same frame header; the response is `INT32 correlationId | INT16 errorCode | body` and the body is present only when the error code is 0. `STRING` is `INT16 length | UTF-8 bytes`. Group ids, member ids, client ids: non-empty, at most 255 UTF-8 bytes (a member id may be empty ONLY in JOIN_GROUP); topic names follow the normal topic rules. Every list has at most 10,000 elements; counts are validated against the bytes that remain before anything is allocated; trailing bytes are an error (`INVALID_REQUEST`).

| apiKey | Name | Request body | Response body (error 0) |
|---|---|---|---|
| 11 | FIND_COORDINATOR | `STRING groupId` | `INT32 nodeId \| STRING host \| INT32 port` (the leader of the `__groups` Raft group) |
| 12 | JOIN_GROUP | `STRING groupId \| STRING memberId \| STRING clientId \| INT32 sessionTimeoutMs \| INT32 topicCount \| STRING topic x n` | `INT32 generation \| STRING memberId \| INT32 assignedCount \| (STRING topic, INT32 partition) x n` |
| 13 | HEARTBEAT | `STRING groupId \| STRING memberId \| INT32 generation` | (empty) |
| 14 | LEAVE_GROUP | `STRING groupId \| STRING memberId` | (empty) |
| 15 | OFFSET_COMMIT | `STRING groupId \| STRING memberId \| INT32 generation \| INT32 n \| (STRING topic, INT32 partition, INT64 offset) x n` | (empty; the error code covers the whole request: all or nothing) |
| 16 | OFFSET_FETCH | `STRING groupId \| INT32 n \| (STRING topic, INT32 partition) x n` | `INT32 n \| (STRING topic, INT32 partition, INT64 offset) x n` (offset -1 = none committed) |
| 17 | DESCRIBE_GROUP | `STRING groupId` | `INT32 generation \| INT8 state (0 unknown group, 1 empty, 2 stable) \| INT32 memberCount \| members \| INT32 offsetCount \| (STRING topic, INT32 partition, INT64 offset) x n`; member = `STRING memberId \| STRING clientId \| INT32 sessionTimeoutMs \| INT32 topicCount \| STRING topic x n \| INT32 assignedCount \| (STRING topic, INT32 partition) x n` |

How a client uses them: FIND_COORDINATOR (any broker answers from its Raft state) -> JOIN_GROUP (empty memberId the first time) -> HEARTBEAT every few seconds (`REBALANCE_IN_PROGRESS` means "rejoin") -> OFFSET_COMMIT with the member id and generation it holds -> LEAVE_GROUP. Retriable after FIND_COORDINATOR and a back-off: 14 and 18 (and the Phase 5 retriable set). 15, 16, 17 are group-state errors the consumer handles by rejoining. 19 and 20 are caller bugs.

## State machine commands (inside Raft COMMAND entries)

These are not wire messages between clients and brokers; they are the payloads of Raft entries, interpreted identically by every replica (owned by `ember-replication`). All integers big-endian.

| commandType | Name | Layout |
|---|---|---|
| 1 | PRODUCE_BATCH | INT8 1, INT64 timestamp (assigned by the LEADER, so every replica stores the same one), INT32 recordCount, records: (INT32 keyLength (-1 = null), key, INT32 valueLength, value) |
| 2 | CREATE_TOPIC | INT8 2, STRING name, INT32 partitionCount |

## Error codes

| Code | Name | When |
|------|------|------|
| 0 | NONE | success |
| 1 | OFFSET_OUT_OF_RANGE | fetchOffset < logStartOffset or > logEndOffset |
| 2 | UNKNOWN_TOPIC_OR_PARTITION | FETCH of a missing topic; partition outside 0..partitionCount-1; PRODUCE to a missing topic that cannot be auto-created |
| 3 | INVALID_REQUEST | the body does not parse, invalid topic name, bad counts |
| 4 | UNSUPPORTED_API | unknown apiKey |
| 5 | UNSUPPORTED_VERSION | the API does not support that apiVersion (only METADATA has version 1) |
| 6 | REQUEST_TOO_LARGE | reserved (oversize frames currently close the connection) |
| 7 | INTERNAL_ERROR | the broker hit an unexpected exception |
| 8 | TOPIC_ALREADY_EXISTS | CREATE_TOPIC for a topic that exists |
| 9 | NOT_LEADER_OR_FOLLOWER | PRODUCE/FETCH sent to a broker that is not the Raft leader of that partition. Retriable: refresh metadata, retry at the leader |
| 10 | REQUEST_TIMED_OUT | the record was not committed in time. Retriable but **AMBIGUOUS**: it may still be committed, so a retry can duplicate it |
| 11 | LEADER_NOT_AVAILABLE | the partition has no known leader right now (an election is going on). Retriable after a back-off |
| 12 | NOT_CONTROLLER | CREATE_TOPIC sent to a broker that is not the leader of the META group. Retriable: METADATA v2 `controllerId` says where to go |
| 13 | BROKER_OVERLOADED | too many produce requests are waiting for their commit in this partition. Retriable after a back-off |
| 14 | NOT_COORDINATOR | a group API sent to a broker that is not the leader of the `__groups` Raft group. Retriable: FIND_COORDINATOR again |
| 15 | UNKNOWN_MEMBER_ID | the group does not know this member (never joined, expired, or removed). The member must rejoin |
| 16 | ILLEGAL_GENERATION | the generation in the request is not the group's current generation: the member is fenced (HEARTBEAT: newer than the leader knows; OFFSET_COMMIT: any mismatch) |
| 17 | REBALANCE_IN_PROGRESS | HEARTBEAT with an older generation: the group changed; rejoin to learn the new assignment |
| 18 | COORDINATOR_NOT_AVAILABLE | no `__groups` leader is known right now (an election is going on). Retriable after a back-off |
| 19 | INVALID_SESSION_TIMEOUT | the session timeout is outside the broker's `--group-min-session-ms` .. `--group-max-session-ms` range. A caller bug |
| 20 | PARTITION_NOT_ASSIGNED | OFFSET_COMMIT for a partition that is not assigned to this member. A caller bug |

## Semantics

* **Partitions.** A topic has N independent partitions (N fixed at creation, immutable). Each partition is its own log with its own offsets starting at 0. A request naming a partition outside `0..N-1` gets UNKNOWN_TOPIC_OR_PARTITION.
* **Auto-create.** PRODUCE to an unknown topic creates it with the broker's `defaultPartitions` (default 3) when `autoCreateTopics=true` (the default), but only if the requested partition is `< defaultPartitions`; otherwise UNKNOWN_TOPIC_OR_PARTITION. An invalid topic name gives INVALID_REQUEST. FETCH never creates topics. With auto-create off, PRODUCE to an unknown topic gives UNKNOWN_TOPIC_OR_PARTITION.
* **Partitioning is done by the client.** The broker never looks at keys. The reference client uses `murmur2(key) & 0x7fffffff % partitionCount` (Kafka's algorithm) for keyed records and round-robin for null keys. The partition count is immutable in Phase 3 because changing it would remap keys and break per-key ordering.
* **The broker assigns offsets and timestamps.** The timestamp is the broker's own `Clock.millis()`, one value per request. Clients never send either.
* **Contiguous batches.** All records of one PRODUCE request get contiguous offsets within its partition, even with concurrent producers (one atomic append under the log's write lock).
* **At-least-once only.** If a client retries after an error or a timeout, the first attempt may have succeeded, so records can be duplicated. There is no idempotent producer yet (later phase).
* **FETCH ranges.** `fetchOffset < logStartOffset` or `> logEndOffset` gives OFFSET_OUT_OF_RANGE. `fetchOffset == logEndOffset` succeeds with `recordsLength = 0` (the client polls again; there is no long-poll).
* **Whole records only.** A response holds only whole records and at most `maxBytes` of them, EXCEPT that when the very first record alone is bigger than `maxBytes` it is returned anyway, so a consumer can never get stuck.
* **Never crosses a segment.** A response is cut at the end of the segment file; the client fetches again from the next offset.
* **No CRC check on the broker when serving.** That is what makes zero-copy possible. The client verifies each record's CRC while decoding.
* **Framing errors.** A frame larger than `maxFrameBytes`: the broker closes the connection (it can no longer trust the framing). A well-framed request whose body does not parse: INVALID_REQUEST with the request's correlationId if at least 8 header bytes were readable, otherwise the connection is closed. Unknown apiKey: UNSUPPORTED_API. An apiVersion the API does not support: UNSUPPORTED_VERSION.
* **Ordering.** Requests on one connection are processed and answered in the order they were sent (pipelining is allowed). Between connections there is no ordering.
* **Validation rule.** Codecs never allocate or read based on a length that has not been validated against the bytes actually remaining. Negative counts, lengths past the end, and trailing bytes after a body are all errors.

## Replication semantics (Phase 5)

* **Acknowledged PRODUCE = committed on a majority and applied on the leader.** The entry sits in the Raft logs of a majority of brokers (fsynced unless the broker runs with the documented-unsafe `--unsafe-no-raft-fsync`) and the leader's partition log contains the records. An unacknowledged PRODUCE may or may not exist.
* **FETCH is served only by the partition leader, from committed data only.** `highWatermark` is the end of the leader's partition log, which is the committed end. A leader that has just been deposed and has not noticed yet can serve a stale end of log, but never uncommitted or lost data.
* **After a leader change no acknowledged record is lost or reordered.** Offsets are identical on every replica because every replica applies the same entries in the same order.
* **Retries are at-least-once.** `REQUEST_TIMED_OUT`, a lost connection during PRODUCE, or `NOT_LEADER_OR_FOLLOWER` after the request was proposed leave the outcome unknown; resending can duplicate the record. (The idempotent producer is a later phase.)
* **CREATE_TOPIC** succeeds once committed and applied on the controller; other brokers apply it a moment later, so a client may briefly see UNKNOWN_TOPIC_OR_PARTITION from them and must retry.
* No linearizable reads: a deposed leader's stale end-of-log is the one deliberate weakness.

## Ordering guarantees

* Records are ordered **within a partition**: offsets increase in the order the broker appended them.
* Records with the **same key** are ordered, because the same key always goes to the same partition (as long as the client uses the same partitioner and the partition count never changes).
* There is **no ordering across partitions**, not even for records produced one after another by the same producer.
* Per connection, requests are handled and answered in order.

## Compatibility and evolution

* **Adding a version is safe.** The request header carries `apiVersion`, and a response is encoded in the version of the request. An old client only ever sends version 0, so it keeps getting version-0 layouts, byte for byte. A new client that sends a version an old broker does not know gets UNSUPPORTED_VERSION instead of garbled bytes.
* **Adding an API is safe.** An old broker answers an unknown apiKey with UNSUPPORTED_API.
* **Adding an error code is safe** for clients that treat unknown codes as a generic failure (the Ember reference client rejects unknown codes as a protocol error, so upgrade clients first).
* **Never change an existing layout in place.** New fields mean a new version.
* Phase 3 changes: apiKey 3 (CREATE_TOPIC), METADATA v1, error code 8. PRODUCE v0, FETCH v0 and METADATA v0 are byte-for-byte unchanged; only the rule "partition must be 0" became "partition must exist".

## Annotated example

Generated by running the real codecs (a small program that encodes the frames, labels every field, and fails if the labels do not add up to the actual bytes). Values are hexadecimal, big-endian.

```
PRODUCE request (57 bytes on the wire)
00 00 00 35                          frameLength = 53 (bytes that follow)
00 00                                apiKey = 0 (PRODUCE)
00 00                                apiVersion = 0
00 00 00 07                          correlationId = 7
00 06                                topic length = 6
6f 72 64 65 72 73                    topic = "orders"
00 00 00 00                          partition = 0
00 00 00 02                          recordCount = 2
00 00 00 06                          record 1: keyLength = 6
75 73 65 72 2d 31                    record 1: key = "user-1"
00 00 00 05                          record 1: valueLength = 5
68 65 6c 6c 6f                       record 1: value = "hello"
ff ff ff ff                          record 2: keyLength = -1 (null key)
00 00 00 02                          record 2: valueLength = 2
68 69                                record 2: value = "hi"

PRODUCE response (34 bytes)
00 00 00 1e                          frameLength = 30
00 00 00 07                          correlationId = 7 (echo)
00 00                                errorCode = 0 (NONE)
00 06                                topic length = 6
6f 72 64 65 72 73                    topic = "orders"
00 00 00 00                          partition = 0
00 00 00 00 00 00 00 64              baseOffset = 100 (the two records got 100 and 101)
00 00 00 02                          recordCount = 2

FETCH request (36 bytes)
00 00 00 20                          frameLength = 32
00 01                                apiKey = 1 (FETCH)
00 00                                apiVersion = 0
00 00 00 08                          correlationId = 8
00 06                                topic length = 6
6f 72 64 65 72 73                    topic = "orders"
00 00 00 00                          partition = 0
00 00 00 00 00 00 00 64              fetchOffset = 100
00 01 00 00                          maxBytes = 65536

FETCH response carrying ONE record (81 bytes). The broker writes the header part from a small buffer and streams the record bytes straight from the log file.
00 00 00 4d                          frameLength = 77 (already counts the record bytes)
00 00 00 08                          correlationId = 8
00 00                                errorCode = 0
00 06                                topic length = 6
6f 72 64 65 72 73                    topic = "orders"
00 00 00 00                          partition = 0
00 00 00 00 00 00 00 00              logStartOffset = 0
00 00 00 00 00 00 00 66              highWatermark = 102
00 00 00 27                          recordsLength = 39
--- from here on: byte-for-byte the log file content (one LogRecord) ---
00 00 00 23                          record: length = 35 (bytes after this field)
a6 bc 71 f9                          record: crc32 of everything after this field
00 00 00 00 00 00 00 64              record: offset = 100
00 00 01 8b cf e5 68 00              record: timestamp = 1700000000000
00 00 00 06                          record: keyLength = 6
75 73 65 72 2d 31                    record: key = "user-1"
68 65 6c 6c 6f                       record: value = "hello" (length = whatever remains)
```

Things to notice:

* `frameLength` never counts itself. 53 = 57 - 4.
* The response echoes `correlationId = 7`.
* A null key is `ff ff ff ff` (-1), an empty key would be `00 00 00 00`.
* In the FETCH response the last 39 bytes are exactly what sits in the log file; `frameLength` (77) already includes them, because the broker knows the slice size before it sends the first byte.

### Phase 3 examples (CREATE_TOPIC and METADATA v1)

Generated the same way (real codecs, labels checked against the real bytes).

```
CREATE_TOPIC request (24 bytes)
00 00 00 14                          frameLength = 20
00 03                                apiKey = 3 (CREATE_TOPIC)
00 00                                apiVersion = 0
00 00 00 15                          correlationId = 21
00 06                                topic length = 6
6f 72 64 65 72 73                    topic = "orders"
00 00 00 04                          partitionCount = 4

CREATE_TOPIC response, success (22 bytes)
00 00 00 12                          frameLength = 18
00 00 00 15                          correlationId = 21
00 00                                errorCode = 0 (NONE)
00 06                                topic length = 6
6f 72 64 65 72 73                    topic = "orders"
00 00 00 04                          partitionCount = 4

CREATE_TOPIC response, topic already exists (10 bytes: the frame ends right after errorCode)
00 00 00 06                          frameLength = 6
00 00 00 16                          correlationId = 22
00 08                                errorCode = 8 (TOPIC_ALREADY_EXISTS)

METADATA version 1 response, topic "orders" with 2 partitions (93 bytes)
00 00 00 59                          frameLength = 89
00 00 00 17                          correlationId = 23
00 00                                errorCode = 0
00 00 00 00                          nodeId = 0
00 09                                host length = 9
6c 6f 63 61 6c 68 6f 73 74           host = "localhost"
00 00 23 8b                          port = 9099
00 00 00 01                          topicCount = 1
00 06                                topic length = 6
6f 72 64 65 72 73                    topic = "orders"
00 00 00 02                          partitionCount = 2
00 00 00 00                          partition 0: partition = 0
00 00 00 00                          partition 0: leaderNodeId = 0   <- NEW in v1
00 00 00 00 00 00 00 00              partition 0: logStartOffset = 0
00 00 00 00 00 00 00 08              partition 0: logEndOffset = 8
00 00 00 01                          partition 1: partition = 1
00 00 00 00                          partition 1: leaderNodeId = 0   <- NEW in v1
00 00 00 00 00 00 00 00              partition 1: logStartOffset = 0
00 00 00 00 00 00 00 00              partition 1: logEndOffset = 0
```

A METADATA **version 0** response for the same data is exactly 4 bytes per partition shorter (no `leaderNodeId`), which is how an old client keeps working unchanged.
