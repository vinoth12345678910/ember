# Step 2 - Protocol: error codes 9-13, METADATA v2, RAFT_MESSAGE

## What we are building
* Five new error codes: `9 NOT_LEADER_OR_FOLLOWER`, `10 REQUEST_TIMED_OUT`, `11 LEADER_NOT_AVAILABLE`, `12 NOT_CONTROLLER`, `13 BROKER_OVERLOADED`.
* **METADATA version 2**: the cluster-aware answer - all brokers, who the controller is, and for every partition its leader, its replicas and the offsets as seen by the answering broker.
* A new one-way API **RAFT_MESSAGE** (apiKey 10): how brokers send each other the four Raft messages of the pure Raft core. The codec lives in `ember-protocol` (which now depends on `ember-raft` for the message types).

## Why it is designed this way
**Versions again.** METADATA v0/v1 answered "about this one broker". A cluster client needs a different shape (a list of brokers, a controller, leaders and replicas), so it is a new version, v2, with the old versions untouched: an old client keeps working against a new broker, byte for byte.

**New error codes tell the client what to *do*.** `NOT_LEADER_OR_FOLLOWER`, `LEADER_NOT_AVAILABLE`, `NOT_CONTROLLER`, `BROKER_OVERLOADED` all mean "ask again, maybe elsewhere / later" (retriable): refresh metadata, back off, retry. `REQUEST_TIMED_OUT` is retriable too but **ambiguous**: the record might have been committed after all, so a retry can duplicate it. Splitting these from `INTERNAL_ERROR` is what lets a client be smart instead of giving up.

**RAFT_MESSAGE is one-way with no response frame.** Raft already has its own request/response pairs (`RequestVote` / `RequestVoteResponse`); both directions are just messages. Making the transport fire-and-forget matches Raft's assumptions (messages may be lost, duplicated, reordered) and means a slow peer can never block the sender waiting for a reply. Each message carries the **group** it belongs to (`groupTopic`, `groupPartition`) so one TCP connection per peer carries the traffic of all groups (the META group and every partition).

**Same strict codec rules.** Never allocate or read based on a length you have not checked against the bytes actually remaining; counts are validated against minimum element size; trailing bytes are an error; booleans must be 0 or 1; message types and entry types must be known. A Raft peer is *trusted* (no authentication in this phase), but a bug or a corrupted frame must still not be able to crash the receiver with an `OutOfMemoryError`.

## Alternatives I rejected and why
* Request/response for Raft messages (like Kafka's older controller RPCs): doubles frames, couples senders to slow receivers.
* A separate port and protocol for broker-to-broker traffic: more configuration; the same Netty pipeline and codec rules work for both.
* Putting leader/replica info in METADATA v1: would break old clients; versions exist for this.

## What goes wrong if you get it wrong
* Missing `groupTopic/groupPartition`: messages for different partitions would be mixed up and could be delivered to the wrong Raft node - a safety disaster.
* An unvalidated `entryCount` in AppendEntries: a peer (or a bit flip) sends 2 billion entries and the receiver dies allocating the list.
* Treating `REQUEST_TIMED_OUT` as "failed": the client retries and silently duplicates a record that was actually committed.

## Tradeoffs and honest limits
No authentication or encryption between peers (documented trust model: a private network). Raft frames may be larger than client frames (a produce batch rides inside an entry); the broker's frame decoder allows up to `maxFrameBytes + 1 MiB` for RAFT_MESSAGE only.

## How it is tested
Round trip of every Raft message type (AppendEntries with NOOP and COMMAND entries, empty heartbeat, failure hints) and of METADATA v2; malformed cases (unknown message type, huge entry count, lengths past the end, trailing bytes, invalid boolean, negative term/index, bad group topic); METADATA v0/v1 still decode; new error codes; version rules (RAFT_MESSAGE only version 0; METADATA 0-2).
