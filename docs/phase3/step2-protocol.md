# Step 2 - Protocol: CREATE_TOPIC, METADATA v1, error code 8

## What we are building
* A new API, `CREATE_TOPIC` (apiKey 3, version 0): "make topic X with N partitions".
* A new **version** of an existing API: `METADATA` version 1, which adds the leader node of each partition.
* A new error code, `8 TOPIC_ALREADY_EXISTS`.
* Version rules: METADATA accepts versions 0 and 1; every other API only version 0. A response is encoded in the version the request used.

## Why it is designed this way
**Evolving a protocol without breaking anyone.** Think of a REST API: you add `/v2/users` and keep `/v1/users` alive for old apps. Ember's request header already contains an `apiVersion`, so a new version is just "same API key, bigger number". The broker looks at the number: version 0 -> answer in the old layout, version 1 -> answer with the extra field. An old client never sends 1, so it never sees the new field. A new client talking to an old broker gets `UNSUPPORTED_VERSION` (clear failure instead of garbled bytes). That is why adding a version is safe.

**Why metadata gets the leader now.** In Phase 3 the leader is always this broker, so the field looks useless. But Phase 5 (replication) needs clients to know which broker to talk to per partition. Adding it now, as a *new version*, is also the live demonstration of how the protocol evolves.

**In memory there is one `MetadataResponse` type.** Its partitions carry a `leaderNodeId`. The v0 codec simply does not write it, and when it reads v0 it sets `leaderNodeId = -1` ("unknown"). One type in the code, two layouts on the wire.

**CREATE_TOPIC as an explicit API.** Auto-creation on first produce is convenient but you cannot say *how many partitions* you want. An explicit call fixes that, and `TOPIC_ALREADY_EXISTS` lets callers distinguish "I created it" from "somebody did first" (like HTTP 409 Conflict).

**Validation stays in the codec for things that never change** (topic name rule, `partitionCount >= 1`) and in the broker for things that are configuration (`maxPartitionsPerTopic`). The codec cannot know a broker's setting.

## Alternatives I rejected and why
* A new apiKey for "metadata with leaders": the version field exists precisely for this.
* Changing METADATA v0 in place: would break every Phase 2 client.
* Negotiating versions (ApiVersions request, as Kafka does): more machinery than we need with two versions.

## Tradeoffs and honest limits
* No version negotiation: a client must know what the broker supports.
* A v0 response loses information (no leader). That is the point of v0.
* `partitionCount` is capped only by broker config, not by the codec.

## How it is tested
Round trips of CREATE_TOPIC and METADATA v0/v1 (request and response); v0 and v1 bytes decode correctly and differ by exactly 4 bytes per partition; every malformed case (negative/huge counts, lengths past the end, trailing bytes, partitionCount 0 or negative, bad topic name); error code 8; unsupported versions for non-METADATA APIs and for METADATA version 2.
