# Step 1 - Protocol for consumer groups

## What we are building
Seven new APIs (all version 0) and seven new error codes:

| apiKey | API | Purpose |
|---|---|---|
| 11 | FIND_COORDINATOR | which broker coordinates groups (the leader of the `__groups` Raft group) |
| 12 | JOIN_GROUP | become a member; get the generation and your partitions |
| 13 | HEARTBEAT | "I am alive" + "has the seating chart changed?" |
| 14 | LEAVE_GROUP | leave politely |
| 15 | OFFSET_COMMIT | save "I have processed up to here" |
| 16 | OFFSET_FETCH | read the saved positions |
| 17 | DESCRIBE_GROUP | members, assignment, committed offsets |

Error codes 14 NOT_COORDINATOR, 15 UNKNOWN_MEMBER_ID, 16 ILLEGAL_GENERATION, 17 REBALANCE_IN_PROGRESS, 18 COORDINATOR_NOT_AVAILABLE, 19 INVALID_SESSION_TIMEOUT, 20 PARTITION_NOT_ASSIGNED.

## Why it is designed this way
Same frame header and the same "error code first, body only on success" rule as every earlier API, so the client and the broker's request pipeline need no new concepts. Think of a group of waiters sharing tables: the **generation** is the version number of the seating chart. Every message that depends on the chart (heartbeat, commit) carries the number it believes, so a waiter reading an old chart can be recognised and refused.

Strict codecs, as before: every length checked before reading, trailing bytes are an error, every list bounded (at most 10,000 elements) so a malicious count cannot allocate memory, ids non-empty and at most 255 UTF-8 bytes (a member id may be empty ONLY in JOIN_GROUP, meaning "I am new").

## Alternatives I rejected and why
* Reusing one generic "group request" with a sub-type byte: fewer classes but weaker typing and worse error messages.
* Putting the coordinator's address into METADATA: a coordinator can change at any moment and METADATA is cached; a dedicated, cheap FIND_COORDINATOR can be repeated whenever a request fails.
* Error codes for "wrong generation" reusing existing ones: the client's reaction differs (rejoin vs retry vs bug), so each gets its own code.

## What goes wrong if you get it wrong
A decoder that trusts a count allocates gigabytes for 8 bytes of input. A decoder that ignores trailing bytes hides a client that is speaking a different layout. Mixing "retry" errors with "rejoin" errors makes consumers either spin or give up.

## Tradeoffs and honest limits
No batching of groups in one request and no per-partition error codes in OFFSET_COMMIT (the whole request succeeds or fails: all-or-nothing, which matches how the state machine applies it). The topic-name rules are the existing, stricter ones.

## How it is tested
`Phase6CodecTest`: round trips for all seven requests and responses, truncated input at every length, negative and oversize counts, trailing bytes, empty and over-long ids, invalid UTF-8, unknown error codes, and the version rule (only version 0 exists).
