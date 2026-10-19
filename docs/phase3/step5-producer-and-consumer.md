# Step 5 - Client: createTopic, metadata v1, EmberProducer, EmberConsumer

## What we are building
* `EmberClient.createTopic`, partition-aware `produce`, and `metadata` that speaks METADATA v1 (the leader of every partition).
* `EmberProducer`: caches topic metadata, partitions every record with the `Partitioner`, and sends one PRODUCE per partition.
* `EmberConsumer`: *manual* assignment ("read partition 2 of `orders` from offset 40"), no groups, positions kept in memory.

## Why it is designed this way
**The producer is where partitioning lives.** The broker only checks that a partition exists. So the producer must know how many partitions a topic has: it asks METADATA once and caches the answer (topic -> partition count + leaders). Because the count is immutable in Phase 3 the cache cannot go stale in a harmful way; we still handle a stale cache (below), because Phase 4+ will need that code.

**Missing topic -> create it.** If metadata does not list the topic and `autoCreateTopics` is on, the producer sends CREATE_TOPIC with `defaultPartitions` (3) and treats `TOPIC_ALREADY_EXISTS` as success - somebody else created it between our two calls, which is fine. This is the "get or create" pattern; it is safe *because* creation is race-safe on the broker (Step 3).

**`sendBatch` returns results in input order.** The caller gives records [a, b, c, d]; keys may send a and c to partition 1 and b and d to partition 0. We group by partition *keeping input order inside each partition* (so per-key order is preserved), send one request per partition, and then use `baseOffset + i` to map each answer back to its original record. The partitions are sent one after another; parallel sends are future work (the "one request in flight per connection" design).

**Retry rule: refresh once, retry once.** `UNKNOWN_TOPIC_OR_PARTITION` can mean our metadata is out of date. We refresh the metadata once and retry that request once, then give up with `EmberException`. Nothing else is retried: a retry after a timeout may duplicate a record that did arrive (the response was lost), so Ember's producer is **at-least-once** until the idempotent producer of a later phase.

**Consumer: explicit and boring on purpose.** `assign`, `seek`, `position`, `poll`. A poll fetches each assigned partition exactly once, in rotating order (so partition 0 is not always served first), and moves each position just past the records it returned. Offsets are held in memory only - remembering them across restarts ("committed offsets") is Phase 6. `OFFSET_OUT_OF_RANGE` is *thrown* rather than silently reset: jumping to the beginning or end hides data loss or duplicates, and the caller should decide.

**Injectable round-robin counter.** Tests pass a counter so null-key placement is deterministic.

## Alternatives I rejected and why
* Server-side partitioning: the broker would have to parse keys, and the producer could not batch per partition.
* Re-partitioning after a metadata refresh in the retry: with an immutable count it would produce the same answer; add it when counts can change.
* Sending partitions in parallel: needs multiple connections or pipelining; later phase.
* A consumer that auto-resets offsets: silently hides problems.

## Tradeoffs and honest limits
* One connection, one request in flight: a batch spread over 8 partitions is 8 round trips.
* Retries are at-least-once (duplicates possible).
* Clients must all use the same partitioner or per-key ordering across clients breaks.
* The producer cache never expires on its own; a topic that is later replaced would need `refresh`.
* Consumer positions are lost when the consumer object is gone.

## How it is tested
Against a real broker: same key always in the same partition across 1,000 sends; offsets within a partition increase by one; 50 keys x 200 sequential sends are read back in exact per-key order with every record exactly once; `sendBatch` metadata matches input order and data; null keys spread round-robin; 8 producer threads x several partitions lose or duplicate nothing; a producer with an empty cache creates the topic; a producer given *wrong* metadata (via a client subclass) refreshes exactly once and then fails; consumer `assignAll` from beginning and from latest, `seek`, and OFFSET_OUT_OF_RANGE; restart, 64 partitions, retention, and a parallelism correctness test with fsync-per-message.
