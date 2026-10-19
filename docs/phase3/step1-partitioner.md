# Step 1 - The partitioner (murmur2 + round-robin)

## What we are building
`com.ember.protocol.Partitioner`: given a record key and the number of partitions of a topic, it answers "which partition does this record go to?".
* key present: `(murmur2(key) & 0x7fffffff) % partitionCount`
* key null: round-robin 0, 1, 2, ... n-1, 0, ... using an injectable counter.

## Why it is designed this way
**Partitions are checkout lanes.** A supermarket with one till has one queue; with 4 tills, 4 queues work in parallel. A topic with N partitions is N independent logs, each with its own lock and fsync, so produces to different partitions never wait for each other.

**The key is "which lane does this customer always use".** Records about the same thing (a user, an order) must stay in order. Order only exists *inside* one partition, so the same key must always map to the same partition. A hash does that: same bytes -> same number -> same lane. `% partitionCount` squeezes the hash into the valid range.

**Why `& 0x7fffffff`.** Java ints can be negative and `-7 % 3` is `-1` in Java: a negative lane number. Clearing the sign bit first guarantees `0 <= result < n`. (We do not use `Math.abs`, because `Math.abs(Integer.MIN_VALUE)` is still negative.)

**Why murmur2 exactly like Kafka.** The hash must be *stable forever and identical in every client*: a Java client and a Python client must put key "user-7" into the same partition, otherwise per-key ordering silently breaks. Using Kafka's exact algorithm (seed 0x9747b28c, 4-byte little-endian blocks, tail fall-through, final mixing) gives us public test vectors to prove ours is right. `String.hashCode()` would also be stable but it is a weak mixer and tied to Java.

**Why partitioning is on the client.** The broker stays simple (it only checks that the partition exists) and a producer can batch records per partition before sending. Kafka does the same.

**Round-robin for null keys** spreads load evenly when no ordering is needed. The counter is injected (an `IntSupplier`) so tests are deterministic.

## Alternatives I rejected and why
* `key.hashCode()`: `byte[].hashCode()` is identity-based, i.e. different for equal arrays. Useless.
* `Arrays.hashCode`: stable, but weaker distribution; we want the Kafka-compatible one with real test vectors.
* Consistent hashing: only worth it when the partition count changes; Phase 3 keeps it immutable.
* Random partition for null keys: harder to test; round-robin is deterministic.

## Tradeoffs and honest limits
* Changing the partition count would remap keys (`hash % 3` != `hash % 4`) and break per-key ordering. That is why the count is immutable in Phase 3.
* All clients must use this same algorithm; the broker cannot enforce it.
* Skewed keys (one very popular key) make one partition hot; no hash fixes that.

## How it is tested
Kafka's own murmur2 vectors; same key -> same partition; range `[0, n)` including keys whose hash is negative; null keys give exactly 0,1,...,n-1,0,...; a distribution test with 20,000 random keys over 8 partitions (fixed seed), each partition within +-15% of its fair share.
