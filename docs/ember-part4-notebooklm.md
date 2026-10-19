# Ember Part 4 - Partitions (Phase 3)

## One-minute pitch

Until now an Ember topic was one long log, and everything about it was serialized: one lock, one fsync queue, one file to append to. Phase 3 splits a topic into **partitions**, the way a supermarket opens several checkout lanes. Each partition is a complete, independent log with its own lock, its own fsync and its own offsets.

Which lane does a record take? The producer decides: if the record has a key, a hash of the key picks the lane, so the same customer (key) always uses the same lane and their records stay in order. If there is no key, records are dealt out round-robin. The broker only checks that the lane exists.

We built the partitioner (Kafka's own murmur2 hash), a new CREATE_TOPIC request, a new version of the METADATA request that tells clients who leads each partition, a topic registry that remembers partition counts safely across crashes, a lock that stops two brokers from sharing one data folder, and a smarter client: a producer that routes and batches, and a consumer that reads any partition. The CLI got `create-topic`, `consume`, a metadata table and a multi-producer `bench` that checks its own results.

The honest surprise: with fsync after every message, 4 partitions were only about 20% faster than 1, not 4x faster, because the SSD itself does not run parallel fsyncs well. With the default once-a-second fsync, 4 partitions with 4 producers produced about 345,000 records per second versus about 240,000 for one. 259 tests pass.

---

## Step 0 - Preflight

**Why.** Never build on a broken base. Before writing a line we read the Phase 2 code and docs and ran the whole build: it was green with 166 tests across four modules. Only then did we start.

## Step 1 - The partitioner

**Why partitions at all.** One checkout lane means every customer waits behind every other. Several lanes let customers be served in parallel. For Ember that means different partitions never block each other.

**Why the key picks the lane.** Order only exists *inside* one lane. If records about the same user could land in different lanes, "created" might be read after "deleted". A hash turns the key into a number, and the same key always gives the same number, so the same lane.

**Why Kafka's exact algorithm.** Every client, in any language, must hash identically, otherwise the same key goes to different lanes depending on who sent it. Using murmur2 exactly like Kafka also gave us public test vectors: six known keys with known hash values. They all matched on the first try.

**A small trap avoided.** Java's `%` can return a negative number for a negative hash, which would be lane minus three. We clear the sign bit first. (We do not use `Math.abs`, because it fails for exactly one number.)

**Round-robin for keyless records** deals records out like cards: lane 0, 1, 2, 3, 0, 1... The counter can be injected so tests are predictable.

## Step 2 - The protocol grows without breaking

**Why a new version instead of changing the old message.** Think of a REST API: you add `/v2` and keep `/v1` alive. Every Ember request already carries a version number. Metadata version 1 adds one field per partition, the leader. A version-0 client never asks for version 1, so it receives the exact bytes it always did. A client asking an old broker for a version it does not know gets a clear UNSUPPORTED_VERSION instead of garbled data.

**Why the leader now, when there is only one broker?** Replication (Phase 5) will need it. Adding it now, as a new version, is also the live demonstration of how protocol evolution works.

**CREATE_TOPIC** lets you say how many partitions you want. A new error code, TOPIC_ALREADY_EXISTS, works like HTTP 409 Conflict: "someone got there first", which callers can treat as success.

## Step 3 - The broker's topic model

**Why a metadata file for the partition count.** You cannot reliably guess the count from folders: a crash in the middle of creating a 4-partition topic could leave only two folders, and guessing "2" would silently re-route every key. So the count is stored in a tiny file, written *atomically*: write a temporary file, force it to disk, then rename it over the real one. A reader sees the old file or the new file, never half of one.

**Why the file is written before the folders.** Then a crash leaves "file says 4, two folders exist", and on the next start the broker simply creates the missing two. The other order could leave folders without a recorded count.

**Old Phase 2 data keeps working.** An old folder `orders-0` is just partition 0 of a one-partition topic. A group of old folders with no metadata file gets its count from the highest number, but only if none is missing. A gap means lost data, so the broker refuses to start and names the topic instead of guessing.

**Dashes in names.** Topic names may contain dashes (`my-topic-v2`), so a folder name is always split at its *last* dash, and the part after it must be digits.

**Creating a topic safely.** Many clients may create the same topic at the same moment. One lock around "check, write file, open logs, publish" guarantees exactly one winner; everyone else gets TOPIC_ALREADY_EXISTS, and an automatic creation racing an explicit one never makes two. The topic only becomes visible once it is complete, and reading never needs that lock.

## Step 4 - Wiring and the data-directory lock

**Why a lock on the data folder.** Two brokers appending to the same files would each believe they own the end of the file and silently overwrite each other. An operating-system file lock turns that quiet corruption into an immediate, loud refusal to start. If the process dies the OS releases the lock, so a crash never leaves a stale lock.

**A subtle trap.** On Unix-like systems, closing *any* handle to a file can release a process's lock on it. So when a second broker in the same program tries to start, we must not even open the lock file; we check an in-memory set first. We tested the same-program case, and in the final check we also started a second real broker process on the same folder: it printed "another Ember broker is using this data directory" and exited with code 1.

## Step 5 - A smarter client

**The producer** remembers how many partitions each topic has (asking the broker once), hashes each record's key, groups the records by partition, sends one request per partition, and returns the results in the original order. If a topic is missing it creates it. If the broker says "unknown partition" it refreshes its knowledge once and retries once, then gives up. Nothing else is retried, because a retry after a lost reply can store a record twice. That is called **at-least-once** delivery.

**The consumer** is deliberately manual: you say "read partition 2 of orders from offset 40". Each poll fetches every assigned partition once, starting from a different partition each time so none is always first. Positions live only in memory; remembering them across restarts is Phase 6. If your offset is out of range it throws instead of quietly jumping, because silently skipping data hides problems.

**The ordering test.** 50 keys each sent 200 times, interleaved, over 4 partitions. Reading everything back, every key's values came back in exactly the order sent, and every record appeared exactly once. There is no ordering across partitions, and that is by design.

## Step 6 - The command line

`create-topic`, `produce` (prints the partition and offset it landed in), `fetch --partition`, `consume` (new records by default, `--from-beginning` for all, `--follow` to keep polling), `metadata` as a table, and `bench`. The bench runs several producer threads, each with its own connection, then reads everything back with one consumer and **verifies** the count and that each partition's offsets are gapless. A benchmark that does not check itself can report a very fast broken system.

## Step 7 - Hardening

We ran the complete test suite six times in a row: all six runs were green (258 tests each). Six green runs only catch frequent flakes, since a one-in-twenty flake survives six runs about three times in four, so we also reviewed the locks and the closing of resources by hand. That review found two real problems that no test had caught: a consumer poll that could silently lose records if a later partition's fetch failed, and a command-line flag that, written without a value, turned into the value "true". Both are fixed with tests.

## Step 8 - What partitions really buy (measured)

Machine: Apple M4, 10 cores, 16 GB, SSD, JDK 21, broker and client on the same laptop. One producer thread per partition, 100-byte values, batches of 500.

With fsync after every message (20,000 records):

| partitions x producers | records/s |
|---|---|
| 1 x 1 | 241 |
| 4 x 4 | 292 |
| 8 x 8 | 287 |

With the default flush, once a second (200,000 records, three runs each):

| partitions x producers | records/s |
|---|---|
| 1 x 1 | 236,383 to 250,564 |
| 4 x 4 | 334,442 to 359,091 |
| 8 x 8 | 290,319 to 329,143 |

Reading everything back ran at about 950,000 to 980,000 records per second whatever the partition count (one consumer, one connection).

**Reading the tables.** Partitions did their job on the lock side, which a test checks, but with fsync after every message the disk became the limit. A separate 30-line experiment showed this SSD gives 271 fsyncs per second to one thread, 341 to four threads and 567 to eight: far from linear. So four lanes did not mean four times the speed. With fsync once a second the work is mostly CPU, and four partitions were about 40 to 50 percent faster than one; eight were no better than four on a 10-core laptop that was also running the client and the broker. Two design choices also limit the effect: every bench producer uses random keys, so all producers meet at every partition, and a producer has one connection with one request in flight. The lesson: parallelism helps only up to the slowest shared resource.

## Step 9 - Final verification with the real jars

`mvn clean verify` was green. Then, with the real packaged jars: a second broker process on the same folder refused to start; a topic with 4 partitions was created (a duplicate gave TOPIC_ALREADY_EXISTS); 20 keyed records were produced and all 20 consumed back; the metadata table showed the end offsets; the broker stopped on SIGTERM (exit code 143, "shut down cleanly"), restarted on the same folder and returned the same 20 records with the same end offsets; re-creating the topic with 8 partitions still said it exists; and a real Ctrl+C (SIGINT) stopped it with exit code 130 and the same message.

**Final test counts:** ember-storage 55, ember-protocol 64, ember-client 16, ember-broker 124. **Total 259** (93 more than Phase 2's 166). Slowest classes: ChaosTest 35.2 s (the old Phase 1 crash simulation), PartitionedProducerConsumerTest 3.8 s, PartitionedBrokerLifecycleTest 2.9 s, ClientIntegrationTest 2.2 s, CliTest 2.2 s.

---

## Bugs and surprises we hit

1. **Adding a request type broke the build in a place that was not "protocol".** The broker handler uses a `switch` over a sealed set of request types, which must cover every case. Adding CREATE_TOPIC to the protocol made the broker stop compiling until we added a temporary branch (replaced in Step 4) so the whole suite could run at the end of Step 2.
2. **Phase 2 tests assumed one partition.** Auto-created topics now have three partitions, so tests that expected "partition 1 does not exist" failed. We set the test fixtures to one partition by default. A test that used a folder called `t-1` as a "stray directory" became a legitimate partition folder, and the old registry test file was rewritten for the new model. All of these are listed in the final report.
3. **The consumer could lose records.** Found by reading the code, not by a failing test: positions advanced per partition, so a failure on the third partition threw away records already fetched from the first two. Positions now move only after every fetch in the poll succeeded.
4. **A bare `--topic` flag became the topic named "true".** Found while reviewing the flag parser for `--follow`. Now rejected with a clear message.
5. **Partitions did not give 4x under fsync.** The benchmark disagreed with our expectation, so we measured the disk directly instead of guessing. See Step 8.
6. **Our own documents had mistakes until checked.** A first draft of the README example contained output we had not actually produced, and a first draft of the hardening note had wrong arithmetic about how often six runs catch a flake. Both were replaced: the README now shows real output from the jars, and the note states the true probability.
7. **A "hang" that was not a hang.** Stopping a background broker with SIGINT from a script does nothing, because non-interactive shells start background jobs with SIGINT ignored. The Ctrl+C check was done from a process that restores the default handler.

---

## Glossary

- **Partition**: one independent, ordered log inside a topic. Its offsets start at 0.
- **Topic**: a named stream of records, split into partitions.
- **Key**: optional bytes attached to a record; the same key always goes to the same partition.
- **Partitioner**: the function that maps a key (or a counter, for keyless records) to a partition number.
- **murmur2**: a fast hash function; Ember uses Kafka's exact version.
- **Round-robin**: dealing records to partitions in turn: 0, 1, 2, 3, 0, ...
- **Leader**: the broker that serves a partition. Always the single broker for now.
- **Metadata**: the broker's description of topics, partitions, offsets and leaders.
- **API version**: a number in each request that selects the message layout, so the protocol can grow without breaking old clients.
- **Atomic rename**: replacing a file in one indivisible step, so readers never see half a file.
- **Data-directory lock**: an operating-system lock that stops two brokers sharing one folder.
- **At-least-once**: a retried record may be stored twice; nothing is lost.
- **Manual assignment**: the consumer is told which partitions to read; no groups yet.
- **fsync**: forcing written data onto the physical disk.
- **Shaded jar**: one jar containing a program and all its libraries.

## Suggested NotebookLM prompts

**Video overview:** "Make a video overview of Ember Phase 3 for a student who knows Spring Boot and Express but is new to partitioning. Use the supermarket checkout-lane analogy throughout, and for each step explain the WHY before the WHAT. Spend extra time on (1) why the same key must always go to the same partition and why there is no ordering across partitions, (2) why the partition count is stored in an atomically written file before the folders are created, and (3) the benchmark surprise: why 4 partitions were only about 20 percent faster with fsync after every message, and what the disk experiment showed. End with the real bugs we found by reviewing the code."

**Quiz:** "Write a 12-question quiz on Ember Phase 3: 4 multiple choice on partitioning (key hashing, round-robin, why the count is immutable, ordering guarantees), 3 on protocol evolution (API versions, why METADATA v1 is safe for old clients, CREATE_TOPIC and error 8), 3 on broker storage (metadata file written first and atomically, legacy folders, the data-directory lock), and 2 on the benchmarks (what the fsync and default-flush tables show and what limits parallelism). Give the answers with a one-sentence explanation each."
