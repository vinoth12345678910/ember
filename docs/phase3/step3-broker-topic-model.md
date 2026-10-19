# Step 3 - The broker's topic model (topic -> partitions -> logs)

## What we are building
`LogManager` stops owning "one Log per topic" and owns **topic -> N partitions -> one Log each**. It also:
* persists each topic's partition count in a small file,
* rebuilds everything at startup (including Phase 2's old single-partition layout and half-created topics),
* creates topics race-safely,
* runs flush/retention maintenance on every partition,
* and `BrokerApis` learns CREATE_TOPIC, partition validation, and METADATA v1.

## Why it is designed this way
**Directory per partition: `<dataDir>/<topic>-<partition>/`.** Phase 2 already used `<topic>-0`. Keeping the scheme means Phase 2 data keeps working: an old directory `orders-0` is simply partition 0 of topic `orders`. Topic names may contain `-` ("my-topic-v2"), so we always split the directory name on its LAST dash and require digits after it.

**Why a metadata file at all?** You cannot reliably recover the partition count from directories: a partition that was never written might not have a directory yet (or a crash might have left only some of them). If the count were guessed from the directories, a crash during creation could silently turn a 4-partition topic into a 2-partition one - and every key would hash to a different place. So the count lives in `_meta/<topic>.properties`.

**Atomic write.** We write `topic.properties.tmp`, fsync it, then `ATOMIC_MOVE` it over the real name (and fsync the directory). A reader (or a crash) sees either no file or the complete file, never half of one - like writing to a temp file and `mv`-ing in a shell script.

**Order of creation: file first, directories second.** If we crash after the file but before all directories exist, startup sees "metadata says 4, directories 0 and 1 exist" and simply creates 2 and 3. The reverse order (directories first) would leave a topic without a recorded count, which is exactly the guess we want to avoid.

**Legacy topics.** A directory group with *no* metadata file is Phase 2 data. Its count = highest index + 1, but only if the indexes are contiguous (0,1,2...). A gap (0 and 2 but no 1) means something is missing; guessing would hide data loss, so the broker refuses to start and names the topic. Once the count is derived the metadata file is written, so later startups use the same rule as everyone else.

**Race-safe creation.** Creation is rare, so one lock around "check exists, write file, open logs, publish" is the simplest correct design: two clients creating the same topic -> one wins, the other gets TOPIC_ALREADY_EXISTS; an auto-create racing an explicit create finds the other's topic and just uses it. Reads (`topic(name)`, `logFor`) use a `ConcurrentHashMap` and never take that lock, so produce/fetch are not slowed by it. A topic is published into the map only when fully built.

**One Log, one lock, per partition.** `Log` has its own read/write lock (Phase 1/2). So an fsync on partition 0 holds only partition 0's lock; partition 1 keeps appending. That is where partition parallelism comes from.

**Auto-create + partitions.** PRODUCE to a missing topic creates it with `defaultPartitions` (3), but only if the requested partition is `< defaultPartitions`. Otherwise "partition 7 of a topic that would only get 3" is an error, not a reason to create a bigger topic.

**Metadata v1 leader.** `BrokerApis` fills `leaderNodeId` with its own `nodeId` for v1 and `-1` for v0 (which cannot carry it).

## Alternatives I rejected and why
* One global lock around every produce: kills the parallelism that partitions exist for.
* Deriving the count from directories only: the crash scenario above.
* A database or JSON file for all topics: one file per topic means creating a topic never rewrites other topics' state.
* Creating directories first, then the metadata file: see "order of creation".

## Tradeoffs and honest limits
* The partition count is immutable (see Step 1). Topics cannot be deleted.
* `_meta` and `*.tmp` leftovers are handled, but a hand-edited metadata file with garbage makes the broker refuse to start (we prefer that to guessing).
* On a case-insensitive file system topics `Orders` and `orders` would collide (unchanged from Phase 2).
* Every partition keeps its segment files open; 256 partitions x several segments can need a bigger `ulimit -n`.
* If opening the logs fails during an in-process create (disk error) we delete the metadata file again so the failed create leaves no trace; a *crash* keeps it and startup finishes the job.

## How it is tested
No sockets: create/duplicate/concurrent create from 16 threads; auto-create with the default count; invalid partitions for produce and fetch; metadata v0 and v1 contents; restart keeps counts and data; legacy `orders-0` becomes a 1-partition topic and gets a metadata file; non-contiguous legacy directories refuse to start; a simulated crash during creation (file says 4, two directories) completes at startup; dashed topic names round-trip; stray directories are ignored; maintenance (flush timer, retention) with the fake clock hits every partition of every topic.
