# Step 6 - The CLI learns partitions

## What we are building
New and changed `EmberCli` commands:
* `create-topic --topic=t --partitions=N`
* `produce --topic=t --key=k --value=v` (now through `EmberProducer`; prints `partition=P offset=O`)
* `fetch --topic=t --partition=P --offset=0 --max-bytes=65536`
* `consume --topic=t [--from-beginning] [--follow]`
* `metadata [--topic=t]` (a table: topic, partition, leader, start offset, end offset)
* `bench --topic=t --partitions=N --producers=P --records=... --value-bytes=... --batch=...`

## Why it is designed this way
**The CLI is a thin skin over the library.** `produce` and `consume` use exactly the `EmberProducer`/`EmberConsumer` classes users would call from Java, so anything you see in the terminal is what the real client does (and the CLI tests exercise the library too).

**`consume` mirrors Kafka's console consumer.** By default it reads *new* records (position = end of each partition); `--from-beginning` starts at the oldest offset. Without `--follow` it stops as soon as one poll returns nothing, i.e. every partition was caught up; with `--follow` it polls every 200 ms until you press Ctrl+C. The CLI is the one place allowed to sleep: it is a human tool, not a test. A hook (`BooleanSupplier stop`) lets tests end the follow loop without sleeping on a clock.

**`bench` measures what partitions are for.** P threads, each with its *own* `EmberProducer` (own connection - the producer is deliberately single-connection), send random-keyed records; random keys spread over all partitions. Then ONE consumer reads back exactly what this run wrote (it remembers each partition's end offset before starting) and the command verifies:
1. the number of records read equals the number produced,
2. each partition's offsets are contiguous (no gaps or repeats).
A benchmark that does not check its own correctness can report a very fast broken system.

**Throughput numbers count whole encoded records** (28-byte header + key + value) in both directions.

## Alternatives I rejected and why
* Printing the partition table with a library: plain `String.format` is enough.
* One shared producer for all bench threads: it is `synchronized`, so threads would queue on it and hide the parallelism we want to see.
* Verifying only the count: gaps and duplicates can cancel out in a count.

## Tradeoffs and honest limits
* Each bench thread sends its partitions one after another (one request in flight per connection), so more partitions than producers does not by itself add concurrency on the client side.
* `consume --follow` has no Ctrl+C handler: the JVM is simply stopped (sockets close with the process).
* Random keys make a balanced load; real workloads can be skewed.

## How it is tested
CLI tests run the commands in-process against a real broker: create-topic (and duplicate error), keyed produce printing partition and offset, fetch from a chosen partition, consume with/without `--from-beginning`, `--follow` with an injected stop, the metadata table, and a small multi-producer bench including its verification line. Error cases give exit codes 1/2.
