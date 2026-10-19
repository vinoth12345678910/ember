# Step 9 - Bench command, real numbers, and the documentation

## What we are building
* `bench` in the CLI: produce N records in batches, then fetch them all back, and print records/s and MB/s per direction.
* Real runs of it against three broker configurations (see `docs/ember-part3-notebooklm.md` for the numbers).
* `docs/PROTOCOL.md` hex dump (generated from real code), `README.md`, and the NotebookLM document.

## Why it is designed this way
**Measure, don't guess.** In Phase 1 the log alone showed that `fsync` after every message is ~1000x slower than never. The bench is the same experiment through the whole stack (TCP, Netty, locks), so we see what a *client* sees.

**MB/s counts whole encoded records** (28-byte header + key + value) in both directions, so produce and fetch are comparable. Fetch time includes the client decoding every record and checking its CRC.

**The three configurations**
1. default: `--flush-ms=1000`, fsync at most once a second.
2. `--flush-messages=1`: fsync after every message (the safest setting).
3. `--flush-ms=never`: only fsync on segment roll and clean shutdown (relies on the OS; in a real cluster, on replication).

**Honest benchmark caveats.** One client, one connection, one machine; client and broker compete for the same CPU; the data is still in the OS page cache when fetched, so the fetch figure is "RAM speed", not "disk speed".

**Hex dump from real code.** A hand-written dump can drift from reality. Ours is printed by a program that also asserts the labels reproduce the real encoded bytes.

## Alternatives I rejected and why
* JMH micro-benchmarks: more precise, but the question here is end-to-end behaviour.
* Multiple concurrent bench clients: interesting (Phase 7), but the simplest experiment answers the fsync question.

## Tradeoffs and honest limits
`--flush-messages=1` was benchmarked with fewer records (5,000) because 200,000 fsyncs would take ~15 minutes.

## How it is tested
`CliTest` runs a small bench (3,000 records) in-process against a real broker and checks the output shape. The real numbers come from the shaded jars.
