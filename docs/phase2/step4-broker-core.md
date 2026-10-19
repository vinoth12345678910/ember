# Step 4 - Broker core without networking

## What we are building
* `BrokerConfig` - all settings in one immutable record, parsed from `--name=value` flags.
* `LogManager` - owns one `Log` per topic, in `<dataDir>/<topic>-0/`, reopens them at start, runs periodic maintenance (fsync per policy, retention).
* `BrokerApis` - the business logic: `ProduceRequest -> ProduceResponse`, `FetchRequest -> LogSlice + header values`, `MetadataRequest -> MetadataResponse`. No sockets and no Netty types.

## Why it is designed this way
**Separate "what the broker does" from "how bytes arrive".** In Spring you keep a `@Service` that knows nothing about HTTP, and a thin `@Controller` on top. `BrokerApis` is the service; the Netty handler (Step 5) is the controller. The service can then be tested with plain method calls, a fake clock and a temp directory - fast, deterministic, no ports.

**Injectable `Clock`.** The broker stamps each record with `Clock.millis()`. Retention and the flush timer also depend on "now". If the code called `System.currentTimeMillis()` directly, tests would have to sleep. A `MutableClock` in tests lets us jump a week ahead instantly.

**Directory `<topic>-0`.** The `-0` is the partition number (Kafka does this too). Topic names may contain dashes, so on start we split at the LAST dash: `my-topic-0` -> topic `my-topic`. Only `-0` directories are opened in Phase 2. (Topic validation forbids anything that could escape the data dir - no `/` or `..` - which is also a security reason for the strict name rule.)

**`ConcurrentHashMap.computeIfAbsent`.** Two producers may create the same new topic at the same instant; this guarantees exactly one `Log` is created for it (two `Log`s on one directory would corrupt it).

**Maintenance is a method.** `runMaintenance(nowMs)` does the work; a daemon scheduler merely calls it every `maintenanceIntervalMs`. Tests call the method directly and never wait.

**Broker timestamp, one per request.** A produce request with 500 records gets the same timestamp for all, and `appendAll` writes them under one lock hold, so offsets are contiguous.

## Alternatives I rejected and why
* One `Log` for all topics: no isolation, no per-topic retention.
* Lazy-open topics on first request after restart: a fetch right after restart would wrongly say "unknown topic".
* A config library: the flag format is trivial; no dependency needed.

## Tradeoffs and honest limits
* Case-insensitive file systems (default macOS): topics `Orders` and `orders` would share a directory. Not guarded in Phase 2.
* If one Log's maintenance fails (disk error) we log it and carry on with the other logs.
* Topics are never deleted.

## How it is tested
`BrokerApisTest` (fake clock): auto-create, invalid topic, bad partition, timestamp from the clock, contiguous offsets, metadata for all/specific/unknown, fetch errors and ranges. `LogManagerTest`: restart reopens topics, maintenance flushes by policy and applies retention using the fake clock, topic names with dashes. `BrokerConfigTest`: flag parsing.
