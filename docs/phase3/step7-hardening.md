# Step 7 - Hardening: flaky tests, locks and resource closing

## What we are doing
No new features. We (1) run the complete suite many times in a row to flush out tests that only fail sometimes, (2) review how locks are used, and (3) check that every file, socket and thread is closed on every path.

## Why it is designed this way
**A flaky test is a bug report you are ignoring.** A test that fails 1 time in 20 usually exposes a real race (in the test or in the broker). The usual cure people reach for - `Thread.sleep` or "retry the test" - hides the race instead of fixing it. The rule in this project: find the root cause (a missing latch, a shared mutable port, an assertion that assumed an order) and fix that. Running the suite six times in a row is cheap insurance, but be honest about what it proves: a test that fails 1 run in 20 survives six runs about 74% of the time (0.95^6), so six green runs only catch *frequent* flakes. The concurrency tests (many threads, many partitions) fail far more often than that when something is genuinely racy, so they are the ones this exercise is good at catching.

**Lock review checklist** (what we looked at and why it is fine):
* `LogManager.topics` is a `ConcurrentHashMap`: readers (produce, fetch, metadata) never block. The single `creationLock` is held only while a topic is created, so a slow `mkdir`/fsync of a new topic cannot delay requests for existing topics.
* A topic is put into the map only after its metadata file is written and all its logs are open, so a reader can never observe a half-built topic.
* Each partition's `Log` has its own read/write lock (Phase 1): an fsync on partition 0 holds only partition 0's lock. There is no lock that spans two partitions anywhere.
* `runMaintenance` takes each log's own lock briefly, one log at a time; it never holds two logs' locks at once, so it cannot deadlock with a produce.
* `EmberProducer`/`EmberConsumer` methods are `synchronized` on the object: one thread at a time per producer, and each producer owns one connection - nothing is shared between producers.
* `DataDirLock` is released last (after the logs close), and closing it twice is harmless.

**Resource closing checklist:** every `BrokerServer`, `EmberClient`, `EmberProducer`, `EmberConsumer` in tests is in a try-with-resources; the lock file channel is closed on every failure path; a failed `start()` cleans up what it already created; `LogManager` closes the logs it already opened if startup fails halfway.

## Alternatives I rejected and why
* `@RepeatedTest` on everything: slower and still shows only one machine's timing.
* Retrying failed tests automatically: hides real races.
* Raising timeouts until failures stop: a timeout is a safety net for hung tests, not a synchronisation tool.

## Tradeoffs and honest limits
Six green runs do not *prove* there is no race, they make a common one unlikely. Wall-clock timeouts in tests (for example `future.get(120, SECONDS)`) only bound a hang, they never decide pass/fail on speed. A slow or loaded machine can still make the whole build slower, but no assertion depends on timing.

## How it is tested
See the results in the final report: six consecutive complete `mvn test` runs (storage, protocol, client and broker modules, including the 37-second chaos test), with the number of tests and any failures recorded per run.

## What the review actually found (real findings)
The six consecutive runs (258 tests each) were all green, so no flaky test needed fixing. The *review* did find two real problems, both fixed with a test:
1. **`EmberConsumer.poll` could lose records.** It advanced each partition's position as soon as that partition's fetch succeeded. If the fetch of a *later* partition then failed (for example OFFSET_OUT_OF_RANGE), the exception discarded the records already fetched from the earlier partitions, but their positions had moved - those records would be skipped forever. Now positions only move after every fetch of the poll succeeded (test: `aFailedPollMovesNoPositionSoNoRecordIsSkipped`).
2. **`--topic` without `=value` silently became the topic named "true".** The CLI's flag parser accepts bare switches (`--follow`); it now refuses to use a bare switch where a value is needed (test in `CliTest`).

Also noted (documented, not changed): `EmberProducer.sendBatch` sends partitions one after another, so if the second partition's request fails the first partition's records are already stored. The caller cannot tell which records made it, so a retry duplicates them - another face of "at-least-once".
