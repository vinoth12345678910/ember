# Ember Part 3 - The Network Layer (Phase 2)

## One-minute pitch

In Phase 1 Ember learned to store messages safely on one machine: append-only files, an index, crash recovery, retention, and a choice of how often to fsync. But nobody could talk to it. Phase 2 gives Ember a mouth and ears.

We designed a tiny binary language ("the wire protocol") for three questions: *store these records* (PRODUCE), *give me records from offset N* (FETCH), and *what do you have?* (METADATA). We built a broker on Netty that speaks it, a plain-Java client and a command-line tool, and made fetching **zero-copy**: the operating system moves bytes from the log file straight into the network socket without Java ever touching them. This works because the bytes on disk are already exactly the bytes on the wire.

The result: on a laptop, one client produces about 320,000 records per second (about 41 MB/s) and fetches back about 1.3 million records per second (about 170 MB/s). Asking the disk to fsync after every single message drops produce to about 240 records per second. Data survives restarts, 8 concurrent producers never get overlapping offsets, and garbage on the socket never takes the broker down. 166 tests pass.

---

## Step 1 - Scaffolding: four modules, arrows pointing one way

**Why.** If the client could import the broker's code, a change in the broker could silently break clients, and a user who only wants to talk to Ember would download a whole networking framework. So the project is split into modules with a one-way dependency chain: storage <- protocol <- broker, and protocol <- client. Think of a Spring multi-module project where each jar has one job.

**What.** Three new modules, Netty's version managed in one place (a "bill of materials", so all Netty pieces share one version), and the shade plugin that builds "fat" runnable jars, like a Spring Boot jar.

**Analogy.** A restaurant with a kitchen (storage), a menu everyone agrees on (protocol), a waiter who works inside (broker) and a customer outside (client). The customer never walks into the kitchen.

## Step 2 - Storage additions: locks, batches, and "pointers to bytes"

**Why locks.** In Express one event loop runs your code, so two requests rarely touch the same variable at once. A broker serves many connections on many threads, and Phase 1's log was not safe for that: two simultaneous appends could be given the same offset. One read/write lock fixes it: many readers at once, but a writer is alone.

**Why `appendAll`.** A producer sends 500 records in one request. If each took the lock separately, another producer's records could land in the middle of the batch. Holding the lock once makes the batch atomic, so its offsets are contiguous, and it is cheaper.

**Why `openSlice`.** For a fetch we do not need the record bytes in Java at all. We only need *where* they are: this file, from this byte, this long. `openSlice` returns exactly that. It opens its own read-only file handle while still holding the read lock. Why? If retention deletes the segment a millisecond later, an already-open file handle keeps working on macOS and Linux (the data is only freed when the last handle closes), so a fetch in flight is never broken by retention. A test proves it: delete the segment after `openSlice`, then still read from the handle.

**Analogy.** A library: instead of photocopying a book for you, the librarian gives you a bookmark and the shelf number.

## Step 3 - The protocol: a contract written in bytes

**Why.** TCP gives you a stream of bytes, not messages. So every message starts with its length, then fixed fields. Every response echoes a number the client chose (the correlation id), so the client can match answers to questions. Errors are a small numeric code; an error response simply ends after the code.

**The big lesson, again.** Never trust a length you have not checked against the bytes actually left. A message saying "I contain two billion records" must be rejected *before* we allocate a list for them. Every count and length is validated against the remaining bytes, and leftover bytes after a body are an error too. 40 protocol tests cover negative counts, lengths past the end, bad UTF-8, topics that are too long, and more.

**Why binary, not JSON.** The FETCH response is the raw log file bytes. With JSON we would have to decode and re-encode every record and zero-copy would be impossible.

**Analogy.** A shipping label with fixed boxes: sender, weight, destination. A post office can sort millions of parcels because the label is always in the same place.

## Step 4 - Broker core without networking

**Why.** Keep "what the broker does" separate from "how bytes arrive", the same as a Spring `@Service` versus a `@Controller`. `BrokerApis` takes a request object and returns a response object; no sockets. So we can test it with a fake clock and finish in milliseconds.

**Fake clock.** The broker stamps records with the time. Retention and flush timers also depend on the time. With a clock that tests move by hand, "a week later" takes one line instead of a week of sleeping. No test in Phase 2 sleeps.

**Directory names.** Each topic lives in `<topic>-0` (the 0 is the partition). Topic names may contain dashes, so on restart the broker splits at the *last* dash.

**Auto-creation race.** Two producers may create the same new topic at the same instant. `ConcurrentHashMap.computeIfAbsent` guarantees one log is created; two logs on one folder would corrupt it.

## Step 5 - The Netty server

**Why Netty.** One thread per connection does not scale (10,000 clients = 10,000 threads). Netty works like Node: a few event-loop threads watch many sockets and call our code when bytes arrive.

**The pipeline** (like Express middleware): an idle-timeout handler, then a frame decoder that cuts the byte stream into whole requests and refuses absurd sizes early, then our request handler.

**Why a separate thread pool for the handler.** Produce and fetch touch the disk and may fsync, which can take milliseconds. Doing that on an event-loop thread would freeze every other connection on that loop, like calling `readFileSync` in Express. So the event loops only move bytes.

**Ordering promise.** Netty pins each connection to one handler thread, so requests on one connection are answered in the order they were sent. That allows pipelining (send ten requests, then read ten answers).

**Failure rules.** Anything that goes wrong while handling a request becomes an INTERNAL_ERROR reply. A request that cannot be parsed but has a readable header gets INVALID_REQUEST. If even the header is unreadable or the framing is broken, we close that one connection.

## Step 6 - Zero-copy fetch

**Why.** The normal way to send a file: kernel reads it into your memory, you hand it back to the kernel for the socket. Two copies and a lot of garbage. With `transferTo` (the `sendfile` system call) the kernel moves bytes from the file cache to the socket directly. Java never sees the data.

**The trick that makes it possible.** The on-disk record format *is* the wire format. The broker writes a small header (lengths, offsets), then tells Netty "send bytes X to Y of this file". That is also why the broker does not check CRCs when serving; the client does.

**File handle lifecycle.** If Netty accepted the file region, Netty closes the handle when the transfer ends or the connection dies; otherwise our `finally` block closes it. No leaks.

**Rules for the response.** Whole records only, never more than `maxBytes`, except the first record is always returned even if it is bigger, so a consumer can never get stuck. A response never crosses a segment boundary; the client simply asks again.

## Step 7 - The client and the CLI

**Why blocking, one request at a time.** It is the easiest thing to reason about, like `await fetch(...)`. Methods are synchronized so threads can share a client. A faster client would use one per thread (the concurrency test does).

**Safety rules.** The client checks the correlation id of every answer. If it is wrong, or a read times out, the client closes itself, because a late answer could be mistaken for the next one. It checks the response length against a limit before allocating. It verifies every record's CRC when decoding.

**CLI.** `produce`, `fetch`, `metadata`, and `bench`. The jars are shaded, so `java -jar` works.

## Step 8 - Hardening

We attacked our own broker: HTTP text sent to the binary port (read as a length it is about 1.2 GB, so it is refused), a frame one byte over the limit, negative and zero lengths, a request delivered one byte per packet, two requests in one packet, 200 simultaneous connections, a client that vanishes in the middle of a 4 MB download, and an idle connection. In every case only that one connection suffers; the broker keeps serving.

## Step 9 - Benchmark, documentation

One client, one connection, 100-byte values, batches of 500, JDK 21 on a MacBook, client and broker on the same machine. "MB/s" counts whole encoded records.

| Broker config | Produce | Fetch (read back everything) |
|---|---|---|
| default (fsync at most once per second), 200,000 records | 321,502 records/s, 41.15 MB/s | 1,312,788 records/s, 168.04 MB/s |
| fsync after **every** message, 5,000 records | 237 records/s, 0.03 MB/s | 221,273 records/s, 28.32 MB/s |
| never fsync on its own, 200,000 records | 327,885 records/s, 41.97 MB/s | 1,356,114 records/s, 173.58 MB/s |

How to read this: the default and "never" are nearly the same, so fsync once a second costs almost nothing here. fsync after every message is roughly 1,350 times slower for produce: each message waits for the physical disk (about 4 ms), the same cost Phase 1 measured. The every-message run used 5,000 records because 200,000 would take about 14 minutes. Fetch numbers are high because the data is still in the operating system's file cache; they show the speed of the network path, not of the disk. The every-message fetch figure is lower mostly because it is a tiny 5,000-record run, not because fetch is slower.

## Step 10 - Final verification

`mvn clean verify` ran green. We started the real broker jar, used the real CLI jar to produce, fetch, ask metadata, run a 200,000-record bench, and then stopped the broker with SIGTERM. The shutdown hook printed "Ember broker shut down cleanly" and the exit code was 143 (128 + 15, the standard "terminated" code). Restarting on the same folder returned the same records and topic sizes. Ctrl+C (SIGINT) gave exit code 130 and the same message. Fetching offset 9 of a 2-record topic returned the OFFSET_OUT_OF_RANGE error with exit code 1.

**Test counts:** ember-storage 55 (the original 38 plus 17 new), ember-protocol 40, ember-client 13, ember-broker 58. **Total 166**, all green. Slowest classes: ChaosTest 37.6 s (the Phase 1 crash simulation), ClientIntegrationTest 1.9 s, HardeningTest 1.7 s, FlushPolicyTest 1.4 s, FetchOverTheWireTest 1.1 s. The whole build takes about 52 s.

---

## Bugs and surprises we hit

1. **Phase 1's real code was in the test folder.** `Log`, `Segment`, `LogRecord` and friends sat in `src/test/java`, so no other module could depend on them. We moved six files to `src/main/java` without touching a single test. Lesson: a green test run does not tell you where the code lives.
2. **Shutting down in the wrong order made Netty complain.** Our first integration test passed but printed walls of `RejectedExecutionException: event executor terminated`. We had stopped the handler thread pool first, and then closing client connections tried to schedule "connection closed" events on a dead pool. The fix: stop accepting, close connections, stop the I/O threads, then stop the handler pool, then close the logs.
3. **The client mislabelled its own mistake.** Producing to a topic called "bad name" made the *client's* encoder throw, and the client reported it as "malformed response" and closed the connection. The integration test caught it. Now a bad argument throws `IllegalArgumentException`, nothing is sent, and the client stays usable.
4. **A test script that deadlocked itself.** The fake server in a client test waited for two requests before answering, but a blocking client only sends the second after getting the first answer. The test failed with a read timeout and the fix was in the test, not the client.
5. **Netty counts the length field in its frame limit.** `LengthFieldBasedFrameDecoder`'s maximum includes the 4 length bytes, so a limit of 1024 would reject a 1024-byte body. We add 4, and a test sends a body of exactly the limit and one byte over to pin this down. (We knew this trap going in; the test is what keeps it fixed.)
6. **fsync-every-message made the bench impractical.** 200,000 messages would take about 14 minutes, so that config uses 5,000 records and the doc says so.
7. **A Ctrl+C that "hangs" in a script is not the broker.** A background process started from a non-interactive shell ignores SIGINT. We proved the broker honours real Ctrl+C by sending SIGINT with the default handler set: shutdown hook ran, exit code 130.

---

## Glossary

- **Broker**: the server that stores and serves messages.
- **Topic**: a named stream of records. In Phase 2 each topic has exactly one partition.
- **Offset**: the position number of a record in a topic, starting at 0.
- **Frame**: one complete message on the wire, preceded by its length.
- **Correlation id**: a number the client picks per request; the broker echoes it so the client can match answers to questions.
- **Zero-copy**: sending file bytes to a socket without copying them through the program's memory (`sendfile` / `transferTo`).
- **Netty**: a Java library for fast network servers using a few event-loop threads.
- **Event loop**: a thread that waits for network events and runs short handlers (the Node.js idea).
- **Pipeline**: the chain of handlers a connection's bytes pass through.
- **Pipelining**: sending several requests before reading any reply.
- **Segment**: one file of the log; a new one starts when the active one is full.
- **Retention**: deleting old segments by age or size.
- **fsync**: asking the operating system to push written data to the physical disk.
- **CRC**: a checksum that reveals corrupted bytes.
- **At-least-once**: if a client retries, a record may be stored twice; nothing is lost but duplicates are possible.
- **High watermark**: the offset up to which records are safe to read. In Phase 2 it equals the log end; replication will change it later.
- **Shaded jar**: one jar containing the program and all its libraries.
- **BOM**: a Maven file that fixes the versions of a group of libraries together.

## Suggested NotebookLM prompts

**Video overview:** "Make a video overview of Ember Phase 2 for a student who knows Spring Boot and Express but is new to networking. Follow the steps in order, and for each one explain the WHY before the WHAT using the analogies in the document (the library bookmark, the shipping label, the event loop as Node). Spend extra time on zero-copy fetch and on why fsync-after-every-message is about 1,350 times slower. Finish with the real bugs we hit, especially the shutdown-order bug."

**Quiz:** "Write a 12-question quiz on Ember Phase 2: 4 multiple choice on the protocol (framing, correlation id, validating lengths), 4 on Netty and threading (event loop vs handler pool, ordering guarantee, shutdown order), 2 on zero-copy and slices (why the channel is opened under the read lock), and 2 on the benchmark numbers (what the fsync results show and why fetch is so fast). Give the answers and a one-sentence explanation each."
