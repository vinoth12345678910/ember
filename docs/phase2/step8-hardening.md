# Step 8 - Hardening: what happens when the world is not friendly

## What we are building
No new features: a test suite (`HardeningTest`, plus the shutdown and restart tests) that attacks the broker the way real networks do, and the fixes those tests demanded.

## Why it is designed this way
A server is not finished when the happy path works. On the internet (or just a buggy client) you will see: bytes that aren't your protocol, half-sent requests, a 2 GB "length", clients that disappear mid-download, thousands of idle sockets. Every one of those must cost the *broker* nothing but a log line.

The defences, and the reason for each:
* **Frame-size limit** (`maxFrameBytes`): `LengthFieldBasedFrameDecoder` rejects the frame from its 4-byte prefix, before buffering it. Without it, one connection announcing "4 GB" could eat the heap. Because the framing can't be trusted afterwards, we close the connection.
* **Garbage = huge length.** An HTTP request `GET /...` read as a big-endian INT32 is about 1.2 billion: above any sane limit, so it is closed quickly.
* **Negative / zero / too-short frames** also close the connection (no correlationId to answer to).
* **Unparseable body but readable header** -> INVALID_REQUEST with the correlationId; the connection stays usable. Cheap for the client to recover.
* **Idle timeout**: a connection that sends nothing for `idleTimeoutMs` is closed, so crashed clients don't leak sockets.
* **Pipelining**: because Netty pins a connection to one handler thread, a client may send 100 requests before reading anything and the answers come back in order, including a zero-copy FETCH in the middle (the header buffer and file region are written in sequence on the same channel).
* **Vanishing clients**: a client that closes during a large fetch makes the write fail; Netty releases the `FileRegion`, which closes the file descriptor. A test does this repeatedly and then checks the broker still answers.
* **Shutdown order** (found by a test!): closing client channels *after* the handler thread pool had stopped made Netty try to schedule "channel unregistered" events on a dead executor (`RejectedExecutionException` noise). Order is now: stop accepting -> close connections -> stop I/O threads -> stop handler threads -> stop timer + close logs.

## Alternatives I rejected and why
* Answering oversized frames with REQUEST_TOO_LARGE: nicer for the client but the broker would have to read and discard up to 2 GB to find the next frame boundary. Closing is safer. (The error code is reserved.)
* Rate-limiting / connection caps: later phase.

## Tradeoffs and honest limits
* The idle timer counts *reads*. A client that is downloading a huge response but sends nothing for 5 minutes would be closed.
* No write back-pressure: a client that stops reading makes the broker queue its responses in memory. Phase 7.
* All segments of all topics keep two file descriptors open (log + index); a huge number of segments needs a bigger `ulimit -n` (or an LRU of open segments - future work).

## How it is tested
`HardeningTest` (garbage, oversize, exact-limit frame, negative/short/zero frames, pipelining order across all three APIs, byte-by-byte delivery, two requests in one write, idle timeout, vanishing client, 200 simultaneous connections, a silent socket); `ProduceMetadataOverTheWireTest` (graceful shutdown with a connected client); `ClientIntegrationTest` (restart persistence, retention + fetch, 8 concurrent clients).
