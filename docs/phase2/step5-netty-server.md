# Step 5 - The Netty server

## What we are building
`BrokerServer`: opens a TCP port, accepts connections, cuts the byte stream into request frames, hands each frame to `BrokerApis`, writes the response. `EmberBroker` is the `main()` that starts it.

## Why it is designed this way
**Why Netty and not `ServerSocket` + one thread per connection?** Think Express (Node): one event loop serves thousands of sockets because it never blocks. Plain Java sockets block a whole thread per connection; 10,000 clients means 10,000 threads. Netty gives Node's model on the JVM: a few *event-loop* threads watch many sockets (`epoll`/`kqueue` via NIO) and call our handlers when bytes arrive.

**Boss and worker groups.** The boss loop (1 thread) only accepts new connections; it hands each to a worker loop, which owns that connection forever (all its I/O happens on one thread, so no locks are needed per connection).

**Pipeline.** Each connection has a chain of handlers, like Express middleware:
1. `IdleStateHandler` - fires an event if nothing was read for `idleTimeoutMs`; we close the connection (a client that vanished without closing would otherwise hold memory forever).
2. `LengthFieldBasedFrameDecoder` - TCP delivers a stream, not messages: one `read` may contain half a request or three requests. This handler reads the INT32 length and emits exactly one complete frame each time. It also enforces `maxFrameBytes`: a frame claiming 2 GB is rejected *before* any buffer is allocated.
3. `RequestHandler` - decode, dispatch to `BrokerApis`, write the reply.

**Why `RequestHandler` runs on a separate `DefaultEventExecutorGroup`.** Produce does disk writes and possibly `fsync` (milliseconds or worse). If that ran on an event loop, every other connection assigned to that loop would freeze - like calling `fs.readFileSync` in an Express handler. So I/O threads only move bytes; a small pool of "handler" threads does the blocking work.

**Ordering guarantee.** Netty pins each connection to ONE executor of the group, so requests of one connection are handled one after another and the responses are written in request order. A client may pipeline (send several requests without waiting) and match answers by order (and correlationId). Different connections have no ordering between them.

**Errors never kill the broker.** Any exception while handling a request becomes an INTERNAL_ERROR response (and is logged). A body that does not parse becomes INVALID_REQUEST with the request's correlationId. If we cannot even read a correlationId, or the framing itself is broken (frame too large, negative length), we close that one connection; framing can't be trusted afterwards.

**Graceful shutdown order.** (1) stop accepting, (2) let handler threads finish in-flight requests (bounded wait), (3) shut down the event loops (closes connections), (4) stop the maintenance timer, (5) close `LogManager`, which fsyncs every log. Closing twice is harmless.

## Alternatives I rejected and why
* Thread-per-connection: simplest to read, does not scale, and teaches less about real brokers.
* Java `AsynchronousSocketChannel` / NIO selectors by hand: it is what Netty does for us, with fewer corner cases handled.
* Running handlers on the event loop: blocks I/O during fsync.

## Tradeoffs and honest limits
* No write back-pressure: if a client reads very slowly Netty queues responses in memory. Phase 7 should use `channel.isWritable()`.
* `handlerThreads` bounds concurrency of disk work; a slow disk makes all connections queue.
* No TLS, no auth.

## How it is tested
Integration tests start a real `BrokerServer` on port 0 with a tiny segment size and talk to it through a raw-socket helper (Step 5), and later through the real client (Step 7). Covered: produce + metadata over the wire, error responses, pipelined ordering, garbage/oversize frames, graceful shutdown with a connected client.
