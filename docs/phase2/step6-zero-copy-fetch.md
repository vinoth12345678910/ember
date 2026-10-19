# Step 6 - Zero-copy fetch over the wire

## What we are building
The FETCH path: `RequestHandler` asks `BrokerApis` for a `LogSlice`, writes a small header buffer, then a Netty `DefaultFileRegion` that points at the slice's bytes in the segment file.

## Why it is designed this way
**The normal way to serve a file.** In Express you would `fs.readFile` (kernel -> your process memory) and `res.send` (your memory -> kernel socket buffer): the same bytes are copied twice and cross the user/kernel boundary twice. The Java heap also fills with garbage the GC must clean.

**Zero-copy.** `FileChannel.transferTo` asks the OS to move bytes from the file's page cache straight into the socket (`sendfile` on Linux/macOS). The data never enters the JVM. Netty exposes it as `FileRegion`. The broker does not even look at the records - which is only possible because *the bytes on disk are already exactly the bytes of the wire format*. That is why LogRecord's layout (length, crc, offset, ...) is also the FETCH response format, and why the broker doesn't verify CRCs when serving: the client verifies them.

**Frame length first.** The response starts with INT32 frameLength, which must include the records. We know `slice.length` before sending anything, so the header can state the final size up front (`FetchResponse.encodeHeader`).

**Lifecycle of the file descriptor.** `openSlice` opened a private read-only channel (Step 2). If we hand it to Netty, Netty closes it when the region is released (after the transfer finishes, or when the connection dies). If anything throws *before* the hand-over, a `finally` closes it. No leaked descriptors, and no early close that would break the transfer.

**Cut at segment end.** One response = one contiguous range of one file = one `transferTo`. Clients just fetch again at `lastOffset + 1`.

**maxBytes semantic.** Whole records only, at most maxBytes, except the first record is always allowed so a big record can never block a consumer forever.

## Alternatives I rejected and why
* Read records into byte[] and encode them: simple, but copies and allocates per fetch.
* Memory-mapped files: lifetime/unmapping problems in Java.
* Splitting a big record across responses: complicates the client; Kafka doesn't either.

## Tradeoffs and honest limits
* Records are not CRC-checked on the broker: silent disk corruption is only noticed by the client.
* `transferTo` of a big region may take several event-loop passes; with a slow reader the region waits in Netty's outbound buffer (no back-pressure yet).
* If retention deletes a segment while a transfer is running, the open descriptor still works (the OS keeps the data until the descriptor is closed).

## How it is tested
Round trips over a real socket: multi-segment catch-up loop (every record exactly once, in order), maxBytes respected, first-record exception, fetch from the middle, at the end (empty), out of range, unknown topic, response bytes identical to the file bytes, and a ~1 MiB response (several partial `transferTo` calls).
