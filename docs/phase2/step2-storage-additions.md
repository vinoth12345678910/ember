# Step 2 - Storage additions: thread-safe Log, appendAll, openSlice

## What we are building
1. `Log` becomes safe to use from many threads (one `ReentrantReadWriteLock`).
2. `Log.appendAll(timestamp, List<KeyValue>)`: append a whole batch atomically, get back the base offset.
3. `Log.openSlice(fetchOffset, maxBytes)` returning a `LogSlice`: "this file, from this byte, this many bytes" - a pointer into the segment file instead of a copy of the data.

## Why it is designed this way
**Thread safety.** In Express, one event loop runs your handler, so you rarely think about two requests touching the same variable. In a broker, Netty hands us requests on several threads. Phase 1's `Log` mutates a `TreeMap`, a "current segment" pointer and counters - two concurrent appends could hand out the same offset. A read/write lock says: many readers at once, but a writer has the log to itself. Fetches are the common case and they only read, so they don't block each other.

**appendAll under one write-lock hold.** A producer sends 500 records in one request. If every record took the lock separately, another producer's records could land in the middle and the offsets of "one batch" would not be contiguous. Holding the lock once makes the batch atomic. It also costs one lock acquisition instead of 500.

**openSlice instead of read().** Reading records into `byte[]`, then copying them into a network buffer, wastes CPU and memory. The OS can send bytes from a file straight to a socket (`FileChannel.transferTo` -> `sendfile`); this is "zero-copy". For that we only need *where* the bytes are. `openSlice` finds the byte range and hands back a channel, a position and a length. Because the stored record format IS the wire format, nothing needs converting.

**Why the channel is opened inside the read lock.** Retention may delete a segment file right after we computed the slice. If we opened the file *after* releasing the lock, we might find it gone. On macOS/Linux an already-open file descriptor keeps working after the file is unlinked (the data is freed when the last descriptor closes). So we open a fresh read-only channel while still holding the read lock (retention needs the write lock, so the file cannot vanish meanwhile). A fetch in flight is never broken by retention. We use a *new* channel, not the segment's, because the segment channel is closed on deletion.

**CRCs are not verified while walking.** We only read the 16-byte header (length, crc, offset) to hop from record to record. The client verifies CRC when it decodes. Verifying would force the broker to read all the bytes, defeating zero-copy.

**Slices never cross a segment.** A response comes from one file => one `transferTo`. The client just fetches again at the next offset (Kafka does the same).

## Alternatives I rejected and why
* `synchronized` on every method: simple but readers would block each other.
* A lock per segment: much more complex; the Log-level structure (TreeMap, active pointer) needs protecting anyway.
* `MappedByteBuffer`: unmapping is unreliable in Java and lifetime bugs crash the JVM.
* Copying into heap `byte[]`: easy, but it is exactly the cost zero-copy removes.

## Tradeoffs and honest limits
* Writers block readers briefly (an `fsync` inside `append` holds the write lock - fsync-per-message makes fetches wait). Fixing this means separating "append" from "flush"; later phase.
* `LogSlice` owns a file descriptor: callers MUST close it. Netty closes it for us once the file region is released.
* Windows can't delete open files; we target macOS/Linux.
* `totalSizeInBytes` etc. take the read lock but call into `FileChannel.size()`, which is fine.

## How it is tested
Segment helpers: unit tests (position of first/middle/last record, whole-record extents under various maxBytes). Log: slices at the first record, in the middle, at the end, one record bigger than maxBytes, spanning a segment end, out-of-range both sides, delete-while-open, `appendAll` contiguity, and a stress test (8 writers x 500 records, 4 readers calling `read` and `openSlice` continuously).
