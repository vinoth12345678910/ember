package com.ember.storage;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

/**
 * Thread-safe: ONE read/write lock guards the segment map, the active-segment pointer and the
 * flush counters. Readers (fetch) share the lock; anything that changes the log takes it alone.
 */
public class Log implements AutoCloseable {

    private static final int DEFAULT_INDEX_INTERVAL_BYTES = 4096;

    private final Path dir;
    private final long maxSegmentBytes;
    private final int indexIntervalBytes;
    private final FlushPolicy flushPolicy;
    private final TreeMap<Long, Segment> segments = new TreeMap<>();
    private Segment active;
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    private long unflushed = 0;            // records appended since the last fsync
    private long firstUnflushedMs = 0;     // timestamp of the oldest unflushed record
    private long flushCount = 0;           // how many fsyncs we have done (for tests and metrics)

    public Log(Path dir, long maxSegmentBytes) throws IOException {
        this(dir, maxSegmentBytes, DEFAULT_INDEX_INTERVAL_BYTES);
    }

    public Log(Path dir, long maxSegmentBytes, int indexIntervalBytes) throws IOException {
        this(dir, maxSegmentBytes, indexIntervalBytes, FlushPolicy.never());
    }

    public Log(Path dir, long maxSegmentBytes, int indexIntervalBytes, FlushPolicy flushPolicy)
            throws IOException {
        Files.createDirectories(dir);
        this.dir = dir;
        this.maxSegmentBytes = maxSegmentBytes;
        this.indexIntervalBytes = indexIntervalBytes;
        this.flushPolicy = flushPolicy;

        try {
            for (long base : existingBaseOffsets(dir)) {
                newSegment(base);                       // each Segment recovers itself
            }
            verifyNoGaps();
            this.active = segments.isEmpty() ? newSegment(0) : segments.lastEntry().getValue();
        } catch (IOException | RuntimeException e) {
            close();                                    // don't leak file handles on failure
            throw e;
        }
    }

    /**
     * Appends a record, rolling to a new segment first if the active one is full.
     * The timestamp is the broker-assigned time of receipt; the flush timer uses it as "now".
     */
    public long append(long timestamp, byte[] key, byte[] value) throws IOException {
        writeLock.lock();
        try {
            return appendLocked(timestamp, key, value);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Appends a whole batch under ONE write-lock hold, so no other writer can slip records in
     * between: the offsets are contiguous. Returns the offset of the first record.
     * Flush accounting is exactly the same as calling append() repeatedly.
     */
    public long appendAll(long timestamp, List<KeyValue> records) throws IOException {
        if (records.isEmpty()) {
            throw new IllegalArgumentException("records must not be empty");
        }
        writeLock.lock();
        try {
            long base = -1;
            for (KeyValue kv : records) {
                long offset = appendLocked(timestamp, kv.key(), kv.value());
                if (base < 0) {
                    base = offset;
                }
            }
            return base;
        } finally {
            writeLock.unlock();
        }
    }

    private long appendLocked(long timestamp, byte[] key, byte[] value) throws IOException {
        if (active.sizeInBytes() >= maxSegmentBytes) {
            if (unflushed > 0) {
                doFlush();                              // sealed segments never change again
            }
            active = newSegment(active.nextOffset());
        }
        long offset = active.append(timestamp, key, value);

        if (unflushed == 0) {
            firstUnflushedMs = timestamp;
        }
        unflushed++;
        if (unflushed >= flushPolicy.maxUnflushedMessages()) {
            doFlush();
        }
        return offset;
    }

    /**
     * Called periodically by a timer. Flushes if the oldest unflushed record has been
     * waiting for at least maxUnflushedMs. Returns true if it flushed.
     */
    public boolean maybeFlush(long nowMs) throws IOException {
        writeLock.lock();
        try {
            if (unflushed > 0 && nowMs - firstUnflushedMs >= flushPolicy.maxUnflushedMs()) {
                doFlush();
                return true;
            }
            return false;
        } finally {
            writeLock.unlock();
        }
    }

    /** Forces an fsync now (does nothing if there is nothing unflushed). */
    public void flush() throws IOException {
        writeLock.lock();
        try {
            if (unflushed > 0) {
                doFlush();
            }
        } finally {
            writeLock.unlock();
        }
    }

    /** Finds the segment that owns this offset, then lets that segment find the record. */
    public LogRecord read(long offset) throws IOException {
        readLock.lock();
        try {
            Map.Entry<Long, Segment> entry = segments.floorEntry(offset);
            if (entry == null) {
                return null;
            }
            return entry.getValue().read(offset);
        } finally {
            readLock.unlock();
        }
    }

    public long nextOffset() {
        readLock.lock();
        try {
            return active.nextOffset();
        } finally {
            readLock.unlock();
        }
    }

    public long startOffset() {
        readLock.lock();
        try {
            return segments.firstKey();
        } finally {
            readLock.unlock();
        }
    }

    public int segmentCount() {
        readLock.lock();
        try {
            return segments.size();
        } finally {
            readLock.unlock();
        }
    }

    public List<Long> segmentBaseOffsets() {
        readLock.lock();
        try {
            return new ArrayList<>(segments.keySet());
        } finally {
            readLock.unlock();
        }
    }

    public long flushCount() {
        readLock.lock();
        try {
            return flushCount;
        } finally {
            readLock.unlock();
        }
    }

    public long unflushedMessages() {
        readLock.lock();
        try {
            return unflushed;
        } finally {
            readLock.unlock();
        }
    }

    public long totalSizeInBytes() throws IOException {
        readLock.lock();
        try {
            long total = 0;
            for (Segment segment : segments.values()) {
                total += segment.sizeInBytes();
            }
            return total;
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Deletes whole segments from the oldest end while they are expired (their newest record
     * is older than retentionMs) or while the log is bigger than retentionBytes.
     * The active segment is never deleted. Returns how many segments were removed.
     */
    public int deleteOldSegments(long nowMs, long retentionMs, long retentionBytes) throws IOException {
        writeLock.lock();
        try {
            int deleted = 0;
            long totalBytes = totalSizeInBytes();

            while (segments.size() > 1) {
                Segment oldest = segments.firstEntry().getValue();
                boolean tooOld = nowMs - oldest.maxTimestamp() > retentionMs;
                boolean tooBig = totalBytes > retentionBytes;
                if (!tooOld && !tooBig) {
                    break;                                   // oldest is fine, so all the newer ones are too
                }
                totalBytes -= oldest.sizeInBytes();
                segments.remove(oldest.baseOffset());
                oldest.delete();
                deleted++;
            }
            return deleted;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        writeLock.lock();
        try {
            if (active != null && unflushed > 0) {
                active.flush();                              // clean shutdown: never leave data in the page cache
                unflushed = 0;
            }
            for (Segment segment : segments.values()) {
                segment.close();
            }
        } finally {
            writeLock.unlock();
        }
    }

    private void doFlush() throws IOException {
        active.flush();
        unflushed = 0;
        flushCount++;
    }

    private Segment newSegment(long baseOffset) throws IOException {
        Segment segment = new Segment(dir, baseOffset, indexIntervalBytes);
        segments.put(baseOffset, segment);
        return segment;
    }

    private static List<Long> existingBaseOffsets(Path dir) throws IOException {
        try (Stream<Path> files = Files.list(dir)) {
            return files.map(p -> p.getFileName().toString())
                    .filter(name -> name.endsWith(".log"))
                    .map(name -> Long.parseLong(name.substring(0, name.length() - 4)))
                    .sorted()
                    .toList();
        }
    }

    /** A sealed segment must end exactly where the next one begins; otherwise data is missing. */
    private void verifyNoGaps() {
        Segment previous = null;
        for (Segment current : segments.values()) {
            if (previous != null && previous.nextOffset() != current.baseOffset()) {
                throw new IllegalStateException("gap in log: segment " + previous.baseOffset()
                        + " ends at " + previous.nextOffset() + " but the next one starts at "
                        + current.baseOffset());
            }
            previous = current;
        }
    }

    /**
     * Finds the bytes to send for a fetch, without reading them. See LogSlice for the contract.
     * The read-only channel is opened while we still hold the read lock: retention needs the
     * write lock to delete a file, so the file cannot vanish between "find" and "open", and an
     * already-open descriptor keeps working even if the file is unlinked right afterwards.
     */
    public LogSlice openSlice(long fetchOffset, int maxBytes) throws IOException {
        readLock.lock();
        try {
            long start = segments.firstKey();
            long end = active.nextOffset();
            if (fetchOffset < start || fetchOffset > end) {
                throw new OffsetOutOfRangeException("offset " + fetchOffset
                        + " is out of range, valid range is [" + start + ", " + end + "]");
            }
            if (fetchOffset == end) {
                return LogSlice.empty(fetchOffset, start, end);
            }
            Segment segment = segments.floorEntry(fetchOffset).getValue();
            long position = segment.positionOf(fetchOffset);
            Segment.Extent extent = segment.lengthOfWholeRecords(position, maxBytes);
            FileChannel channel = segment.openReadOnlyChannel();
            return new LogSlice(channel, position, extent.length(), fetchOffset, extent.lastOffset(),
                    extent.recordCount(), start, end);
        } finally {
            readLock.unlock();
        }
    }
}
