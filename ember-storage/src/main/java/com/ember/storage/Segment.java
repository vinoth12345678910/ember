package com.ember.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

public class Segment implements AutoCloseable {

    private static final int DEFAULT_INDEX_INTERVAL_BYTES = 4096;

    private final long baseOffset;
    private final Path file;
    private final FileChannel channel;
    private final OffsetIndex index;
    private final int indexIntervalBytes;
    private final long recoveredTruncatedBytes;
    private long nextOffset;
    private long maxTimestamp = -1;                   // -1 means "no records yet"
    private int bytesSinceLastIndexEntry = 0;

    public Segment(Path dir, long baseOffset) throws IOException {
        this(dir, baseOffset, DEFAULT_INDEX_INTERVAL_BYTES);
    }

    public Segment(Path dir, long baseOffset, int indexIntervalBytes) throws IOException {
        Files.createDirectories(dir);
        this.baseOffset = baseOffset;
        this.nextOffset = baseOffset;
        this.indexIntervalBytes = indexIntervalBytes;
        this.file = dir.resolve(String.format("%020d.log", baseOffset));
        this.channel = FileChannel.open(file, CREATE, READ, WRITE);
        this.index = new OffsetIndex(dir, baseOffset);
        this.recoveredTruncatedBytes = recover();     // fresh file: nothing to scan
    }

    /**
     * Scans the file from byte 0, keeps every valid record, cuts off everything from the
     * first bad one, and rebuilds the index, nextOffset and maxTimestamp. Returns how many bytes were cut.
     */
    private long recover() throws IOException {
        index.reset();                                // the index is derived data: rebuild it
        nextOffset = baseOffset;
        maxTimestamp = -1;
        bytesSinceLastIndexEntry = 0;

        long pos = 0;
        while (true) {
            LogRecord r;
            try {
                r = readAt(pos);
            } catch (CorruptRecordException e) {
                break;                                // bad length, bad CRC or half-written
            }
            if (r == null || r.offset() != nextOffset) {
                break;                                // clean end, or a record in the wrong place
            }
            if (bytesSinceLastIndexEntry >= indexIntervalBytes) {
                index.append(r.offset(), pos);        // same rule as append()
                bytesSinceLastIndexEntry = 0;
            }
            pos += r.sizeInBytes();
            bytesSinceLastIndexEntry += r.sizeInBytes();
            nextOffset = r.offset() + 1;
            maxTimestamp = Math.max(maxTimestamp, r.timestamp());
        }

        long cut = channel.size() - pos;
        if (cut > 0) {
            channel.truncate(pos);
            channel.force(false);
        }
        return cut;
    }

    /** Appends one record at the end of the file and returns the offset it was given. */
    public long append(long timestamp, byte[] key, byte[] value) throws IOException {
        LogRecord record = new LogRecord(nextOffset, timestamp, key, value);
        ByteBuffer buf = record.encode();
        int size = buf.remaining();

        long position = channel.size();

        // sparse: only add an index entry once enough bytes have passed since the last one
        if (bytesSinceLastIndexEntry >= indexIntervalBytes) {
            index.append(nextOffset, position);
            bytesSinceLastIndexEntry = 0;
        }

        long writeAt = position;
        while (buf.hasRemaining()) {
            writeAt += channel.write(buf, writeAt);
        }
        bytesSinceLastIndexEntry += size;
        maxTimestamp = Math.max(maxTimestamp, timestamp);
        return nextOffset++;
    }

    /** Finds the record with exactly this offset: jump with the index, then scan forward. */
    public LogRecord read(long targetOffset) throws IOException {
        if (targetOffset < baseOffset || targetOffset >= nextOffset) {
            return null;
        }
        long pos = index.lookup(targetOffset);
        while (true) {
            LogRecord r = readAt(pos);
            if (r == null || r.offset() > targetOffset) {
                return null;
            }
            if (r.offset() == targetOffset) {
                return r;
            }
            pos += r.sizeInBytes();
        }
    }

    /** Reads the record that starts at this byte position, or null if we are at the end of the file. */
    public LogRecord readAt(long position) throws IOException {
        long size = channel.size();
        if (position >= size) {
            return null;
        }

        ByteBuffer lenBuf = ByteBuffer.allocate(4);
        readFully(lenBuf, position);
        int length = lenBuf.getInt(0);

        if (length < LogRecord.HEADER_SIZE - 4 || position + 4 + length > size) {
            throw new CorruptRecordException("bad length " + length + " at position " + position);
        }

        ByteBuffer buf = ByteBuffer.allocate(4 + length);
        readFully(buf, position);
        buf.flip();
        return LogRecord.decode(buf);
    }

    /** What {@link #lengthOfWholeRecords} found: how many bytes, how many records, and the last offset. */
    record Extent(int length, int recordCount, long lastOffset) {
    }

    /**
     * Byte position where the record with exactly this offset starts. The offset must be inside
     * this segment. Jumps with the sparse index, then hops over records using their length prefix.
     */
    long positionOf(long targetOffset) throws IOException {
        if (targetOffset < baseOffset || targetOffset >= nextOffset) {
            throw new IllegalArgumentException("offset " + targetOffset + " is not in segment "
                    + baseOffset + ".." + nextOffset);
        }
        long pos = index.lookup(targetOffset);
        while (true) {
            ByteBuffer header = readHeader(pos);
            long offset = header.getLong(8);
            if (offset == targetOffset) {
                return pos;
            }
            if (offset > targetOffset) {
                throw new CorruptRecordException("offset " + targetOffset + " missing at position " + pos);
            }
            pos += 4 + header.getInt(0);
        }
    }

    /**
     * Starting at a record boundary, extends over whole records while the total stays within
     * maxBytes. The first record is always included, even if it alone is bigger than maxBytes,
     * so a consumer can never get stuck. Stops at the end of this segment. Only headers are read.
     */
    Extent lengthOfWholeRecords(long startPosition, int maxBytes) throws IOException {
        long size = channel.size();
        long pos = startPosition;
        int count = 0;
        long lastOffset = -1;
        while (pos < size) {
            ByteBuffer header = readHeader(pos);
            long recordSize = 4L + header.getInt(0);
            if (count > 0 && (pos - startPosition) + recordSize > maxBytes) {
                break;
            }
            lastOffset = header.getLong(8);
            pos += recordSize;
            count++;
        }
        return new Extent((int) (pos - startPosition), count, lastOffset);
    }

    /** A private read-only descriptor on the .log file, owned (and closed) by the caller. */
    FileChannel openReadOnlyChannel() throws IOException {
        return FileChannel.open(file, READ);
    }

    /** Reads length(4) crc(4) offset(8) at this position, without verifying anything. */
    private ByteBuffer readHeader(long position) throws IOException {
        ByteBuffer header = ByteBuffer.allocate(16);
        readFully(header, position);
        int length = header.getInt(0);
        if (length < LogRecord.HEADER_SIZE - 4 || position + 4 + length > channel.size()) {
            throw new CorruptRecordException("bad length " + length + " at position " + position);
        }
        return header;
    }

    /** Asks the OS to push written bytes to the physical disk (fsync). */
    public void flush() throws IOException {
        channel.force(false);
        index.flush();
    }

    /** Closes the segment and removes its .log and .index files from disk. */
    public void delete() throws IOException {
        close();
        Files.deleteIfExists(file);
        Files.deleteIfExists(file.resolveSibling(String.format("%020d.index", baseOffset)));
    }

    public long baseOffset() {
        return baseOffset;
    }

    public long nextOffset() {
        return nextOffset;
    }

    /** Newest record timestamp in this segment, or -1 if it has no records. */
    public long maxTimestamp() {
        return maxTimestamp;
    }

    public long sizeInBytes() throws IOException {
        return channel.size();
    }

    public long recoveredTruncatedBytes() {
        return recoveredTruncatedBytes;
    }

    public Path file() {
        return file;
    }

    public OffsetIndex index() {
        return index;
    }

    @Override
    public void close() throws IOException {
        index.close();
        channel.close();
    }

    private void readFully(ByteBuffer buf, long position) throws IOException {
        long pos = position;
        while (buf.hasRemaining()) {
            int n = channel.read(buf, pos);
            if (n < 0) {
                throw new CorruptRecordException("unexpected end of file at position " + pos);
            }
            pos += n;
        }
    }
}