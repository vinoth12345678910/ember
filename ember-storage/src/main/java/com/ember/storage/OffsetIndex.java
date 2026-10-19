package com.ember.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

public class OffsetIndex implements AutoCloseable {

    private static final int ENTRY_SIZE = 8;   // relativeOffset(4) + position(4)

    private final long baseOffset;
    private final FileChannel channel;
    private int[] relOffsets = new int[256];
    private int[] positions = new int[256];
    private int entries = 0;

    public OffsetIndex(Path dir, long baseOffset) throws IOException {
        Files.createDirectories(dir);
        this.baseOffset = baseOffset;
        Path file = dir.resolve(String.format("%020d.index", baseOffset));
        this.channel = FileChannel.open(file, CREATE, READ, WRITE);
        load();
    }

    /** Adds one entry: "the record with this offset starts at this byte position". */
    public void append(long offset, long position) throws IOException {
        int rel = (int) (offset - baseOffset);

        ByteBuffer buf = ByteBuffer.allocate(ENTRY_SIZE);
        buf.putInt(rel);
        buf.putInt((int) position);
        buf.flip();

        long writeAt = channel.size();
        while (buf.hasRemaining()) {
            writeAt += channel.write(buf, writeAt);
        }
        addToMemory(rel, (int) position);
    }

    /** Returns the byte position to start scanning from for this offset. */
    public long lookup(long targetOffset) {
        int rel = (int) (targetOffset - baseOffset);
        int lo = 0, hi = entries - 1, best = -1;

        // binary search: the LARGEST entry whose offset is <= target
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            if (relOffsets[mid] <= rel) {
                best = mid;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        return best < 0 ? 0 : positions[best];
    }

    public int entryCount() {
        return entries;
    }

    public void flush() throws IOException {
        channel.force(false);
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    private void load() throws IOException {
        long size = channel.size();
        ByteBuffer buf = ByteBuffer.allocate((int) size);
        long pos = 0;
        while (buf.hasRemaining()) {
            int n = channel.read(buf, pos);
            if (n < 0) break;
            pos += n;
        }
        buf.flip();
        while (buf.remaining() >= ENTRY_SIZE) {
            addToMemory(buf.getInt(), buf.getInt());
        }
    }

    /** Wipes the index (file and memory). Used when rebuilding it from the log. */
    public void reset() throws IOException {
        channel.truncate(0);
        entries = 0;
    }

    private void addToMemory(int rel, int position) {
        if (entries == relOffsets.length) {
            relOffsets = Arrays.copyOf(relOffsets, entries * 2);
            positions = Arrays.copyOf(positions, entries * 2);
        }
        relOffsets[entries] = rel;
        positions[entries] = position;
        entries++;
    }
}