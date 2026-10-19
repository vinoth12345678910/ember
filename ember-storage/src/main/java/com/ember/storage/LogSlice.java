package com.ember.storage;

import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * A pointer into a segment file: "bytes [position, position+length) of this channel are whole
 * records". The channel is a private read-only descriptor, so closing it never affects the log,
 * and a later retention delete of the file cannot break a fetch that is already in flight.
 * An empty slice has a null channel and length 0.
 */
public final class LogSlice implements AutoCloseable {

    public final FileChannel channel;
    public final long position;
    public final int length;
    public final long firstOffset;
    public final long lastOffset;
    public final int recordCount;
    public final long logStartOffset;
    public final long logEndOffset;

    public LogSlice(FileChannel channel, long position, int length, long firstOffset, long lastOffset,
                    int recordCount, long logStartOffset, long logEndOffset) {
        this.channel = channel;
        this.position = position;
        this.length = length;
        this.firstOffset = firstOffset;
        this.lastOffset = lastOffset;
        this.recordCount = recordCount;
        this.logStartOffset = logStartOffset;
        this.logEndOffset = logEndOffset;
    }

    public static LogSlice empty(long fetchOffset, long logStartOffset, long logEndOffset) {
        return new LogSlice(null, 0, 0, fetchOffset, fetchOffset - 1, 0, logStartOffset, logEndOffset);
    }

    /** Safe to call twice (closing an already closed FileChannel is a no-op). */
    @Override
    public void close() throws IOException {
        if (channel != null) {
            channel.close();
        }
    }
}
