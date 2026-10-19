package com.ember.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class LogSliceTest {

    private static final int RECORD = 40;               // 28 header + 2 key + 10 value
    private static final long SEGMENT_BYTES = 200;      // 5 records per segment (rolls once >= 200)

    private static byte[] key(int i) {
        return ("k" + i).getBytes();
    }

    private static Log filled(Path dir, int count) throws IOException {
        Log log = new Log(dir, SEGMENT_BYTES, 80);
        for (int i = 0; i < count; i++) {
            log.append(1000 + i, key(i), "0123456789".getBytes());
        }
        return log;
    }

    private static List<LogRecord> decode(LogSlice slice) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(slice.length);
        long pos = slice.position;
        while (buf.hasRemaining()) {
            pos += slice.channel.read(buf, pos);
        }
        buf.flip();
        List<LogRecord> out = new ArrayList<>();
        while (buf.hasRemaining()) {
            out.add(LogRecord.decode(buf));
        }
        return out;
    }

    @Test
    void sliceOnTheFirstRecord(@TempDir Path dir) throws IOException {
        try (Log log = filled(dir, 5); LogSlice s = log.openSlice(0, 3 * RECORD)) {
            assertEquals(0, s.position);
            assertEquals(3 * RECORD, s.length);
            assertEquals(3, s.recordCount);
            assertEquals(0, s.firstOffset);
            assertEquals(2, s.lastOffset);
            assertEquals(0, s.logStartOffset);
            assertEquals(5, s.logEndOffset);
            assertEquals(List.of(0L, 1L, 2L), decode(s).stream().map(LogRecord::offset).toList());
        }
    }

    @Test
    void sliceInTheMiddle(@TempDir Path dir) throws IOException {
        try (Log log = filled(dir, 5); LogSlice s = log.openSlice(2, 10_000)) {
            assertEquals(2 * RECORD, s.position);
            assertEquals(3, s.recordCount);
            assertEquals(4, s.lastOffset);
            assertArrayEquals(key(2), decode(s).get(0).key());
        }
    }

    @Test
    void sliceExactlyAtTheEndIsEmpty(@TempDir Path dir) throws IOException {
        try (Log log = filled(dir, 7); LogSlice s = log.openSlice(7, 1000)) {
            assertNull(s.channel);
            assertEquals(0, s.length);
            assertEquals(0, s.recordCount);
            assertEquals(7, s.logEndOffset);
        }
    }

    @Test
    void oneRecordBiggerThanMaxBytesIsStillReturned(@TempDir Path dir) throws IOException {
        try (Log log = filled(dir, 5); LogSlice s = log.openSlice(1, 5)) {
            assertEquals(RECORD, s.length);
            assertEquals(1, s.recordCount);
            assertEquals(1, s.lastOffset);
        }
    }

    @Test
    void sliceStopsAtTheSegmentBoundary(@TempDir Path dir) throws IOException {
        try (Log log = filled(dir, 12)) {
            assertEquals(List.of(0L, 5L, 10L), log.segmentBaseOffsets());
            try (LogSlice s = log.openSlice(3, 1_000_000)) {
                assertEquals(2, s.recordCount);          // offsets 3 and 4, then the segment ends
                assertEquals(4, s.lastOffset);
            }
            try (LogSlice s = log.openSlice(5, 1_000_000)) {
                assertEquals(5, s.recordCount);
                assertEquals(9, s.lastOffset);
                assertEquals(List.of(5L, 6L, 7L, 8L, 9L), decode(s).stream().map(LogRecord::offset).toList());
            }
        }
    }

    @Test
    void outOfRangeOnBothSides(@TempDir Path dir) throws IOException {
        try (Log log = filled(dir, 5)) {
            assertThrows(OffsetOutOfRangeException.class, () -> log.openSlice(-1, 100));
            OffsetOutOfRangeException tooBig =
                    assertThrows(OffsetOutOfRangeException.class, () -> log.openSlice(6, 100));
            assertTrue(tooBig.getMessage().contains("[0, 5]"), tooBig.getMessage());
        }
    }

    @Test
    void outOfRangeBelowStartAfterRetention(@TempDir Path dir) throws IOException {
        try (Log log = filled(dir, 12)) {
            assertEquals(2, log.deleteOldSegments(1_000_000, 0, Long.MAX_VALUE));
            assertEquals(10, log.startOffset());
            assertThrows(OffsetOutOfRangeException.class, () -> log.openSlice(9, 100));
            try (LogSlice s = log.openSlice(10, 100)) {
                assertEquals(2, s.recordCount);
            }
        }
    }

    @Test
    void channelStillReadableAfterRetentionDeletesTheFile(@TempDir Path dir) throws IOException {
        try (Log log = filled(dir, 12)) {
            LogSlice s = log.openSlice(0, 1_000_000);
            assertEquals(5, s.recordCount);
            assertEquals(2, log.deleteOldSegments(1_000_000, 0, Long.MAX_VALUE));
            assertFalse(dir.resolve(String.format("%020d.log", 0)).toFile().exists());
            List<LogRecord> records = decode(s);          // the open descriptor still works
            assertEquals(5, records.size());
            assertEquals(0, records.get(0).offset());
            s.close();
            s.close();                                    // twice is fine
        }
    }

    @Test
    void appendAllGivesContiguousOffsets(@TempDir Path dir) throws IOException {
        try (Log log = new Log(dir, 200, 80)) {
            log.append(1, key(0), "x".getBytes());
            long base = log.appendAll(2, List.of(
                    new KeyValue(null, "a".getBytes()),
                    new KeyValue("k".getBytes(), new byte[0]),
                    new KeyValue(null, "c".getBytes())));
            assertEquals(1, base);
            assertEquals(4, log.nextOffset());
            assertNull(log.read(1).key());
            assertEquals(0, log.read(2).value().length);
            assertEquals(2, log.read(3).timestamp());
        }
    }

    @Test
    void appendAllCountsTowardsTheFlushPolicy(@TempDir Path dir) throws IOException {
        try (Log log = new Log(dir, 10_000, 80, FlushPolicy.afterMessages(3))) {
            log.appendAll(1, List.of(new KeyValue(null, "a".getBytes()), new KeyValue(null, "b".getBytes())));
            assertEquals(0, log.flushCount());
            assertEquals(2, log.unflushedMessages());
            log.appendAll(1, List.of(new KeyValue(null, "c".getBytes())));
            assertEquals(1, log.flushCount());
            assertEquals(0, log.unflushedMessages());
        }
    }

    @Test
    void appendAllRejectsAnEmptyBatch(@TempDir Path dir) throws IOException {
        try (Log log = new Log(dir, 200, 80)) {
            assertThrows(IllegalArgumentException.class, () -> log.appendAll(1, List.of()));
        }
    }
}
