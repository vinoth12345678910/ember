package com.ember.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class RetentionTest {

    private static final long SMALL_SEGMENT = 200;      // a new segment every ~5 records

    /** 100 records; record i has timestamp i * 1000 (so record 99 is at 99_000 ms). */
    private static void fill(Log log) throws IOException {
        for (int i = 0; i < 100; i++) {
            log.append(i * 1000L, ("k" + i).getBytes(), ("value-" + i).getBytes());
        }
    }

    @Test
    void sizeRuleDeletesOldestSegmentsFirst(@TempDir Path dir) throws IOException {
        try (Log log = new Log(dir, SMALL_SEGMENT, 100)) {
            fill(log);
            long limit = log.totalSizeInBytes() / 2;

            int deleted = log.deleteOldSegments(109_000, Long.MAX_VALUE, limit);

            assertTrue(deleted > 0);
            assertTrue(log.totalSizeInBytes() <= limit);
            assertTrue(log.startOffset() > 0);
            assertNull(log.read(0));                                   // oldest data is gone
            assertNotNull(log.read(log.startOffset()));                // new start is readable
            assertArrayEquals("value-99".getBytes(), log.read(99).value());
            assertFalse(Files.exists(dir.resolve("00000000000000000000.log")));
            assertFalse(Files.exists(dir.resolve("00000000000000000000.index")));
        }
    }

    @Test
    void ageRuleOnlyDeletesSegmentsWhoseNewestRecordIsExpired(@TempDir Path dir) throws IOException {
        try (Log log = new Log(dir, SMALL_SEGMENT, 100)) {
            fill(log);

            // now = 109_000, keep 20_000 ms  ->  anything newer than 89_000 must survive
            int deleted = log.deleteOldSegments(109_000, 20_000, Long.MAX_VALUE);

            assertTrue(deleted > 0);
            assertTrue(log.startOffset() > 0);
            assertTrue(log.startOffset() <= 89);                       // never ate into fresh data
            assertNotNull(log.read(89));
        }
    }

    @Test
    void theActiveSegmentIsNeverDeleted(@TempDir Path dir) throws IOException {
        try (Log log = new Log(dir, SMALL_SEGMENT, 100)) {
            fill(log);

            // extreme limits: everything is "too old" and "too big"
            log.deleteOldSegments(1_000_000, 0, 0);

            assertEquals(1, log.segmentCount());
            assertEquals(100, log.nextOffset());
            assertNotNull(log.read(99));
            assertEquals(100, log.append(100_000, null, "still works".getBytes()));
        }
    }

    @Test
    void nothingIsDeletedWhenWithinLimits(@TempDir Path dir) throws IOException {
        try (Log log = new Log(dir, SMALL_SEGMENT, 100)) {
            fill(log);
            int before = log.segmentCount();

            assertEquals(0, log.deleteOldSegments(99_000, Long.MAX_VALUE, Long.MAX_VALUE));
            assertEquals(before, log.segmentCount());
            assertEquals(0, log.startOffset());
        }
    }

    @Test
    void deletedSegmentsStayGoneAfterRestart(@TempDir Path dir) throws IOException {
        long startAfterDelete;
        try (Log log = new Log(dir, SMALL_SEGMENT, 100)) {
            fill(log);
            log.deleteOldSegments(109_000, 20_000, Long.MAX_VALUE);
            startAfterDelete = log.startOffset();
            assertTrue(startAfterDelete > 0);
        }
        try (Log log = new Log(dir, SMALL_SEGMENT, 100)) {            // must not complain about a "gap"
            assertEquals(startAfterDelete, log.startOffset());
            assertEquals(100, log.nextOffset());
            assertArrayEquals("value-99".getBytes(), log.read(99).value());
        }
    }
}