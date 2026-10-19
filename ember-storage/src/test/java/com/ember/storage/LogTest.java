package com.ember.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assertions.*;

class LogTest {

    // ~40 bytes per record and a 200-byte limit: a new segment every ~5 records
    private static final long SMALL_SEGMENT = 200;

    @Test
    void offsetsKeepCountingAcrossRolls(@TempDir Path dir) throws IOException {
        try (Log log = new Log(dir, SMALL_SEGMENT, 100)) {
            for (int i = 0; i < 100; i++) {
                assertEquals(i, log.append(i, ("k" + i).getBytes(), ("value-" + i).getBytes()));
            }
            assertEquals(100, log.nextOffset());
            assertTrue(log.segmentCount() > 1, "log should have rolled into several segments");
        }
    }

    @Test
    void anyOffsetIsReadableFromAnySegment(@TempDir Path dir) throws IOException {
        try (Log log = new Log(dir, SMALL_SEGMENT, 100)) {
            for (int i = 0; i < 100; i++) {
                log.append(i, ("k" + i).getBytes(), ("value-" + i).getBytes());
            }
            for (int i = 0; i < 100; i++) {
                LogRecord r = log.read(i);
                assertEquals(i, r.offset());
                assertArrayEquals(("value-" + i).getBytes(), r.value());
            }
            assertNull(log.read(100));      // not written yet
            assertNull(log.read(-1));
        }
    }

    @Test
    void segmentsAreNamedAfterTheirFirstOffset(@TempDir Path dir) throws IOException {
        try (Log log = new Log(dir, SMALL_SEGMENT, 100)) {
            for (int i = 0; i < 50; i++) {
                log.append(i, null, ("value-" + i).getBytes());
            }
            List<Long> bases = log.segmentBaseOffsets();
            assertEquals(0L, bases.get(0));
            for (int i = 1; i < bases.size(); i++) {
                assertTrue(bases.get(i) > bases.get(i - 1));
            }
            for (long base : bases) {
                assertTrue(Files.exists(dir.resolve(String.format("%020d.log", base))));
            }
        }
    }

    @Test
    private static void chop(Path file, long bytes) throws IOException {
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.WRITE)) {
            ch.truncate(ch.size() - bytes);
        }
    }

    @Test
    void reopeningRestoresEverything(@TempDir Path dir) throws IOException {
        int segmentsBefore;
        try (Log log = new Log(dir, SMALL_SEGMENT, 100)) {
            for (int i = 0; i < 100; i++) {
                log.append(i, ("k" + i).getBytes(), ("value-" + i).getBytes());
            }
            segmentsBefore = log.segmentCount();
        }
        try (Log log = new Log(dir, SMALL_SEGMENT, 100)) {
            assertEquals(100, log.nextOffset());
            assertEquals(segmentsBefore, log.segmentCount());
            assertArrayEquals("value-77".getBytes(), log.read(77).value());
            assertEquals(100, log.append(100, null, "after-restart".getBytes()));
        }
    }

    @Test
    void halfWrittenTailOfTheActiveSegmentIsCutOff(@TempDir Path dir) throws IOException {
        long lastBase;
        try (Log log = new Log(dir, SMALL_SEGMENT, 100)) {
            for (int i = 0; i < 100; i++) {
                log.append(i, ("k" + i).getBytes(), ("value-" + i).getBytes());
            }
            List<Long> bases = log.segmentBaseOffsets();
            lastBase = bases.get(bases.size() - 1);
        }
        chop(dir.resolve(String.format("%020d.log", lastBase)), 3);   // simulate a crash mid-write

        try (Log log = new Log(dir, SMALL_SEGMENT, 100)) {
            assertEquals(99, log.nextOffset());             // record 99 was torn, so it is gone
            assertNull(log.read(99));
            assertArrayEquals("value-98".getBytes(), log.read(98).value());
            assertEquals(99, log.append(99, null, "new-99".getBytes()));
            assertArrayEquals("new-99".getBytes(), log.read(99).value());
        }
    }

    @Test
    void damageInASealedSegmentFailsLoudly(@TempDir Path dir) throws IOException {
        try (Log log = new Log(dir, SMALL_SEGMENT, 100)) {
            for (int i = 0; i < 100; i++) {
                log.append(i, ("k" + i).getBytes(), ("value-" + i).getBytes());
            }
        }
        chop(dir.resolve(String.format("%020d.log", 0)), 3);          // damage the FIRST segment

        assertThrows(IllegalStateException.class, () -> new Log(dir, SMALL_SEGMENT, 100));
    }
}