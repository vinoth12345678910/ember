package com.ember.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class FlushPolicyTest {

    private static final long BIG_SEGMENT = 1_000_000;     // no rolling in these tests

    @Test
    void neverPolicyDoesNotFlushOnAppend(@TempDir Path dir) throws IOException {
        try (Log log = new Log(dir, BIG_SEGMENT, 4096, FlushPolicy.never())) {
            for (int i = 0; i < 100; i++) {
                log.append(i, null, "x".getBytes());
            }
            assertEquals(0, log.flushCount());
            assertEquals(100, log.unflushedMessages());
        }
    }

    @Test
    void everyMessagePolicyFlushesEachAppend(@TempDir Path dir) throws IOException {
        try (Log log = new Log(dir, BIG_SEGMENT, 4096, FlushPolicy.everyMessage())) {
            for (int i = 0; i < 10; i++) {
                log.append(i, null, "x".getBytes());
            }
            assertEquals(10, log.flushCount());
            assertEquals(0, log.unflushedMessages());
        }
    }

    @Test
    void afterNMessagesFlushesInBatches(@TempDir Path dir) throws IOException {
        try (Log log = new Log(dir, BIG_SEGMENT, 4096, FlushPolicy.afterMessages(4))) {
            for (int i = 0; i < 10; i++) {
                log.append(i, null, "x".getBytes());
            }
            assertEquals(2, log.flushCount());              // after message 4 and after message 8
            assertEquals(2, log.unflushedMessages());       // messages 9 and 10 are still at risk
        }
    }

    @Test
    void timerPolicyBoundsHowLongARecordStaysUnflushed(@TempDir Path dir) throws IOException {
        try (Log log = new Log(dir, BIG_SEGMENT, 4096, FlushPolicy.afterMillis(1000))) {
            log.append(0, null, "a".getBytes());
            log.append(10, null, "b".getBytes());
            log.append(20, null, "c".getBytes());

            assertFalse(log.maybeFlush(500));               // oldest unflushed waited only 500 ms
            assertEquals(0, log.flushCount());

            assertTrue(log.maybeFlush(1000));               // 1000 ms: time to flush
            assertEquals(1, log.flushCount());
            assertEquals(0, log.unflushedMessages());

            assertFalse(log.maybeFlush(5000));              // nothing unflushed, nothing to do

            log.append(6000, null, "d".getBytes());
            assertFalse(log.maybeFlush(6500));
            assertTrue(log.maybeFlush(7000));
            assertEquals(2, log.flushCount());
        }
    }

    @Test
    void rollingAlwaysFlushesTheSegmentBeingSealed(@TempDir Path dir) throws IOException {
        try (Log log = new Log(dir, 200, 100, FlushPolicy.never())) {
            for (int i = 0; i < 100; i++) {
                log.append(i, ("k" + i).getBytes(), ("value-" + i).getBytes());
            }
            // one fsync per roll, even though the policy says "never"
            assertEquals(log.segmentCount() - 1, log.flushCount());
        }
    }

    @Test
    void printsTheCostOfFsync(@TempDir Path dir) throws IOException {
        long never = millis(dir.resolve("never"), FlushPolicy.never());
        long each = millis(dir.resolve("each"), FlushPolicy.everyMessage());
        long batch = millis(dir.resolve("batch"), FlushPolicy.afterMessages(50));

        System.out.printf("%n=== 300 appends: never=%d ms | every 50 messages=%d ms | every message=%d ms ===%n",
                never, batch, each);
    }

    private static long millis(Path dir, FlushPolicy policy) throws IOException {
        try (Log log = new Log(dir, 10_000_000, 4096, policy)) {
            long start = System.nanoTime();
            for (int i = 0; i < 300; i++) {
                log.append(i, null, ("value-" + i).getBytes());
            }
            return (System.nanoTime() - start) / 1_000_000;
        }
    }
}