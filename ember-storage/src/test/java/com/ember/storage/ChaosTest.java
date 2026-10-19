package com.ember.storage;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.junit.jupiter.api.Assertions.*;

class ChaosTest {

    private static final long SEGMENT_BYTES = 4096;
    private static final int INDEX_INTERVAL = 256;
    private static final int ROUNDS = 40;
    private static final int RECORDS_PER_ROUND = 300;

    @ParameterizedTest
    @ValueSource(longs = {1, 42, 777, 2026, 31337})
    void survivesManyRandomCrashes(long seed, @TempDir Path dir) throws IOException {
        Random rnd = new Random(seed);
        List<byte[]> model = new ArrayList<>();   // model.get(offset) = the value we wrote at that offset
        long clock = 0;
        long lastBase = 0;                        // base offset of the newest segment (the one we damage)
        long appended = 0;
        long lost = 0;

        for (int round = 0; round < ROUNDS; round++) {
            try (Log log = open(dir)) {
                String ctx = "seed=" + seed + " round=" + round;

                // 1. what did the previous crash leave behind?
                assertTrue(log.nextOffset() >= lastBase, ctx + ": data in a sealed segment was lost");
                assertTrue(log.nextOffset() <= model.size(), ctx + ": log contains records we never wrote");
                verifyAgainstModel(log, model, ctx);

                // 2. the crash destroyed the tail: forget those records (their producers would retry)
                lost += model.size() - log.nextOffset();
                while (model.size() > log.nextOffset()) {
                    model.remove(model.size() - 1);
                }

                // 3. keep writing
                for (int i = 0; i < RECORDS_PER_ROUND; i++) {
                    byte[] value = new byte[rnd.nextInt(200)];
                    rnd.nextBytes(value);
                    long offset = model.size();
                    assertEquals(offset, log.append(clock++, ("k" + offset).getBytes(), value), ctx);
                    model.add(value);
                    appended++;
                }

                // 4. every few rounds, trim old data (size rule only)
                if (round % 5 == 4) {
                    log.deleteOldSegments(clock, Long.MAX_VALUE, 20_000);
                }

                List<Long> bases = log.segmentBaseOffsets();
                lastBase = bases.get(bases.size() - 1);
            }
            // 5. the crash itself
            damageTail(rnd, dir, lastBase);
        }

        try (Log log = open(dir)) {                  // one last restart, then check everything again
            verifyAgainstModel(log, model, "seed=" + seed + " final");
        }

        System.out.printf("%n=== chaos seed=%d: %d rounds, %d records written, %d lost to crashes, 0 corrupt reads ===%n",
                seed, ROUNDS, appended, lost);
    }

    private static Log open(Path dir) throws IOException {
        return new Log(dir, SEGMENT_BYTES, INDEX_INTERVAL, FlushPolicy.afterMessages(25));
    }

    /** Every record the log still has must be exactly what we wrote, and nothing else must exist. */
    private static void verifyAgainstModel(Log log, List<byte[]> model, String ctx) throws IOException {
        for (long off = log.startOffset(); off < log.nextOffset(); off++) {
            LogRecord r = log.read(off);
            assertNotNull(r, ctx + ": offset " + off + " is missing");
            assertEquals(off, r.offset(), ctx);
            assertArrayEquals(("k" + off).getBytes(), r.key(), ctx + ": wrong key at " + off);
            assertArrayEquals(model.get((int) off), r.value(), ctx + ": wrong value at " + off);
        }
        assertNull(log.read(log.nextOffset()), ctx + ": record exists past the end");
        if (log.startOffset() > 0) {
            assertNull(log.read(log.startOffset() - 1), ctx + ": deleted record is still readable");
        }
    }

    /** Simulates a crash by damaging the end of the newest segment file in one of four ways. */
    private static void damageTail(Random rnd, Path dir, long lastBase) throws IOException {
        Path file = dir.resolve(String.format("%020d.log", lastBase));
        try (FileChannel ch = FileChannel.open(file, READ, WRITE)) {
            long size = ch.size();
            switch (rnd.nextInt(4)) {
                case 0 -> { /* clean crash: no damage */ }
                case 1 -> {                                           // torn write: last bytes never made it
                    if (size > 0) {
                        ch.truncate(size - Math.min(size, 1 + rnd.nextInt(60)));
                    }
                }
                case 2 -> {                                           // bit rot: flip one byte near the end
                    if (size > 0) {
                        long pos = size - 1 - rnd.nextInt((int) Math.min(size, 400));
                        ByteBuffer one = ByteBuffer.allocate(1);
                        ch.read(one, pos);
                        byte flipped = (byte) (one.get(0) ^ 0xFF);
                        one.clear();
                        one.put(flipped);
                        one.flip();
                        ch.write(one, pos);
                    }
                }
                case 3 -> {                                           // junk after the end (random or zeros)
                    ByteBuffer junk = ByteBuffer.allocate(1 + rnd.nextInt(80));
                    if (rnd.nextBoolean()) {
                        byte[] noise = new byte[junk.capacity()];
                        rnd.nextBytes(noise);
                        junk.put(noise);
                        junk.flip();
                    }
                    long p = size;
                    while (junk.hasRemaining()) {
                        p += ch.write(junk, p);
                    }
                }
            }
        }
    }
}