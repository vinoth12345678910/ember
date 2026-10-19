package com.ember.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class LogConcurrencyTest {

    private static final int WRITERS = 8;
    private static final int PER_WRITER = 500;

    @Test
    void concurrentWritersAndReadersKeepTheLogConsistent(@TempDir Path dir) throws Exception {
        try (Log log = new Log(dir, 4096, 512)) {
            ExecutorService pool = Executors.newFixedThreadPool(WRITERS + 4);
            AtomicBoolean writersDone = new AtomicBoolean(false);
            CountDownLatch go = new CountDownLatch(1);

            // writer w appends values "w-i"; even writers use appendAll in batches of 5, odd use append
            List<Future<List<long[]>>> writers = new ArrayList<>();   // {offset, w, i}
            for (int w = 0; w < WRITERS; w++) {
                final int writer = w;
                writers.add(pool.submit(() -> {
                    go.await();
                    List<long[]> seen = new ArrayList<>();
                    if (writer % 2 == 0) {
                        for (int i = 0; i < PER_WRITER; i += 5) {
                            List<KeyValue> batch = new ArrayList<>();
                            for (int j = 0; j < 5; j++) {
                                batch.add(new KeyValue(null, (writer + "-" + (i + j)).getBytes()));
                            }
                            long base = log.appendAll(i, batch);
                            for (int j = 0; j < 5; j++) {
                                seen.add(new long[]{base + j, writer, i + j});
                            }
                        }
                    } else {
                        for (int i = 0; i < PER_WRITER; i++) {
                            long off = log.append(i, null, (writer + "-" + i).getBytes());
                            seen.add(new long[]{off, writer, i});
                        }
                    }
                    return seen;
                }));
            }

            List<Future<?>> readers = new ArrayList<>();
            for (int r = 0; r < 4; r++) {
                final int reader = r;
                readers.add(pool.submit(() -> {
                    go.await();
                    while (!writersDone.get()) {
                        long end = log.nextOffset();
                        if (end == 0) continue;
                        long target = (reader * 7919L + end / 2) % end;
                        if (target >= log.startOffset()) {
                            assertEquals(target, log.read(target).offset());
                        }
                        try (LogSlice slice = log.openSlice(target, 2000)) {
                            assertTrue(slice.recordCount >= 1);
                            ByteBuffer buf = ByteBuffer.allocate(slice.length);
                            long pos = slice.position;
                            while (buf.hasRemaining()) pos += slice.channel.read(buf, pos);
                            buf.flip();
                            long expected = target;
                            while (buf.hasRemaining()) {
                                assertEquals(expected++, LogRecord.decode(buf).offset());
                            }
                        }
                    }
                    return null;
                }));
            }

            go.countDown();
            List<long[]> all = new ArrayList<>();
            for (Future<List<long[]>> f : writers) {
                all.addAll(f.get(60, TimeUnit.SECONDS));
            }
            writersDone.set(true);
            for (Future<?> f : readers) {
                f.get(60, TimeUnit.SECONDS);              // rethrows any assertion error from a reader
            }
            pool.shutdown();

            int total = WRITERS * PER_WRITER;
            assertEquals(total, all.size());
            assertEquals(total, log.nextOffset());
            boolean[] taken = new boolean[total];
            for (long[] e : all) {
                assertFalse(taken[(int) e[0]], "offset handed out twice: " + e[0]);
                taken[(int) e[0]] = true;
                LogRecord rec = log.read(e[0]);
                assertEquals(e[1] + "-" + e[2], new String(rec.value()));
            }
            // per writer, offsets grow with i (each writer's own order is preserved)
            long[] last = new long[WRITERS];
            java.util.Arrays.fill(last, -1);
            all.sort((a, b) -> Long.compare(a[0], b[0]));
            int[] lastI = new int[WRITERS];
            java.util.Arrays.fill(lastI, -1);
            for (long[] e : all) {
                assertTrue(e[2] > lastI[(int) e[1]]);
                lastI[(int) e[1]] = (int) e[2];
            }
        }
    }
}
