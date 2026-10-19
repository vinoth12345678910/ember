package com.ember.broker;

import com.ember.storage.Log;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** The topic -> partition -> Log model, without any sockets. */
class LogManagerTest {

    private static BrokerConfig config(Path dir, String... extra) {
        String[] flags = new String[extra.length + 1];
        flags[0] = "--data-dir=" + dir;
        System.arraycopy(extra, 0, flags, 1, extra.length);
        return BrokerConfig.parse(flags);
    }

    private static List<String> names(LogManager m) {
        return m.topics().stream().map(Topic::name).toList();
    }

    private static Log log(LogManager m, String topic, int partition) {
        return m.logFor(topic, partition);
    }

    // ---- creation

    @Test
    void createTopicMakesIndependentPartitions(@TempDir Path dir) throws IOException {
        try (LogManager m = new LogManager(config(dir))) {
            Topic t = m.createTopic("orders", 4);
            assertEquals(4, t.partitionCount());
            log(m, "orders", 0).append(1, null, "a".getBytes());
            log(m, "orders", 3).append(1, null, "b".getBytes());
            assertEquals(1, log(m, "orders", 0).nextOffset());
            assertEquals(0, log(m, "orders", 1).nextOffset());
            assertEquals(1, log(m, "orders", 3).nextOffset());
            for (int p = 0; p < 4; p++) {
                assertTrue(Files.isDirectory(dir.resolve("orders-" + p)));
            }
            assertTrue(Files.exists(dir.resolve("_meta").resolve("orders.properties")));
        }
    }

    @Test
    void duplicateCreateIsRejectedEvenWithADifferentCount(@TempDir Path dir) throws IOException {
        try (LogManager m = new LogManager(config(dir))) {
            m.createTopic("t", 2);
            assertThrows(TopicAlreadyExistsException.class, () -> m.createTopic("t", 2));
            assertThrows(TopicAlreadyExistsException.class, () -> m.createTopic("t", 5));
            assertEquals(2, m.topic("t").partitionCount());
        }
    }

    @Test
    void createValidatesNameAndCount(@TempDir Path dir) throws IOException {
        try (LogManager m = new LogManager(config(dir, "--max-partitions=8", "--default-partitions=2"))) {
            assertThrows(IllegalArgumentException.class, () -> m.createTopic("", 1));
            assertThrows(IllegalArgumentException.class, () -> m.createTopic("a/b", 1));
            assertThrows(IllegalArgumentException.class, () -> m.createTopic("..", 1));
            assertThrows(IllegalArgumentException.class, () -> m.createTopic("x".repeat(129), 1));
            assertThrows(IllegalArgumentException.class, () -> m.createTopic("t", 0));
            assertThrows(IllegalArgumentException.class, () -> m.createTopic("t", 9));
            assertEquals(8, m.createTopic("t", 8).partitionCount());
            assertEquals(List.of("t"), names(m));
        }
    }

    @Test
    void sixteenThreadsCreatingTheSameTopicProduceExactlyOneWinner(@TempDir Path dir) throws Exception {
        try (LogManager m = new LogManager(config(dir))) {
            ExecutorService pool = Executors.newFixedThreadPool(16);
            CountDownLatch go = new CountDownLatch(1);
            AtomicInteger created = new AtomicInteger(), rejected = new AtomicInteger();
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < 16; i++) {
                final int count = 1 + i % 4;                       // threads even disagree about the count
                futures.add(pool.submit(() -> {
                    go.await();
                    try {
                        m.createTopic("race", count);
                        created.incrementAndGet();
                    } catch (TopicAlreadyExistsException e) {
                        rejected.incrementAndGet();
                    }
                    return null;
                }));
            }
            go.countDown();
            for (Future<?> f : futures) {
                f.get(30, TimeUnit.SECONDS);
            }
            pool.shutdown();
            assertEquals(1, created.get());
            assertEquals(15, rejected.get());
            // the persisted count is the winner's count, and the in-memory one agrees
            Path meta = dir.resolve("_meta").resolve("race.properties");
            assertEquals("partitions=" + m.topic("race").partitionCount() + "\n", Files.readString(meta));
        }
    }

    @Test
    void autoCreateRacingAnExplicitCreateNeverCreatesTwiceOrLosesEither(@TempDir Path dir) throws Exception {
        try (LogManager m = new LogManager(config(dir, "--default-partitions=3"))) {
            ExecutorService pool = Executors.newFixedThreadPool(16);
            CountDownLatch go = new CountDownLatch(1);
            AtomicInteger explicitWins = new AtomicInteger();
            List<Future<Log>> futures = new ArrayList<>();
            for (int i = 0; i < 16; i++) {
                final boolean explicit = i % 4 == 0;
                futures.add(pool.submit(() -> {
                    go.await();
                    if (explicit) {
                        try {
                            m.createTopic("mixed", 3);
                            explicitWins.incrementAndGet();
                        } catch (TopicAlreadyExistsException e) {
                            // an auto-create got there first: fine
                        }
                        return m.logFor("mixed", 0);
                    }
                    return m.getOrAutoCreate("mixed", 0);
                }));
            }
            go.countDown();
            Log first = futures.get(0).get(30, TimeUnit.SECONDS);
            for (Future<Log> f : futures) {
                assertSame(first, f.get(30, TimeUnit.SECONDS));     // one Log instance for partition 0, everybody got it
            }
            pool.shutdown();
            assertEquals(3, m.topic("mixed").partitionCount());
            assertTrue(explicitWins.get() <= 1);
        }
    }

    // ---- auto-create

    @Test
    void autoCreateUsesTheDefaultPartitionCount(@TempDir Path dir) throws IOException {
        try (LogManager m = new LogManager(config(dir, "--default-partitions=3"))) {
            assertNotNull(m.getOrAutoCreate("auto", 1));
            assertEquals(3, m.topic("auto").partitionCount());
            assertNotNull(m.getOrAutoCreate("auto", 2));
            assertNull(m.getOrAutoCreate("auto", 3));               // beyond the topic's count
            assertNull(m.getOrAutoCreate("auto", -1));
        }
    }

    @Test
    void autoCreateRefusesPartitionsBeyondTheDefault(@TempDir Path dir) throws IOException {
        try (LogManager m = new LogManager(config(dir, "--default-partitions=3"))) {
            assertNull(m.getOrAutoCreate("nope", 3));
            assertNull(m.getOrAutoCreate("nope", 7));
            assertNull(m.topic("nope"));                            // and it did not create anything
        }
    }

    @Test
    void autoCreateCanBeSwitchedOff(@TempDir Path dir) throws IOException {
        try (LogManager m = new LogManager(config(dir, "--auto-create-topics=false"))) {
            assertNull(m.getOrAutoCreate("t", 0));
            m.createTopic("t", 1);
            assertNotNull(m.getOrAutoCreate("t", 0));
        }
    }

    // ---- restart and the directory scheme

    @Test
    void restartKeepsPartitionCountsAndData(@TempDir Path dir) throws IOException {
        try (LogManager m = new LogManager(config(dir))) {
            m.createTopic("alpha", 4);
            m.createTopic("beta", 1);
            for (int p = 0; p < 4; p++) {
                log(m, "alpha", p).append(10 + p, null, ("a" + p).getBytes());
            }
            log(m, "beta", 0).append(1, null, "b".getBytes());
        }
        try (LogManager m = new LogManager(config(dir, "--default-partitions=7"))) {
            assertEquals(List.of("alpha", "beta"), names(m));
            assertEquals(4, m.topic("alpha").partitionCount());     // NOT changed by a different default
            assertEquals(1, m.topic("beta").partitionCount());
            for (int p = 0; p < 4; p++) {
                assertEquals("a" + p, new String(log(m, "alpha", p).read(0).value()));
            }
            assertNull(m.topic("gamma"));
        }
    }

    @Test
    void topicNamesWithDashesRoundTripThroughTheDirectoryScheme(@TempDir Path dir) throws IOException {
        try (LogManager m = new LogManager(config(dir))) {
            m.createTopic("my-topic-v2", 3);
            m.createTopic("a-0", 2);                                // looks like "topic a, partition 0" but is a topic
            log(m, "my-topic-v2", 2).append(1, null, "x".getBytes());
            log(m, "a-0", 1).append(1, null, "y".getBytes());
        }
        assertTrue(Files.isDirectory(dir.resolve("my-topic-v2-2")));
        assertTrue(Files.isDirectory(dir.resolve("a-0-1")));
        try (LogManager m = new LogManager(config(dir))) {
            assertEquals(List.of("a-0", "my-topic-v2"), names(m));
            assertEquals(3, m.topic("my-topic-v2").partitionCount());
            assertEquals(2, m.topic("a-0").partitionCount());
            assertEquals("x", new String(log(m, "my-topic-v2", 2).read(0).value()));
            assertEquals("y", new String(log(m, "a-0", 1).read(0).value()));
        }
    }

    @Test
    void directoryNamesAreParsedOnTheLastDash() {
        assertEquals(new LogManager.PartitionDir("t", 0), LogManager.parseDirectoryName("t-0"));
        assertEquals(new LogManager.PartitionDir("a-b-c", 12), LogManager.parseDirectoryName("a-b-c-12"));
        assertEquals(new LogManager.PartitionDir("a-0", 3), LogManager.parseDirectoryName("a-0-3"));
        assertNull(LogManager.parseDirectoryName("t"));
        assertNull(LogManager.parseDirectoryName("t-"));
        assertNull(LogManager.parseDirectoryName("-0"));
        assertNull(LogManager.parseDirectoryName("t-x"));
        assertNull(LogManager.parseDirectoryName("t-1a"));
        assertNull(LogManager.parseDirectoryName("t-99999999999"));
        assertNull(LogManager.parseDirectoryName("bad topic-0"));
    }

    @Test
    void strayDirectoriesAndFilesAreIgnored(@TempDir Path dir) throws IOException {
        Files.createDirectories(dir.resolve("lost+found"));
        Files.createDirectories(dir.resolve("notes-final"));
        Files.createDirectories(dir.resolve(".hidden"));
        Files.writeString(dir.resolve("README.txt"), "hi");
        Files.writeString(dir.resolve("file-3"), "a file named like a partition");
        try (LogManager m = new LogManager(config(dir))) {
            assertEquals(List.of(), names(m));
        }
    }

    // ---- legacy (Phase 2) layout

    @Test
    void aLegacySingleDirectoryBecomesAOnePartitionTopicWithAMetadataFile(@TempDir Path dir) throws IOException {
        try (Log legacy = new Log(dir.resolve("orders-0"), 4096)) {         // exactly what Phase 2 wrote
            legacy.append(5, "k".getBytes(), "old data".getBytes());
        }
        assertFalse(Files.exists(dir.resolve("_meta").resolve("orders.properties")));
        try (LogManager m = new LogManager(config(dir, "--default-partitions=5"))) {
            assertEquals(1, m.topic("orders").partitionCount());
            assertEquals("old data", new String(log(m, "orders", 0).read(0).value()));
        }
        assertEquals("partitions=1\n", Files.readString(dir.resolve("_meta").resolve("orders.properties")));
    }

    @Test
    void contiguousLegacyPartitionsAreAdopted(@TempDir Path dir) throws IOException {
        for (int p = 0; p < 3; p++) {
            try (Log l = new Log(dir.resolve("t-" + p), 4096)) {
                l.append(1, null, ("p" + p).getBytes());
            }
        }
        try (LogManager m = new LogManager(config(dir))) {
            assertEquals(3, m.topic("t").partitionCount());
            assertEquals("p2", new String(log(m, "t", 2).read(0).value()));
        }
    }

    @Test
    void nonContiguousLegacyPartitionsRefuseToStartAndNameTheTopic(@TempDir Path dir) throws IOException {
        Files.createDirectories(dir.resolve("orders-0"));
        Files.createDirectories(dir.resolve("orders-2"));
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> new LogManager(config(dir)));
        assertTrue(e.getMessage().contains("orders"), e.getMessage());
        assertTrue(e.getMessage().contains("contiguous"), e.getMessage());
    }

    @Test
    void aLegacyTopicNotStartingAtZeroAlsoRefuses(@TempDir Path dir) throws IOException {
        Files.createDirectories(dir.resolve("orders-1"));
        assertThrows(IllegalStateException.class, () -> new LogManager(config(dir)));
    }

    // ---- crash recovery of creation

    @Test
    void aCrashDuringCreationIsCompletedAtStartup(@TempDir Path dir) throws IOException {
        // simulate: the metadata file (written FIRST) says 4, but only partitions 0 and 1 got created
        Files.createDirectories(dir.resolve("_meta"));
        Files.writeString(dir.resolve("_meta").resolve("half.properties"), "partitions=4\n");
        try (Log l = new Log(dir.resolve("half-0"), 4096)) {
            l.append(1, null, "kept".getBytes());
        }
        Files.createDirectories(dir.resolve("half-1"));
        try (LogManager m = new LogManager(config(dir, "--default-partitions=2"))) {
            assertEquals(4, m.topic("half").partitionCount());
            assertTrue(Files.isDirectory(dir.resolve("half-2")));
            assertTrue(Files.isDirectory(dir.resolve("half-3")));
            assertEquals("kept", new String(log(m, "half", 0).read(0).value()));
            assertEquals(0, log(m, "half", 3).nextOffset());
        }
    }

    @Test
    void aMetadataFileWithNoDirectoriesAtAllCreatesThemAll(@TempDir Path dir) throws IOException {
        Files.createDirectories(dir.resolve("_meta"));
        Files.writeString(dir.resolve("_meta").resolve("fresh.properties"), "partitions=3\n");
        try (LogManager m = new LogManager(config(dir))) {
            assertEquals(3, m.topic("fresh").partitionCount());
        }
    }

    @Test
    void anExtraDirectoryBeyondTheDeclaredCountIsIgnored(@TempDir Path dir) throws IOException {
        try (LogManager m = new LogManager(config(dir))) {
            m.createTopic("t", 2);
        }
        Files.createDirectories(dir.resolve("t-5"));
        try (LogManager m = new LogManager(config(dir))) {
            assertEquals(2, m.topic("t").partitionCount());
            assertNull(log(m, "t", 5));
        }
    }

    @Test
    void aCorruptMetadataFileRefusesToStart(@TempDir Path dir) throws IOException {
        Files.createDirectories(dir.resolve("_meta"));
        Files.writeString(dir.resolve("_meta").resolve("bad.properties"), "partitions=lots\n");
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> new LogManager(config(dir)));
        assertTrue(e.getMessage().contains("bad"), e.getMessage());
    }

    @Test
    void leftoverTempFilesFromACrashedWriteAreRemoved(@TempDir Path dir) throws IOException {
        Files.createDirectories(dir.resolve("_meta"));
        Path tmp = dir.resolve("_meta").resolve("x.properties.tmp");
        Files.writeString(tmp, "partitions=9\n");
        try (LogManager m = new LogManager(config(dir))) {
            assertEquals(List.of(), names(m));
        }
        assertFalse(Files.exists(tmp));
    }

    // ---- maintenance over every partition

    @Test
    void maintenanceFlushesEveryPartitionOfEveryTopic(@TempDir Path dir) throws IOException {
        try (LogManager m = new LogManager(config(dir, "--flush-ms=100"))) {
            m.createTopic("a", 3);
            m.createTopic("b", 2);
            List<Log> all = new ArrayList<>();
            for (Topic t : m.topics()) {
                all.addAll(t.partitions());
            }
            for (Log l : all) {
                l.append(1000, null, "x".getBytes());
            }
            m.runMaintenance(1050);
            all.forEach(l -> assertEquals(0, l.flushCount()));
            m.runMaintenance(1100);
            for (Log l : all) {
                assertEquals(1, l.flushCount());
                assertEquals(0, l.unflushedMessages());
            }
        }
    }

    @Test
    void retentionAppliesToEveryPartitionOfEveryTopic(@TempDir Path dir) throws IOException {
        try (LogManager m = new LogManager(config(dir, "--segment-bytes=200", "--retention-ms=10000"))) {
            m.createTopic("a", 3);
            m.createTopic("b", 2);
            for (Topic t : m.topics()) {
                for (Log l : t.partitions()) {
                    for (int i = 0; i < 20; i++) {
                        l.append(1000, null, "0123456789".getBytes());
                    }
                    assertEquals(4, l.segmentCount());
                }
            }
            m.runMaintenance(5000);
            m.topics().forEach(t -> t.partitions().forEach(l -> assertEquals(4, l.segmentCount())));
            m.runMaintenance(20_000);
            for (Topic t : m.topics()) {
                for (Log l : t.partitions()) {
                    assertEquals(1, l.segmentCount());
                    assertEquals(18, l.startOffset());
                }
            }
        }
    }

    @Test
    void closeIsIdempotentAndStopsTheScheduler(@TempDir Path dir) throws IOException {
        LogManager m = new LogManager(config(dir, "--maintenance-interval-ms=1000000"));
        m.startMaintenance(new MutableClock(0));
        m.startMaintenance(new MutableClock(0));
        m.close();
        m.close();
    }
}
