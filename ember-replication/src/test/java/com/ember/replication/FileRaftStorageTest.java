package com.ember.replication;

import com.ember.raft.EntryType;
import com.ember.raft.InMemoryRaftStorage;
import com.ember.raft.LogEntry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static java.nio.file.StandardOpenOption.WRITE;
import static org.junit.jupiter.api.Assertions.*;

class FileRaftStorageTest {

    private static LogEntry e(long term, String s) {
        return LogEntry.command(term, s.getBytes());
    }

    private static List<LogEntry> readAll(FileRaftStorage s) {
        return s.entries(1, Integer.MAX_VALUE);
    }

    // ---------------------------------------------------------------- basics and persistence

    @Test
    void aNewStorageIsEmpty(@TempDir Path dir) {
        try (FileRaftStorage s = new FileRaftStorage(dir, true)) {
            assertEquals(0, s.lastIndex());
            assertEquals(0, s.lastTerm());
            assertEquals(0, s.termAt(0));
            assertEquals(0, s.currentTerm());
            assertEquals(-1, s.votedFor());
            assertTrue(s.entries(1, 10).isEmpty());
        }
    }

    @Test
    void everythingSyncedSurvivesAReopen(@TempDir Path dir) {
        try (FileRaftStorage s = new FileRaftStorage(dir, true)) {
            s.setTermAndVote(7, 2);
            s.append(List.of(e(1, "a"), LogEntry.noop(3), e(3, "c")));
            s.sync();
        }
        try (FileRaftStorage s = new FileRaftStorage(dir, true)) {
            assertEquals(7, s.currentTerm());
            assertEquals(2, s.votedFor());
            assertEquals(3, s.lastIndex());
            assertEquals(3, s.lastTerm());
            assertEquals(List.of(e(1, "a"), LogEntry.noop(3), e(3, "c")), readAll(s));
            assertEquals(EntryType.NOOP, s.entryAt(2).type());
            assertEquals(1, s.termAt(1));
        }
    }

    @Test
    void truncateThenAppendIsPersistent(@TempDir Path dir) {
        try (FileRaftStorage s = new FileRaftStorage(dir, true)) {
            s.append(List.of(e(1, "a"), e(1, "b"), e(1, "c"), e(1, "d")));
            s.truncateFrom(3);
            s.append(List.of(e(2, "X")));
            s.sync();
        }
        try (FileRaftStorage s = new FileRaftStorage(dir, true)) {
            assertEquals(List.of(e(1, "a"), e(1, "b"), e(2, "X")), readAll(s));
        }
    }

    @Test
    void boundsAreChecked(@TempDir Path dir) {
        try (FileRaftStorage s = new FileRaftStorage(dir, true)) {
            s.append(List.of(e(1, "a")));
            assertThrows(IllegalArgumentException.class, () -> s.termAt(2));
            assertThrows(IllegalArgumentException.class, () -> s.entryAt(0));
            assertThrows(IllegalArgumentException.class, () -> s.truncateFrom(0));
            assertThrows(IllegalArgumentException.class, () -> s.truncateFrom(3));
            s.truncateFrom(2);                                         // just past the end: a no-op
            assertEquals(1, s.lastIndex());
        }
    }

    @Test
    void largeEntriesAndTheReadPathBeyondTheCache(@TempDir Path dir) {
        List<LogEntry> written = new ArrayList<>();
        try (FileRaftStorage s = new FileRaftStorage(dir, false)) {
            for (int i = 0; i < 600; i++) {                               // more than the 256-entry cache
                byte[] payload = new byte[i % 50 == 0 ? 200_000 : 30];
                new Random(i).nextBytes(payload);
                written.add(LogEntry.command(1 + i / 100, payload));
            }
            s.append(written);
            s.sync();
            assertEquals(written, readAll(s));                           // old entries come back through positional reads
        }
        try (FileRaftStorage s = new FileRaftStorage(dir, false)) {
            assertEquals(written, readAll(s));
        }
    }

    @Test
    void theUnsafeModeIsFunctionallyIdentical(@TempDir Path dir) {
        try (FileRaftStorage s = new FileRaftStorage(dir, false)) {
            s.setTermAndVote(3, 1);
            s.append(List.of(e(3, "x")));
            s.sync();
        }
        try (FileRaftStorage s = new FileRaftStorage(dir, false)) {
            assertEquals(3, s.currentTerm());
            assertEquals(List.of(e(3, "x")), readAll(s));
        }
    }

    // ---------------------------------------------------------------- the state file

    @Test
    void termAndVoteAreReplacedAsOne(@TempDir Path dir) throws IOException {
        try (FileRaftStorage s = new FileRaftStorage(dir, true)) {
            s.setTermAndVote(4, 2);
            s.setTermAndVote(5, -1);
            assertEquals(16, Files.size(dir.resolve("state")));
            assertFalse(Files.exists(dir.resolve("state.tmp")), "the temp file was renamed away");
        }
        try (FileRaftStorage s = new FileRaftStorage(dir, true)) {
            assertEquals(5, s.currentTerm());
            assertEquals(-1, s.votedFor());
        }
    }

    @Test
    void aLeftoverTempFileFromACrashedRenameIsIgnoredAndRemoved(@TempDir Path dir) throws IOException {
        try (FileRaftStorage s = new FileRaftStorage(dir, true)) {
            s.setTermAndVote(4, 2);
        }
        Files.write(dir.resolve("state.tmp"), new byte[]{1, 2, 3});          // the crash happened during the next write
        try (FileRaftStorage s = new FileRaftStorage(dir, true)) {
            assertEquals(4, s.currentTerm(), "the old complete state is still the truth");
            assertEquals(2, s.votedFor());
        }
        assertFalse(Files.exists(dir.resolve("state.tmp")));
    }

    @Test
    void aTempFileWithoutAnyStateFileMeansTermZero(@TempDir Path dir) throws IOException {
        Files.createDirectories(dir);
        Files.write(dir.resolve("state.tmp"), new byte[16]);
        try (FileRaftStorage s = new FileRaftStorage(dir, true)) {
            assertEquals(0, s.currentTerm());
            assertEquals(-1, s.votedFor());
        }
    }

    @Test
    void aCorruptStateFileRefusesToOpen(@TempDir Path dir) throws IOException {
        try (FileRaftStorage s = new FileRaftStorage(dir, true)) {
            s.setTermAndVote(4, 2);
        }
        byte[] bytes = Files.readAllBytes(dir.resolve("state"));
        bytes[10] ^= 1;
        Files.write(dir.resolve("state"), bytes);
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> new FileRaftStorage(dir, true));
        assertTrue(e.getMessage().contains("refusing"), e.getMessage());
        Files.write(dir.resolve("state"), new byte[5]);
        assertThrows(IllegalStateException.class, () -> new FileRaftStorage(dir, true));
    }

    // ---------------------------------------------------------------- crash fuzz

    private static List<LogEntry> sampleLog() {
        List<LogEntry> log = new ArrayList<>();
        Random r = new Random(5);
        for (int i = 0; i < 12; i++) {
            byte[] payload = new byte[r.nextInt(40)];
            r.nextBytes(payload);
            log.add(i == 4 ? LogEntry.noop(2) : LogEntry.command(1 + i / 4, payload));
        }
        return log;
    }

    private static void writeSample(Path dir, List<LogEntry> log) {
        try (FileRaftStorage s = new FileRaftStorage(dir, true)) {
            s.append(log);
            s.sync();
        }
    }

    @Test
    void cuttingTheFileAtEveryByteRecoversACleanPrefix(@TempDir Path base) throws IOException {
        List<LogEntry> log = sampleLog();
        Path original = base.resolve("original");
        writeSample(original, log);
        byte[] bytes = Files.readAllBytes(original.resolve("log"));
        for (int cut = 0; cut <= bytes.length; cut++) {
            Path dir = base.resolve("cut-" + cut);
            Files.createDirectories(dir);
            Files.write(dir.resolve("log"), java.util.Arrays.copyOf(bytes, cut));
            try (FileRaftStorage s = new FileRaftStorage(dir, true)) {
                List<LogEntry> got = readAll(s);
                assertTrue(got.size() <= log.size());
                assertEquals(log.subList(0, got.size()), got, "cut at " + cut + " must recover an exact prefix");
                // the file itself was repaired: appending works and indexes continue
                s.append(List.of(e(9, "after")));
                assertEquals(got.size() + 1, s.lastIndex());
                s.sync();
            }
            try (FileRaftStorage s = new FileRaftStorage(dir, true)) {
                assertEquals("after", new String(s.entryAt(s.lastIndex()).command()));
            }
        }
    }

    @Test
    void aSingleFlippedBitNeverYieldsACorruptEntry(@TempDir Path base) throws IOException {
        List<LogEntry> log = sampleLog();
        Path original = base.resolve("original");
        writeSample(original, log);
        byte[] bytes = Files.readAllBytes(original.resolve("log"));
        Random random = new Random(11);
        for (int attempt = 0; attempt < 400; attempt++) {
            byte[] damaged = bytes.clone();
            int where = random.nextInt(damaged.length);
            damaged[where] ^= (byte) (1 << random.nextInt(8));
            Path dir = base.resolve("flip-" + attempt);
            Files.createDirectories(dir);
            Files.write(dir.resolve("log"), damaged);
            try (FileRaftStorage s = new FileRaftStorage(dir, true)) {
                List<LogEntry> got = readAll(s);
                assertEquals(log.subList(0, got.size()), got, "flip at byte " + where + ": only a clean prefix may be returned");
                assertTrue(got.size() < log.size(), "a flipped bit must cost at least the damaged entry");
            }
        }
    }

    @Test
    void garbageAppendedAfterTheLastEntryIsDropped(@TempDir Path dir) throws IOException {
        writeSample(dir, sampleLog());
        try (FileChannel ch = FileChannel.open(dir.resolve("log"), WRITE)) {
            ch.write(java.nio.ByteBuffer.wrap(new byte[]{0, 0, 0, 90, 1, 2, 3}), ch.size());
        }
        try (FileRaftStorage s = new FileRaftStorage(dir, true)) {
            assertEquals(sampleLog(), readAll(s));
        }
    }

    // ---------------------------------------------------------------- model-based test

    @Test
    void aLongRandomSequenceOfOperationsBehavesLikeTheInMemoryModel(@TempDir Path dir) {
        for (long seed = 1; seed <= 25; seed++) {
            Path d = dir.resolve("seed-" + seed);
            Random rnd = new Random(seed);
            InMemoryRaftStorage model = new InMemoryRaftStorage();
            FileRaftStorage real = new FileRaftStorage(d, false);
            long term = 0;
            try {
                for (int step = 0; step < 400; step++) {
                    switch (rnd.nextInt(10)) {
                        case 0, 1, 2, 3 -> {
                            int n = 1 + rnd.nextInt(4);
                            List<LogEntry> batch = new ArrayList<>();
                            for (int i = 0; i < n; i++) {
                                byte[] payload = new byte[rnd.nextInt(25)];
                                rnd.nextBytes(payload);
                                batch.add(rnd.nextInt(8) == 0 ? LogEntry.noop(term) : LogEntry.command(term, payload));
                            }
                            model.append(batch);
                            real.append(batch);
                        }
                        case 4, 5 -> {
                            if (model.lastIndex() > 0) {
                                long from = 1 + rnd.nextInt((int) model.lastIndex());
                                model.truncateFrom(from);
                                real.truncateFrom(from);
                            }
                        }
                        case 6 -> {
                            term += rnd.nextInt(3);
                            int vote = rnd.nextInt(4) - 1;
                            model.setTermAndVote(term, vote);
                            real.setTermAndVote(term, vote);
                        }
                        case 7 -> {
                            model.sync();
                            real.sync();
                        }
                        case 8 -> {                                       // close and reopen after a sync: everything must come back
                            model.sync();
                            real.sync();
                            real.close();
                            real = new FileRaftStorage(d, false);
                        }
                        default -> {                                      // read something random
                            if (model.lastIndex() > 0) {
                                long i = 1 + rnd.nextInt((int) model.lastIndex());
                                assertEquals(model.entryAt(i), real.entryAt(i));
                                assertEquals(model.termAt(i), real.termAt(i));
                                int max = 1 + rnd.nextInt(6);
                                assertEquals(model.entries(i, max), real.entries(i, max));
                            }
                        }
                    }
                    assertEquals(model.lastIndex(), real.lastIndex(), "seed " + seed + " step " + step);
                    assertEquals(model.lastTerm(), real.lastTerm(), "seed " + seed + " step " + step);
                    assertEquals(model.currentTerm(), real.currentTerm());
                    assertEquals(model.votedFor(), real.votedFor());
                }
                assertEquals(model.entries(1, Integer.MAX_VALUE), real.entries(1, Integer.MAX_VALUE), "seed " + seed);
            } finally {
                real.close();
            }
        }
    }
}
