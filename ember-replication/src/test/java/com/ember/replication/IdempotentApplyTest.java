package com.ember.replication;

import com.ember.protocol.ProducerRecord;
import com.ember.raft.LogEntry;
import com.ember.storage.FlushPolicy;
import com.ember.storage.Log;
import com.ember.storage.LogRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static java.nio.file.StandardOpenOption.WRITE;
import static org.junit.jupiter.api.Assertions.*;

/** The crash-consistency heart: replaying committed entries onto a data log that already holds some, part, or none of them. */
class IdempotentApplyTest {

    private static Log open(Path dir) throws IOException {
        return new Log(dir, 1024 * 1024, 4096, FlushPolicy.never());
    }

    /** Three batches of 3, 4 and 2 records: values "v0".."v8". */
    private static List<LogEntry> entries() {
        List<LogEntry> out = new ArrayList<>();
        int[] sizes = {3, 4, 2};
        int next = 0;
        for (int b = 0; b < sizes.length; b++) {
            List<ProducerRecord> records = new ArrayList<>();
            for (int i = 0; i < sizes[b]; i++) {
                records.add(new ProducerRecord(("k" + next).getBytes(), ("v" + next).getBytes()));
                next++;
            }
            out.add(LogEntry.command(1, Commands.produceBatch(1000L + b, records)));
        }
        return out;
    }

    private static void replay(PartitionStateMachine sm, List<LogEntry> entries) {
        long index = 1;
        sm.apply(index++, LogEntry.noop(1));
        for (LogEntry e : entries) sm.apply(index++, e);
    }

    private static void assertHoldsExactlyNineRecords(Log log) throws IOException {
        assertEquals(9, log.nextOffset());
        for (int i = 0; i < 9; i++) {
            LogRecord r = log.read(i);
            assertEquals(i, r.offset());
            assertEquals("v" + i, new String(r.value()));
            assertEquals("k" + i, new String(r.key()));
            assertEquals(1000L + (i < 3 ? 0 : i < 7 ? 1 : 2), r.timestamp(), "the leader's timestamp inside the command");
        }
    }

    @Test
    void aFirstApplyWritesEverythingAndReturnsBaseOffsets(@TempDir Path dir) throws IOException {
        try (Log log = open(dir)) {
            PartitionStateMachine sm = new PartitionStateMachine("p", log);
            assertNull(sm.apply(1, LogEntry.noop(1)));
            List<LogEntry> e = entries();
            assertEquals(0L, sm.apply(2, e.get(0)));
            assertEquals(3L, sm.apply(3, e.get(1)));
            assertEquals(7L, sm.apply(4, e.get(2)));
            assertHoldsExactlyNineRecords(log);
            assertEquals(9, sm.runningOffset());
        }
    }

    @Test
    void a_restartWithACompleteDataLogDoesNotDuplicateAnything(@TempDir Path dir) throws IOException {
        try (Log log = open(dir)) {
            replay(new PartitionStateMachine("p", log), entries());
        }
        try (Log log = open(dir)) {                                         // the process restarted: runningOffset is 0 again
            PartitionStateMachine fresh = new PartitionStateMachine("p", log);
            replay(fresh, entries());
            assertHoldsExactlyNineRecords(log);
            assertEquals(9, fresh.runningOffset());
        }
    }

    @Test
    void b_aDataLogCutInTheMiddleOfABatchGetsExactlyTheMissingTailReappended(@TempDir Path dir) throws IOException {
        try (Log log = open(dir)) {
            replay(new PartitionStateMachine("p", log), entries());
            log.flush();
        }
        Path segment;
        try (var files = Files.list(dir)) {
            segment = files.filter(f -> f.toString().endsWith(".log")).findFirst().orElseThrow();
        }
        // find the byte position where record #6 (the 3rd record of the second batch) starts, then cut half-way into it
        long cutAt;
        try (Log log = open(dir)) {
            long pos = 0;
            for (int i = 0; i < 6; i++) pos += log.read(i).sizeInBytes();
            cutAt = pos + log.read(6).sizeInBytes() / 2;
        }
        try (FileChannel ch = FileChannel.open(segment, WRITE)) {
            ch.truncate(cutAt);
        }
        try (Log log = open(dir)) {                                         // Phase 1 recovery cuts the torn record
            assertEquals(6, log.nextOffset(), "records 0..5 survive: batch 1 whole and batch 2's first 3 records");
            PartitionStateMachine sm = new PartitionStateMachine("p", log);
            replay(sm, entries());
            assertHoldsExactlyNineRecords(log);
        }
    }

    @Test
    void b2_everyPossibleCutPointRecoversToTheSameNineRecords(@TempDir Path base) throws IOException {
        Path original = base.resolve("original");
        try (Log log = open(original)) {
            replay(new PartitionStateMachine("p", log), entries());
            log.flush();
        }
        Path segment;
        try (var files = Files.list(original)) {
            segment = files.filter(f -> f.toString().endsWith(".log")).findFirst().orElseThrow();
        }
        byte[] bytes = Files.readAllBytes(segment);
        for (int cut = 0; cut <= bytes.length; cut += 7) {
            Path dir = base.resolve("cut-" + cut);
            Files.createDirectories(dir);
            Files.write(dir.resolve(segment.getFileName().toString()), java.util.Arrays.copyOf(bytes, cut));
            try (Log log = open(dir)) {
                replay(new PartitionStateMachine("p", log), entries());
                assertHoldsExactlyNineRecords(log);
            }
        }
    }

    @Test
    void c_aDeletedDataLogIsRebuiltIdenticallyFromTheRaftLog(@TempDir Path base) throws IOException {
        Path first = base.resolve("first");
        Path second = base.resolve("second");
        try (Log log = open(first)) {
            replay(new PartitionStateMachine("p", log), entries());
            log.flush();
        }
        try (Log log = open(second)) {
            replay(new PartitionStateMachine("p", log), entries());
            log.flush();
        }
        byte[] a = Files.readAllBytes(only(first));
        byte[] b = Files.readAllBytes(only(second));
        assertArrayEquals(a, b, "two replicas applying the same entries hold byte-identical data logs");
        // wipe the data log entirely and rebuild
        try (var files = Files.list(first)) {
            for (Path f : files.toList()) Files.delete(f);
        }
        try (Log log = open(first)) {
            replay(new PartitionStateMachine("p", log), entries());
            log.flush();
        }
        assertArrayEquals(a, Files.readAllBytes(only(first)));
    }

    @Test
    void aDataLogThatLostMoreThanItsTailFailsLoudlyInsteadOfGuessing(@TempDir Path dir) throws IOException {
        try (Log log = open(dir)) {
            PartitionStateMachine sm = new PartitionStateMachine("p", log);
            List<LogEntry> e = entries();
            sm.apply(1, e.get(0));
            sm.apply(2, e.get(1));
            // somebody removes records behind our back: simulate by using a fresh, EMPTY log for the same state machine
        }
        Path other = dir.resolve("other");
        try (Log empty = open(other)) {
            PartitionStateMachine sm = new PartitionStateMachine("p", empty);
            // fabricate the impossible state: running offset ahead of the log
            java.lang.reflect.Field f;
            try {
                f = PartitionStateMachine.class.getDeclaredField("runningOffset");
                f.setAccessible(true);
                f.setLong(sm, 5);
            } catch (ReflectiveOperationException ex) {
                throw new AssertionError(ex);
            }
            IllegalStateException ex = assertThrows(IllegalStateException.class, () -> sm.apply(1, entries().get(0)));
            assertTrue(ex.getMessage().contains("lost more than a torn tail"), ex.getMessage());
        }
    }

    @Test
    void noopsApplyNothing(@TempDir Path dir) throws IOException {
        try (Log log = open(dir)) {
            PartitionStateMachine sm = new PartitionStateMachine("p", log);
            for (int i = 1; i <= 5; i++) assertNull(sm.apply(i, LogEntry.noop(1)));
            assertEquals(0, log.nextOffset());
            assertEquals(0, sm.runningOffset());
        }
    }

    private static Path only(Path dir) throws IOException {
        try (var files = Files.list(dir)) {
            return files.filter(f -> f.toString().endsWith(".log")).findFirst().orElseThrow();
        }
    }
}
