package com.ember.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.junit.jupiter.api.Assertions.*;

class SegmentRecoveryTest {

    private static Path logFile(Path dir) {
        return dir.resolve("00000000000000000000.log");
    }

    private static void chop(Path file, long bytes) throws IOException {
        try (FileChannel ch = FileChannel.open(file, WRITE)) {
            ch.truncate(ch.size() - bytes);
        }
    }

    private static void flipByte(Path file, long position) throws IOException {
        try (FileChannel ch = FileChannel.open(file, READ, WRITE)) {
            ByteBuffer b = ByteBuffer.allocate(1);
            ch.read(b, position);
            byte original = b.get(0);
            b.clear();
            b.put((byte) (original ^ 0xFF));
            b.flip();
            ch.write(b, position);
        }
    }

    private static void write10(Segment seg) throws IOException {
        for (int i = 0; i < 10; i++) {
            seg.append(i, ("k" + i).getBytes(), ("value-" + i).getBytes());
        }
    }

    @Test
    void cleanReopenKeepsEverything(@TempDir Path dir) throws IOException {
        try (Segment seg = new Segment(dir, 0)) {
            for (int i = 0; i < 50; i++) {
                seg.append(i, null, ("value-" + i).getBytes());
            }
        }
        try (Segment seg = new Segment(dir, 0)) {
            assertEquals(0, seg.recoveredTruncatedBytes());
            assertEquals(50, seg.nextOffset());
            assertArrayEquals("value-30".getBytes(), seg.read(30).value());
            assertEquals(50, seg.append(50, null, "next".getBytes()));
        }
    }

    @Test
    void halfWrittenTailIsCutOff(@TempDir Path dir) throws IOException {
        try (Segment seg = new Segment(dir, 0)) {
            write10(seg);
        }
        chop(logFile(dir), 5);                               // last record loses its final 5 bytes

        try (Segment seg = new Segment(dir, 0)) {
            assertTrue(seg.recoveredTruncatedBytes() > 0);
            assertEquals(9, seg.nextOffset());
            assertArrayEquals("value-8".getBytes(), seg.read(8).value());
            assertNull(seg.read(9));
            assertEquals(9, seg.append(9, null, "again".getBytes()));
            assertArrayEquals("again".getBytes(), seg.read(9).value());
        }
    }

    @Test
    void corruptRecordIsCutOffTogetherWithEverythingAfterIt(@TempDir Path dir) throws IOException {
        long posOfRecord5 = 0;
        try (Segment seg = new Segment(dir, 0)) {
            write10(seg);
            for (int i = 0; i < 5; i++) {
                posOfRecord5 += seg.readAt(posOfRecord5).sizeInBytes();
            }
        }
        flipByte(logFile(dir), posOfRecord5 + 30);           // a byte inside record 5's value

        try (Segment seg = new Segment(dir, 0)) {
            assertEquals(5, seg.nextOffset());               // records 0..4 survive
            assertArrayEquals("value-4".getBytes(), seg.read(4).value());
            assertNull(seg.read(5));
            assertNull(seg.read(9));                         // valid records after the damage are dropped too
        }
    }

    @Test
    void indexIsRebuiltWhenItIsMissing(@TempDir Path dir) throws IOException {
        try (Segment seg = new Segment(dir, 0, 100)) {
            for (int i = 0; i < 1000; i++) {
                seg.append(i, ("k" + i).getBytes(), ("value-" + i).getBytes());
            }
        }
        Files.delete(dir.resolve("00000000000000000000.index"));

        try (Segment seg = new Segment(dir, 0, 100)) {
            assertTrue(seg.index().entryCount() > 10);
            assertArrayEquals("value-777".getBytes(), seg.read(777).value());
        }
    }
}