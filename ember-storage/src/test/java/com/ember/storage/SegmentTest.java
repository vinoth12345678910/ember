package com.ember.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class SegmentTest {

    @Test
    void appendAndReadBackInOrder(@TempDir Path dir) throws IOException {
        try (Segment seg = new Segment(dir, 0)) {
            assertEquals(0, seg.append(1L, "k1".getBytes(), "v1".getBytes()));
            assertEquals(1, seg.append(2L, "k2".getBytes(), "v2".getBytes()));
            assertEquals(2, seg.append(3L, "k3".getBytes(), "v3".getBytes()));

            long pos = 0;
            for (int i = 0; i < 3; i++) {
                LogRecord r = seg.readAt(pos);
                assertEquals(i, r.offset());
                assertArrayEquals(("v" + (i + 1)).getBytes(), r.value());
                pos += r.sizeInBytes();
            }
            assertNull(seg.readAt(pos));       // end of file
        }
    }

    @Test
    void offsetsStartAtTheBaseOffset(@TempDir Path dir) throws IOException {
        try (Segment seg = new Segment(dir, 5310)) {
            assertEquals(5310, seg.append(1L, null, "a".getBytes()));
            assertEquals(5311, seg.append(2L, null, "b".getBytes()));
            assertEquals(5312, seg.nextOffset());
        }
    }

    @Test
    void fileIsNamedAfterBaseOffset(@TempDir Path dir) throws IOException {
        try (Segment seg = new Segment(dir, 5310)) {
            assertTrue(Files.exists(dir.resolve("00000000000000005310.log")));
        }
    }
    @Test
    void readByOffsetUsesTheSparseIndex(@TempDir Path dir) throws IOException {
        try (Segment seg = new Segment(dir, 0, 100)) {      // tiny interval: an entry every ~100 bytes
            for (int i = 0; i < 1000; i++) {
                seg.append(i, ("k" + i).getBytes(), ("value-" + i).getBytes());
            }

            LogRecord r = seg.read(777);
            assertEquals(777, r.offset());
            assertArrayEquals("value-777".getBytes(), r.value());

            assertNull(seg.read(1000));                     // not written yet
            assertNull(seg.read(-1));

            // sparse: some entries, but far fewer than one per record
            assertTrue(seg.index().entryCount() > 10);
            assertTrue(seg.index().entryCount() < 500);
        }
    }
}