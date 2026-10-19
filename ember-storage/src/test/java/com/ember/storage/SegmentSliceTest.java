package com.ember.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class SegmentSliceTest {

    /** 10 records with 10-byte values: each is 28 + 2 (key) + 10 = 40 bytes. */
    private static Segment filled(Path dir) throws IOException {
        Segment s = new Segment(dir, 100, 80);          // sparse index entry every ~2 records
        for (int i = 0; i < 10; i++) {
            s.append(i, ("k" + i).getBytes(), "0123456789".getBytes());
        }
        return s;
    }

    @Test
    void positionOfFindsEveryRecord(@TempDir Path dir) throws IOException {
        try (Segment s = filled(dir)) {
            for (int i = 0; i < 10; i++) {
                assertEquals(i * 40L, s.positionOf(100 + i));
            }
        }
    }

    @Test
    void positionOfRejectsOffsetsOutsideTheSegment(@TempDir Path dir) throws IOException {
        try (Segment s = filled(dir)) {
            assertThrows(IllegalArgumentException.class, () -> s.positionOf(99));
            assertThrows(IllegalArgumentException.class, () -> s.positionOf(110));
        }
    }

    @Test
    void extentCoversWholeRecordsOnly(@TempDir Path dir) throws IOException {
        try (Segment s = filled(dir)) {
            Segment.Extent e = s.lengthOfWholeRecords(80, 130);   // room for 3 records (120), not 4
            assertEquals(120, e.length());
            assertEquals(3, e.recordCount());
            assertEquals(104, e.lastOffset());
        }
    }

    @Test
    void extentAlwaysIncludesTheFirstRecord(@TempDir Path dir) throws IOException {
        try (Segment s = filled(dir)) {
            Segment.Extent e = s.lengthOfWholeRecords(0, 1);
            assertEquals(40, e.length());
            assertEquals(1, e.recordCount());
            assertEquals(100, e.lastOffset());
        }
    }

    @Test
    void extentStopsAtTheEndOfTheSegment(@TempDir Path dir) throws IOException {
        try (Segment s = filled(dir)) {
            Segment.Extent e = s.lengthOfWholeRecords(320, 10_000);
            assertEquals(80, e.length());
            assertEquals(2, e.recordCount());
            assertEquals(109, e.lastOffset());
        }
    }
}
