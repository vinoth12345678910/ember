package com.ember.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class OffsetIndexTest {

    @Test
    void lookupFindsTheClosestEarlierEntry(@TempDir Path dir) throws IOException {
        try (OffsetIndex idx = new OffsetIndex(dir, 1000)) {
            idx.append(1000, 0);
            idx.append(1050, 4096);
            idx.append(1100, 8192);

            assertEquals(0, idx.lookup(1000));
            assertEquals(0, idx.lookup(1049));      // before the 2nd entry
            assertEquals(4096, idx.lookup(1050));   // exact hit
            assertEquals(4096, idx.lookup(1099));
            assertEquals(8192, idx.lookup(5000));   // past the last entry
        }
    }

    @Test
    void emptyIndexStartsFromByteZero(@TempDir Path dir) throws IOException {
        try (OffsetIndex idx = new OffsetIndex(dir, 0)) {
            assertEquals(0, idx.lookup(42));
        }
    }


    @Test
    void entriesSurviveReopening(@TempDir Path dir) throws IOException {
        try (OffsetIndex idx = new OffsetIndex(dir, 0)) {
            idx.append(0, 0);
            idx.append(100, 5000);
        }
        try (OffsetIndex reopened = new OffsetIndex(dir, 0)) {
            assertEquals(2, reopened.entryCount());
            assertEquals(5000, reopened.lookup(150));
        }
    }

}