package com.ember.protocol;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class PartitionerTest {

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void murmur2MatchesKafkasOwnTestVectors() {
        assertEquals(-973932308, Partitioner.murmur2(b("21")));
        assertEquals(-790332482, Partitioner.murmur2(b("foobar")));
        assertEquals(-985981536, Partitioner.murmur2(b("a-little-bit-long-string")));
        assertEquals(-1486304829, Partitioner.murmur2(b("a-little-bit-longer-string")));
        assertEquals(-58897971, Partitioner.murmur2(b("lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8")));
        assertEquals(479470107, Partitioner.murmur2(new byte[]{'a', 'b', 'c'}));
    }

    @Test
    void emptyKeyHasAStableHash() {
        assertEquals(Partitioner.murmur2(new byte[0]), Partitioner.murmur2(new byte[0]));
        int p = new Partitioner().partition(new byte[0], 5);
        assertTrue(p >= 0 && p < 5);
    }

    @Test
    void sameKeyAlwaysGoesToTheSamePartition() {
        Partitioner partitioner = new Partitioner();
        int first = partitioner.partition(b("user-42"), 7);
        for (int i = 0; i < 100; i++) {
            assertEquals(first, partitioner.partition(b("user-42"), 7));
            assertEquals(first, new Partitioner().partition(b("user-42"), 7));   // another instance agrees
        }
    }

    @Test
    void resultIsAlwaysInRangeEvenWhenTheHashIsNegative() {
        Partitioner partitioner = new Partitioner();
        assertTrue(Partitioner.murmur2(b("foobar")) < 0, "this key's hash is negative");
        Random random = new Random(1);
        for (int n = 1; n <= 16; n++) {
            for (int i = 0; i < 500; i++) {
                byte[] key = new byte[random.nextInt(40)];
                random.nextBytes(key);
                int p = partitioner.partition(key, n);
                assertTrue(p >= 0 && p < n, "p=" + p + " n=" + n);
            }
        }
    }

    @Test
    void nullKeysRoundRobinExactly() {
        Partitioner partitioner = new Partitioner();
        for (int round = 0; round < 3; round++) {
            for (int expected = 0; expected < 4; expected++) {
                assertEquals(expected, partitioner.partition(null, 4));
            }
        }
    }

    @Test
    void theRoundRobinCounterIsInjectable() {
        int[] counter = {Integer.MAX_VALUE};                       // overflow into negatives must still be in range
        Partitioner partitioner = new Partitioner(() -> counter[0]++);
        for (int i = 0; i < 10; i++) {
            int p = partitioner.partition(null, 3);
            assertTrue(p >= 0 && p < 3);
        }
        assertEquals(1, new Partitioner(() -> 7).partition(null, 3));
    }

    @Test
    void aSinglePartitionTakesEverything() {
        Partitioner partitioner = new Partitioner();
        assertEquals(0, partitioner.partition(b("anything"), 1));
        assertEquals(0, partitioner.partition(null, 1));
    }

    @Test
    void zeroPartitionsIsAProgrammingError() {
        assertThrows(IllegalArgumentException.class, () -> new Partitioner().partition(b("k"), 0));
    }

    @Test
    void randomKeysSpreadEvenlyOverEightPartitions() {
        Partitioner partitioner = new Partitioner();
        Random random = new Random(12345);
        int partitions = 8, keys = 20_000;
        int[] counts = new int[partitions];
        for (int i = 0; i < keys; i++) {
            byte[] key = new byte[16];
            random.nextBytes(key);
            counts[partitioner.partition(key, partitions)]++;
        }
        double expected = keys / (double) partitions;
        for (int p = 0; p < partitions; p++) {
            assertTrue(Math.abs(counts[p] - expected) <= expected * 0.15, "partition " + p + " got " + counts[p]);
        }
    }
}
