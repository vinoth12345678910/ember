package com.ember.client;

import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class LatencyHistogramTest {

    private static void assertWithin(double expected, long actual, double relative, String what) {
        assertTrue(Math.abs(actual - expected) <= Math.max(1, expected * relative), what + ": expected about " + expected + " but got " + actual);
    }

    @Test
    void anEmptyHistogramReportsZero() {
        LatencyHistogram h = new LatencyHistogram();
        assertEquals(0, h.count());
        assertEquals(0, h.percentile(0.5));
        assertEquals(0, h.max());
    }

    @Test
    void smallValuesAreExact() {
        LatencyHistogram h = new LatencyHistogram();
        for (int v = 0; v < 32; v++) h.record(v);
        assertEquals(32, h.count());
        assertEquals(15, h.percentile(0.5), "the 16th of 0..31");
        assertEquals(31, h.percentile(1.0));
        assertEquals(31, h.max());
    }

    @Test
    void percentilesOfAUniformDistributionAreWithinAboutThreePercent() {
        LatencyHistogram h = new LatencyHistogram();
        for (long v = 1; v <= 100_000; v++) h.record(v);
        assertWithin(50_000, h.percentile(0.50), 0.032, "p50");
        assertWithin(90_000, h.percentile(0.90), 0.032, "p90");
        assertWithin(99_000, h.percentile(0.99), 0.032, "p99");
        assertWithin(99_900, h.percentile(0.999), 0.032, "p99.9");
        assertEquals(100_000, h.percentile(1.0), "the maximum is exact: it never reports above the largest value");
        assertEquals(100_000, h.max());
    }

    @Test
    void aSkewedDistributionKeepsItsTail() {
        LatencyHistogram h = new LatencyHistogram();
        Random random = new Random(1);
        long[] sorted = new long[200_000];
        for (int i = 0; i < sorted.length; i++) {
            long v = (long) (200 * Math.exp(random.nextGaussian() * 1.2));          // log-normal, like real latencies
            sorted[i] = Math.max(1, v);
            h.record(sorted[i]);
        }
        java.util.Arrays.sort(sorted);
        for (double q : new double[]{0.5, 0.9, 0.99, 0.999}) {
            long truth = sorted[(int) Math.ceil(q * sorted.length) - 1];
            assertWithin(truth, h.percentile(q), 0.035, "q=" + q);
        }
    }

    @Test
    void hugeValuesStayWithinTheBucketResolution() {
        LatencyHistogram h = new LatencyHistogram();
        long hour = 3_600_000_000L;                                                // an hour in microseconds
        h.record(hour);
        assertWithin(hour, h.percentile(1.0), 0.032, "one hour");
        assertEquals(hour, h.max());
    }

    @Test
    void mergingTwoHistogramsEqualsRecordingEverythingInOne() {
        LatencyHistogram a = new LatencyHistogram();
        LatencyHistogram b = new LatencyHistogram();
        LatencyHistogram both = new LatencyHistogram();
        Random random = new Random(7);
        for (int i = 0; i < 50_000; i++) {
            long v = 1 + random.nextInt(1_000_000);
            (i % 2 == 0 ? a : b).record(v);
            both.record(v);
        }
        a.merge(b);
        assertEquals(both.count(), a.count());
        assertEquals(both.max(), a.max());
        for (double q : new double[]{0.1, 0.5, 0.9, 0.99, 0.999}) assertEquals(both.percentile(q), a.percentile(q));
    }

    @Test
    void percentilesNeverDecrease() {
        LatencyHistogram h = new LatencyHistogram();
        Random random = new Random(3);
        for (int i = 0; i < 10_000; i++) h.record(random.nextInt(5_000_000));
        long previous = 0;
        for (double q = 0.01; q <= 1.0; q += 0.01) {
            long v = h.percentile(q);
            assertTrue(v >= previous, "q=" + q);
            previous = v;
        }
    }

    @Test
    void everyBucketBoundIsConsistentWithItsIndex() {
        for (long v : new long[]{0, 1, 31, 32, 33, 63, 64, 1000, 65_535, 1_000_000, 123_456_789L}) {
            int index = LatencyHistogram.indexOf(v);
            assertTrue(LatencyHistogram.upperBoundOf(index) >= v, "value " + v + " must be <= the upper bound of its own bucket");
            if (index > 0) assertTrue(LatencyHistogram.upperBoundOf(index - 1) < v, "value " + v + " must be above the previous bucket");
        }
    }
}
