package com.ember.client;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/** The fixed-rate scheduler with a MANUAL clock: no sleeping, every number is exact. */
class OpenLoopScheduleTest {

    private static final long MS = 1_000_000L;

    @Test
    void intendedTimesFollowTheRateNotTheCompletionOfEarlierRequests() {
        OpenLoopSchedule s = new OpenLoopSchedule(1_000, 100);            // 100 requests per second: one every 10 ms
        assertEquals(1_000, s.intendedNanos(0));
        assertEquals(1_000 + 10 * MS, s.intendedNanos(1));
        assertEquals(1_000 + 990 * MS, s.intendedNanos(99));
    }

    @Test
    void weWaitUntilTheIntendedTimeButNeverWaitWhenLate() {
        OpenLoopSchedule s = new OpenLoopSchedule(0, 100);
        assertEquals(7 * MS, s.waitNanos(1, 3 * MS));
        assertEquals(0, s.waitNanos(1, 10 * MS));
        assertEquals(0, s.waitNanos(1, 500 * MS), "late: send at once, never skip a request");
    }

    @Test
    void aStallShowsUpInEveryRequestThatShouldHaveBeenSentDuringIt() {
        OpenLoopSchedule s = new OpenLoopSchedule(0, 100);
        long now = 0;
        long[] open = new long[11];
        for (int i = 0; i <= 10; i++) {
            now += s.waitNanos(i, now);
            now += (i == 3 ? 60 : 1) * MS;                                // request 3 hits a 60 ms stall, the others take 1 ms
            open[i] = s.latencyNanos(i, now);
        }
        assertEquals(1 * MS, open[0]);
        assertEquals(1 * MS, open[2]);
        assertEquals(60 * MS, open[3]);
        assertEquals(51 * MS, open[4], "request 4 was due at 40 ms but could only start at 90 ms");
        assertEquals(42 * MS, open[5]);
        assertEquals(33 * MS, open[6]);
        assertEquals(24 * MS, open[7]);
        assertEquals(15 * MS, open[8]);
        assertEquals(6 * MS, open[9]);
        assertEquals(1 * MS, open[10], "back on schedule: the backlog is gone");

        // the same stall in a CLOSED loop: the next request is sent only when the previous one finished, so only ONE request is slow
        long closedNow = 0;
        long slowClosed = 0;
        for (int i = 0; i <= 10; i++) {
            long start = closedNow;
            closedNow += (i == 3 ? 60 : 1) * MS;
            if (closedNow - start > 10 * MS) slowClosed++;
        }
        assertEquals(1, slowClosed, "the closed loop hides the 5 other requests that were delayed by the stall");
        long slowOpen = java.util.Arrays.stream(open).filter(l -> l > 10 * MS).count();
        assertEquals(6, slowOpen);
    }

    @Test
    void theRateMustBePositive() {
        assertThrows(IllegalArgumentException.class, () -> new OpenLoopSchedule(0, 0));
        assertThrows(IllegalArgumentException.class, () -> new OpenLoopSchedule(0, -5));
    }

    @Test
    void verySlowRatesDoNotOverflow() {
        OpenLoopSchedule s = new OpenLoopSchedule(0, 0.5);                // one request every 2 seconds
        assertEquals(2_000 * MS, s.intendedNanos(1));
        assertEquals(2_000_000 * MS, s.intendedNanos(1000));
    }
}
