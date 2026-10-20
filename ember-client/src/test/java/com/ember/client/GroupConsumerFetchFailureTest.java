package com.ember.client;

import com.ember.protocol.PartitionRef;
import com.ember.storage.LogRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/**
 * REGRESSION (found in Phase 7 by reading the code): poll() advanced the position of partition A, then the fetch of partition B
 * threw, poll() threw, and A's records were dropped while A's position stayed advanced: those records were skipped for ever.
 */
class GroupConsumerFetchFailureTest {

    private static LogRecord record(long offset, String value) {
        return new LogRecord(offset, 1L, null, value.getBytes());
    }

    private static GroupConsumerConfig config() {
        ClientConfig fast = ClientConfig.defaults().withBootstrap(List.of("localhost:1")).withRetries(0, 1, 1).withTimeouts(200, 200);
        return GroupConsumerConfig.of(List.of("localhost:1"), "g").withClient(fast).withAutoCommit(false, 5000);   // nothing ever connects: the fetcher is fake
    }

    @Test
    void recordsAlreadyFetchedAreReturnedWhenALaterPartitionFails() throws Exception {
        PartitionRef a = new PartitionRef("t", 0);
        PartitionRef b = new PartitionRef("t", 1);
        AtomicBoolean bBroken = new AtomicBoolean(true);
        List<String> asked = new ArrayList<>();
        Map<PartitionRef, Long> start = new LinkedHashMap<>();
        start.put(a, 0L);
        start.put(b, 0L);
        try (EmberGroupConsumer consumer = new EmberGroupConsumer(config())) {
            consumer.subscribe("t");
            consumer.adoptForTesting(start, (topic, partition, offset, maxBytes) -> {
                asked.add(partition + "@" + offset);
                if (partition == 0) {
                    return new FetchResult(offset, 0, 3, offset >= 3 ? List.of() : List.of(record(0, "a0"), record(1, "a1"), record(2, "a2")));
                }
                if (bBroken.get()) {
                    throw new IOException("leader of t-1 is unreachable");
                }
                return new FetchResult(offset, 0, 2, offset >= 2 ? List.of() : List.of(record(0, "b0"), record(1, "b1")));
            });
            List<GroupRecord> got = new ArrayList<>();
            // rotation starts a poll at partition 0 or 1; run polls until the one that reads A first and then fails on B has happened
            for (int i = 0; i < 4; i++) {
                try {
                    got.addAll(consumer.poll(Duration.ZERO));
                } catch (IOException expectedWhenBIsTheOnlyPartitionTried) {
                    // a poll that returns nothing may throw: that loses nothing
                }
            }
            assertEquals(3, got.size(), "A's three records must have been delivered despite B failing: " + asked);
            assertEquals(3, consumer.position("t", 0), "A's position moved exactly past the records the application received");
            assertEquals(0, consumer.position("t", 1), "B never delivered anything, so it did not move");

            bBroken.set(false);
            for (int i = 0; i < 3; i++) got.addAll(consumer.poll(Duration.ZERO));
            assertEquals(5, got.size(), "B's records arrive once B recovers; nothing was skipped");
            List<String> values = new ArrayList<>();
            got.forEach(r -> values.add(new String(r.value())));
            assertEquals(List.of("a0", "a1", "a2", "b0", "b1"), values.stream().sorted().toList());
            assertEquals(2, consumer.position("t", 1));
        }
    }

    @Test
    void whenNothingWasFetchedYetTheFailureIsStillReportedToTheApplication() throws Exception {
        PartitionRef a = new PartitionRef("t", 0);
        try (EmberGroupConsumer consumer = new EmberGroupConsumer(config())) {
            consumer.subscribe("t");
            consumer.adoptForTesting(Map.of(a, 0L), (topic, partition, offset, maxBytes) -> {
                throw new IOException("cluster unreachable");
            });
            assertThrows(IOException.class, () -> consumer.poll(Duration.ZERO));
            assertEquals(0, consumer.position("t", 0));
        }
    }
}
