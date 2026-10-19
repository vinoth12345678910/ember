package com.ember.broker;

import com.ember.client.ClusterClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.*;

/** A leader that still has a live majority must NOT resign: with one of three brokers dead, leadership stays put. */
class LeaderStabilityTest {

    @Test
    void withOneBrokerDownTheRemainingLeadersDoNotFlap(@TempDir Path dir) throws Exception {
        // production-like timings: election 300-600 ms, heartbeat 100 ms
        try (TestCluster c = new TestCluster(dir, 3, false, "--raft-election-min-ms=300", "--raft-election-max-ms=600", "--raft-heartbeat-ms=100")) {
            c.awaitController();
            try (ClusterClient admin = new ClusterClient(ClusterIntegrationTest.cfg(c))) {
                admin.createTopic("t", 3);
            }
            c.awaitTopicEverywhere("t", 3);
            for (int p = 0; p < 3; p++) c.awaitLeader("t", p);
            c.crash(1);
            for (int p = 0; p < 3; p++) {
                final int part = p;
                Await.until(20_000, "a leader among the two survivors", () -> c.leaderOf("t", part) > 0);
            }
            int[] last = new int[3];
            int[] changes = new int[3];
            for (int p = 0; p < 3; p++) last[p] = c.leaderOf("t", p);
            long end = System.nanoTime() + 12_000_000_000L;
            while (System.nanoTime() < end) {
                LockSupport.parkNanos(20_000_000L);
                for (int p = 0; p < 3; p++) {
                    int now = c.leaderOf("t", p);
                    if (now > 0 && now != last[p]) {
                        changes[p]++;
                        last[p] = now;
                    }
                }
            }
            System.out.println("leader changes in 12 s: " + java.util.Arrays.toString(changes));
            for (int p = 0; p < 3; p++) assertEquals(0, changes[p], "partition " + p + " changed leader although a majority was alive\n" + c.describe());
        }
    }
}
