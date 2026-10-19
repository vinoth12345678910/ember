package com.ember.broker;

import com.ember.client.EmberClient;
import com.ember.protocol.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * REGRESSION (found with real processes): Raft messages from peers were handled on the same pool as client requests. A
 * produce waiting for its commit blocked its handler thread, and the peer connection pinned to the same thread could not
 * deliver the acknowledgement the produce was waiting for. With ONE handler thread that deadlock is certain.
 */
class PeerTrafficIsolationTest {

    @Test
    void aBlockedProduceDoesNotStopRaftMessagesFromFlowing(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, false, "--handler-threads=1", "--produce-timeout-ms=20000",
                "--raft-election-min-ms=1500", "--raft-election-max-ms=2500", "--raft-heartbeat-ms=100")) {
            ClusterIntegrationTest.createTopic(c, "t", 1);
            int leader = c.awaitLeader("t", 0);
            try (EmberClient client = new EmberClient("localhost", c.port(leader))) {
                c.isolate(leader);                                          // the entry cannot reach anybody yet
                CompletableFuture<Long> produce = CompletableFuture.supplyAsync(() -> {
                    try {
                        return client.produce("t", 0, List.of(new ProducerRecord(null, "blocked-then-released".getBytes())));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
                Await.until(10_000, "the produce to be waiting for its commit",
                        () -> c.servers[leader].replicaManager().topic("t").replica(0).group().lastLogIndex() > 1);
                c.heal();                                                   // now the followers can answer, if their messages can be processed
                assertEquals(0L, produce.get(15, TimeUnit.SECONDS), "the produce completes as soon as the link is healed");
            }
        }
    }
}
