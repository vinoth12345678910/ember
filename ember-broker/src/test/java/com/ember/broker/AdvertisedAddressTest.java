package com.ember.broker;

import com.ember.client.EmberClient;
import com.ember.protocol.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Clients are told the ADVERTISED address of each broker; the brokers keep using their real (internal) addresses among
 * themselves. If a broker advertised an address that is only valid for clients (say, a Docker host-mapped port) and its peers
 * used it too, the cluster would break; this test makes the advertised ones unreachable and the cluster must still work.
 */
class AdvertisedAddressTest {

    private static final InetSocketAddress NOWHERE_2 = InetSocketAddress.createUnresolved("broker-two.example", 19092);
    private static final InetSocketAddress NOWHERE_3 = InetSocketAddress.createUnresolved("broker-three.example", 19093);

    @Test
    void metadataAndFindCoordinatorReturnAdvertisedAddressesWhilePeersKeepUsingTheInternalOnes(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, false, Map.of(2, NOWHERE_2, 3, NOWHERE_3))) {
            ClusterIntegrationTest.createTopic3(c, "t", 1);                                  // goes over the INTERNAL addresses
            int leader = c.awaitLeader("t", 0);
            for (int ask = 1; ask <= 3; ask++) {
                try (EmberClient client = new EmberClient("localhost", c.port(ask))) {
                    ClusterMetadataResponse m = client.clusterMetadata("t");
                    for (ClusterMetadataResponse.Broker b : m.brokers()) {
                        switch (b.nodeId()) {
                            case 1 -> {
                                assertEquals("localhost", b.host(), "broker 1 advertises its real address");
                                assertEquals(c.port(1), b.port());
                            }
                            case 2 -> {
                                assertEquals("broker-two.example", b.host());
                                assertEquals(19092, b.port());
                            }
                            default -> {
                                assertEquals("broker-three.example", b.host());
                                assertEquals(19093, b.port());
                            }
                        }
                    }
                    FindCoordinatorResponse found = client.findCoordinator("g");
                    InetSocketAddress expected = c.advertised.get(found.nodeId());
                    assertEquals(expected.getHostString(), found.host());
                    assertEquals(expected.getPort(), found.port());
                }
            }
            // METADATA v1 tells a client how to reach THIS broker: its advertised address too
            try (EmberClient client = new EmberClient("localhost", c.port(2))) {
                MetadataResponse v1 = client.metadata("t");
                assertEquals("broker-two.example", v1.broker().host());
                assertEquals(19092, v1.broker().port());
            }
            // the cluster itself works over the internal addresses: a produce to the leader is replicated to every replica
            try (EmberClient client = new EmberClient("localhost", c.port(leader))) {
                client.produce("t", 0, List.of(new ProducerRecord(null, "hello".getBytes())));
            }
            Await.until(30_000, "the record on all three replicas\n" + c.describe(), () -> {
                for (int i = 1; i <= 3; i++) {
                    var log = c.servers[i].replicaManager().logFor("t", 0);
                    if (log == null || log.nextOffset() != 1) return false;
                }
                return true;
            });
            // /status shows both, so an operator can see the difference
            Map<String, Object> status = TestJson.object(Http.get(c.adminPort(1), "/status").body());
            @SuppressWarnings("unchecked") List<Map<String, Object>> cluster = (List<Map<String, Object>>) (List<?>) status.get("cluster");
            Map<String, Object> second = cluster.stream().filter(n -> ((Double) n.get("nodeId")).intValue() == 2).findFirst().orElseThrow();
            assertEquals("localhost", second.get("host"));
            assertEquals("broker-two.example", second.get("advertisedHost"));
        }
    }

    @Test
    void withoutAdvertisedAddressesEverythingIsTheInternalAddress(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, false)) {
            c.awaitController();
            try (EmberClient client = new EmberClient("localhost", c.port(1))) {
                for (ClusterMetadataResponse.Broker b : client.clusterMetadata().brokers()) {
                    assertEquals("localhost", b.host());
                    assertEquals(c.port(b.nodeId()), b.port());
                }
            }
        }
    }

    @Test
    void aSingleBrokerCanAdvertiseADifferentAddressToo(@TempDir Path dir) throws Exception {
        try (BrokerServer server = ServerFixture.start(ServerFixture.config(dir, "--advertise=public.example:4242"), java.time.Clock.systemUTC())) {
            try (EmberClient client = new EmberClient("localhost", server.port())) {
                ClusterMetadataResponse.Broker b = client.clusterMetadata().brokers().get(0);
                assertEquals("public.example", b.host());
                assertEquals(4242, b.port());
                assertEquals("public.example", client.findCoordinator("g").host());
            }
        }
    }
}
