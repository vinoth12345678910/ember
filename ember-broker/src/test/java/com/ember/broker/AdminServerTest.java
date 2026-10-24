package com.ember.broker;

import com.ember.client.EmberClient;
import com.ember.protocol.ClusterMetadataResponse;
import com.ember.protocol.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** The admin HTTP endpoints of real in-process brokers. */
class AdminServerTest {

    private static List<ProducerRecord> records(int from, int count) {
        List<ProducerRecord> out = new ArrayList<>();
        for (int i = from; i < from + count; i++) out.add(new ProducerRecord("k".getBytes(), ("value-" + i).getBytes()));
        return out;
    }

    private static long bytesOf(List<ProducerRecord> records) {
        long n = 0;
        for (ProducerRecord r : records) n += r.key().length + r.value().length;
        return n;
    }

    @Test
    void metricsHaveTheExpectedNamesAndCountersIncreaseByTheExpectedAmounts(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, false)) {
            ClusterIntegrationTest.createTopic(c, "t", 1);
            int leader = c.awaitLeader("t", 0);
            int admin = c.adminPort(leader);
            List<Http.Sample> before = Http.scrape(admin);
            for (String name : new String[]{"ember_requests_total", "ember_produce_requests_total", "ember_produce_records_total", "ember_produce_bytes_total",
                    "ember_produce_latency_seconds_count", "ember_fetch_requests_total", "ember_fetch_bytes_total", "ember_connections",
                    "ember_raft_term", "ember_raft_role", "ember_raft_commit_index", "ember_raft_last_applied", "ember_raft_last_index",
                    "ember_raft_leader_changes_total", "ember_raft_elections_started_total", "ember_raft_checkquorum_stepdowns_total",
                    "ember_raft_pending_proposals", "ember_raft_fsync_total", "ember_raft_apply_latency_seconds_count",
                    "ember_peer_queue_depth", "ember_peer_messages_sent_total", "ember_peer_messages_dropped_total", "ember_peer_connected",
                    "ember_peer_reconnects_total", "ember_log_start_offset", "ember_log_end_offset", "ember_log_size_bytes", "ember_log_segments",
                    "ember_retention_deleted_segments_total", "ember_jvm_heap_used_bytes", "ember_jvm_heap_max_bytes", "ember_jvm_threads",
                    "ember_jvm_gc_collections_total", "ember_jvm_gc_collection_seconds_total", "ember_process_open_fds",
                    "ember_jvm_direct_buffer_used_bytes", "ember_process_uptime_seconds"}) {
                assertTrue(before.stream().anyMatch(s -> s.name().equals(name)), "metric " + name + " is missing from /metrics");
            }
            assertEquals(2.0, Http.value(before, "ember_raft_role", "group", "t-0"), "the leader exports raft_role 2");

            double okBefore = Http.value(before, "ember_produce_requests_total", "result", "ok");
            double recordsBefore = Http.value(before, "ember_produce_records_total");
            double bytesBefore = Http.value(before, "ember_produce_bytes_total");
            double latencyCountBefore = Http.value(before, "ember_produce_latency_seconds_count");
            double requestsBefore = Http.value(before, "ember_requests_total", "api", "produce", "error", "none");
            double applyCountBefore = Http.value(before, "ember_raft_apply_latency_seconds_count", "group", "t-0");
            double commitBefore = Http.value(before, "ember_raft_commit_index", "group", "t-0");
            double fetchBefore = Http.value(before, "ember_fetch_requests_total", "result", "ok");

            List<ProducerRecord> all = new ArrayList<>();
            try (EmberClient client = new EmberClient("localhost", c.port(leader))) {
                for (int batch = 0; batch < 3; batch++) {
                    List<ProducerRecord> r = records(batch * 4, 4);
                    all.addAll(r);
                    client.produce("t", 0, r);
                }
                assertEquals(12, client.fetch("t", 0, 0, 65536).records().size());
            }
            List<Http.Sample> after = Http.scrape(admin);
            assertEquals(okBefore + 3, Http.value(after, "ember_produce_requests_total", "result", "ok"));
            assertEquals(recordsBefore + 12, Http.value(after, "ember_produce_records_total"));
            assertEquals(bytesBefore + bytesOf(all), Http.value(after, "ember_produce_bytes_total"));
            assertEquals(requestsBefore + 3, Http.value(after, "ember_requests_total", "api", "produce", "error", "none"));
            assertEquals(applyCountBefore + 3, Http.value(after, "ember_raft_apply_latency_seconds_count", "group", "t-0"));
            assertTrue(Http.value(after, "ember_raft_commit_index", "group", "t-0") >= commitBefore + 3);
            assertEquals(fetchBefore + 1, Http.value(after, "ember_fetch_requests_total", "result", "ok"));
            assertTrue(Http.value(after, "ember_fetch_bytes_total") > 0);
            assertEquals(12.0, Http.value(after, "ember_log_end_offset", "topic", "t", "partition", "0"));
            assertTrue(Http.value(after, "ember_log_size_bytes", "topic", "t", "partition", "0") > 0);
            assertEquals(latencyCountBefore + 3, Http.value(after, "ember_produce_latency_seconds_count"),
                    "the latency histogram is recorded when the response is on the wire");
            double inf = Http.value(after, "ember_produce_latency_seconds_bucket", "le", "+Inf");
            assertEquals(latencyCountBefore + 3, inf, "the +Inf bucket equals the count");
        }
    }

    @Test
    void groupMetricsAppearOnceAGroupExistsAndLagIsCommittedMinusEnd(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, false)) {
            ClusterIntegrationTest.createTopic(c, "orders", 2);
            EmberGroupConsumerTest.produce(c, "orders", 0, 20);
            try (com.ember.client.EmberGroupConsumer consumer = new com.ember.client.EmberGroupConsumer(EmberGroupConsumerTest.groupConfig(c, "g"))) {
                consumer.subscribe("orders");
                EmberGroupConsumerTest.drain(consumer, 20, 30_000);
                consumer.commitSync();
                int coordinator = c.awaitCoordinator();
                List<Http.Sample> samples = Http.scrape(c.adminPort(coordinator));
                assertEquals(1.0, Http.value(samples, "ember_group_members", "group", "g"));
                assertEquals(1.0, Http.value(samples, "ember_group_generation", "group", "g"));
                assertTrue(Http.value(samples, "ember_group_rebalances_total", "group", "g") >= 1);
                assertEquals(1.0, Http.value(samples, "ember_group_offset_commits_total", "result", "ok"));
                final double[] lag = {0};
                Await.until(15_000, "group_lag to show 0 for both partitions on the coordinator", () -> {
                    try {
                        List<Http.Sample> s = Http.scrape(c.adminPort(coordinator));
                        double l0 = Http.value(s, "ember_group_lag", "group", "g", "topic", "orders", "partition", "0");
                        double l1 = Http.value(s, "ember_group_lag", "group", "g", "topic", "orders", "partition", "1");
                        lag[0] = l0 + l1;
                        return l0 == 0 && l1 == 0;
                    } catch (Exception e) {
                        return false;
                    }
                });
                EmberGroupConsumerTest.produce(c, "orders", 20, 30);
                Await.until(15_000, "group_lag to show the 10 new records", () -> {
                    try {
                        List<Http.Sample> s = Http.scrape(c.adminPort(coordinator));
                        return Http.value(s, "ember_group_lag", "group", "g", "topic", "orders", "partition", "0")
                                + Http.value(s, "ember_group_lag", "group", "g", "topic", "orders", "partition", "1") >= 10;
                    } catch (Exception e) {
                        return false;
                    }
                });
            }
        }
    }

    @Test
    void liveIsUpAtOnceAndReadyOnlyAfterTheBrokerJoinedTheCluster(@TempDir Path dir) throws Exception {
        BrokerServer server = new BrokerServer(ServerFixture.config(dir, "--admin-port=ephemeral"), Clock.systemUTC());
        try {
            int port = server.bind();
            int admin = server.adminPort();
            assertTrue(admin > 0);
            assertEquals(200, Http.get(admin, "/health/live").status());
            Http.Response notReady = Http.get(admin, "/health/ready");
            assertEquals(503, notReady.status());
            Map<String, Object> json = TestJson.object(notReady.body());
            assertEquals(false, json.get("ready"));
            assertTrue(json.get("reason").toString().contains("not joined"), notReady.body());

            server.start(Map.of(0, new InetSocketAddress("localhost", port)));
            Await.until(20_000, "/health/ready to turn 200 once the lone broker leads its groups", () -> {
                try {
                    return Http.get(admin, "/health/ready").status() == 200;
                } catch (Exception e) {
                    return false;
                }
            });
            assertEquals(true, TestJson.object(Http.get(admin, "/health/ready").body()).get("ready"));
        } finally {
            server.close();
        }
    }

    @Test
    void aRestartedBrokerBecomesReadyAgainOnceItHasCaughtUp(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, false)) {
            ClusterIntegrationTest.createTopic(c, "t", 2);
            EmberGroupConsumerTest.produce(c, "t", 0, 200);
            for (int i = 1; i <= 3; i++) {
                final int broker = i;
                Await.until(30_000, "broker " + broker + " to be ready\n" + c.describe(), () -> {
                    try {
                        return Http.get(c.adminPort(broker), "/health/ready").status() == 200;
                    } catch (Exception e) {
                        return false;
                    }
                });
            }
            c.crash(2);
            EmberGroupConsumerTest.produce(c, "t", 200, 400);                       // broker 2 misses these
            c.restart(2);
            Await.until(60_000, "the restarted broker 2 to catch up and become ready\n" + c.describe(), () -> {
                try {
                    return Http.get(c.adminPort(2), "/health/ready").status() == 200;
                } catch (Exception e) {
                    return false;
                }
            });
            long total = 0;
            for (int p = 0; p < 2; p++) {
                assertNotNull(c.servers[2].replicaManager().logFor("t", p),
                        "ready, yet partition t-" + p + " does not exist on the restarted broker; its /status: " + Http.get(c.adminPort(2), "/status").body());
                total += c.servers[2].replicaManager().logFor("t", p).nextOffset();
            }
            assertEquals(400, total, "ready means caught up: the restarted broker holds all 400 records");
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void statusParsesAndItsLeadersAgreeWithMetadataV2(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, false)) {
            ClusterIntegrationTest.createTopic(c, "t", 3);
            for (int p = 0; p < 3; p++) c.awaitLeader("t", p);
            int broker = 1;
            Await.until(20_000, "/status leaders to agree with METADATA v2 on broker 1\n" + c.describe(), () -> {
                try (EmberClient client = new EmberClient("localhost", c.port(broker))) {
                    ClusterMetadataResponse m = client.clusterMetadata("t");
                    Map<String, Object> status = TestJson.object(Http.get(c.adminPort(broker), "/status").body());
                    List<Object> topics = (List<Object>) status.get("topics");
                    Map<String, Object> t = (Map<String, Object>) topics.get(0);
                    List<Object> parts = (List<Object>) t.get("partitions");
                    for (int p = 0; p < 3; p++) {
                        int metadataLeader = m.topics().get(0).partitions().get(p).leaderId();
                        int statusLeader = ((Double) ((Map<String, Object>) parts.get(p)).get("leader")).intValue();
                        if (metadataLeader < 0 || metadataLeader != statusLeader) return false;
                    }
                    return true;
                } catch (Exception e) {
                    return false;
                }
            });
            Map<String, Object> status = TestJson.object(Http.get(c.adminPort(1), "/status").body());
            assertEquals(1.0, status.get("nodeId"));
            assertEquals(true, status.get("joined"));
            assertEquals(3, ((List<Object>) status.get("cluster")).size());
            assertEquals(3 + 3, ((List<Object>) status.get("raftGroups")).size(), "meta + __groups + __acks + 3 partitions");
            assertEquals(2, ((List<Object>) status.get("peers")).size());
            for (Object g : (List<Object>) status.get("raftGroups")) {
                Map<String, Object> group = (Map<String, Object>) g;
                assertNotNull(group.get("role"));
                assertTrue((Double) group.get("commitIndex") >= (Double) group.get("lastApplied"));
            }
            assertFalse(Http.get(c.adminPort(1), "/status").body().contains("value-"), "no record contents in /status");
        }
    }

    @Test
    void anAdminPortOfZeroOpensNoPortAndTheDefaultWithAnEphemeralBrokerPortIsOff(@TempDir Path dir) throws Exception {
        try (BrokerServer off = ServerFixture.start(ServerFixture.config(dir.resolve("a"), "--admin-port=0"), Clock.systemUTC());
             BrokerServer byDefault = ServerFixture.start(ServerFixture.config(dir.resolve("b")), Clock.systemUTC())) {
            assertEquals(-1, off.adminPort());
            assertEquals(-1, byDefault.adminPort(), "port 0 (ephemeral) has no meaningful 'port + 1000'");
        }
    }

    @Test
    void theDefaultAdminPortIsTheDataPortPlus1000AndOtherMethodsOrPathsAreRefused() {
        BrokerConfig fixed = BrokerConfig.parse("--port=9099");
        assertEquals(10099, fixed.effectiveAdminPort());
        assertEquals(0, BrokerConfig.parse("--port=9099", "--admin-port=0").effectiveAdminPort());
        assertEquals(7777, BrokerConfig.parse("--port=9099", "--admin-port=7777").effectiveAdminPort());
        assertEquals("127.0.0.1", fixed.adminHost(), "unauthenticated: loopback unless asked otherwise");
        assertFalse(fixed.enableDebugEndpoints());
        assertTrue(BrokerConfig.parse("--enable-debug-endpoints").enableDebugEndpoints());
    }

    @Test
    void postIsRefusedAndUnknownPathsAre404(@TempDir Path dir) throws Exception {
        try (BrokerServer server = ServerFixture.start(ServerFixture.config(dir, "--admin-port=ephemeral"), Clock.systemUTC())) {
            assertEquals(405, Http.send(server.adminPort(), "/metrics", "POST").status());
            assertEquals(404, Http.get(server.adminPort(), "/nope").status());
        }
    }

    @Test
    void theChecksumEndpointIsAbsentUnlessEnabledAndEqualAcrossReplicasWhenEnabled(@TempDir Path dir) throws Exception {
        try (TestCluster off = new TestCluster(dir.resolve("off"), 1, false)) {
            ClusterIntegrationTest.createTopic(off, "t", 1);
            assertEquals(404, Http.get(off.adminPort(1), "/debug/partition-checksum?topic=t&partition=0").status());
        }
        try (TestCluster c = new TestCluster(dir.resolve("on"), 3, false, "--enable-debug-endpoints")) {
            ClusterIntegrationTest.createTopic(c, "t", 1);
            EmberGroupConsumerTest.produce(c, "t", 0, 50);
            Await.until(30_000, "equal data-log checksums on all replicas\n" + c.describe(), () -> {
                try {
                    String reference = null;
                    for (int i = 1; i <= 3; i++) {
                        Http.Response r = Http.get(c.adminPort(i), "/debug/partition-checksum?topic=t&partition=0");
                        if (r.status() != 200) return false;
                        Map<String, Object> json = TestJson.object(r.body());
                        if ((Double) json.get("endOffset") != 50.0) return false;
                        String crc = json.get("crc32").toString();
                        if (reference == null) reference = crc;
                        else if (!reference.equals(crc)) return false;
                    }
                    return true;
                } catch (Exception e) {
                    return false;
                }
            });
            assertEquals(400, Http.get(c.adminPort(1), "/debug/partition-checksum").status());
            assertEquals(404, Http.get(c.adminPort(1), "/debug/partition-checksum?topic=nope&partition=0").status());
        }
    }
}
