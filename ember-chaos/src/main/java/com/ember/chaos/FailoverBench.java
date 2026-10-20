package com.ember.chaos;

import com.ember.client.ClientConfig;
import com.ember.client.ClusterClient;
import com.ember.client.EmberException;
import com.ember.protocol.ClusterMetadataResponse;
import com.ember.protocol.ErrorCode;
import com.ember.protocol.ProducerRecord;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Two measurements that need a real crash: how long clients are blocked after the LEADER of a partition is killed
 * (time to the first successful produce that started after the kill), and how long a broker that missed N records needs after it
 * comes back until it is ready and holds all of them.
 */
public final class FailoverBench {

    public record Stats(int n, long min, long median, long p90, long max, double mean) {
        public static Stats of(List<Long> values) {
            List<Long> sorted = new ArrayList<>(values);
            java.util.Collections.sort(sorted);
            int n = sorted.size();
            double mean = sorted.stream().mapToLong(Long::longValue).average().orElse(0);
            return new Stats(n, sorted.get(0), sorted.get(n / 2), sorted.get(Math.min(n - 1, (int) Math.ceil(0.9 * n) - 1)), sorted.get(n - 1), mean);
        }

        @Override
        public String toString() {
            return String.format(Locale.ROOT, "n=%d  min %d ms  median %d ms  p90 %d ms  max %d ms  mean %.0f ms", n, min, median, p90, max, mean);
        }
    }

    private FailoverBench() {
    }

    private static ClientConfig clientConfig(List<String> bootstrap) {
        return ClientConfig.defaults().withBootstrap(bootstrap).withRetries(60, 20, 200).withTimeouts(1_000, 5_000);
    }

    private static boolean allReady(List<String> admins) {
        return admins.stream().allMatch(AdminClient::ready);
    }

    private static void awaitReady(List<String> admins, long timeoutMs) {
        if (!InProcessCluster.await(timeoutMs, () -> allReady(admins))) {
            throw new IllegalStateException("the cluster did not become ready within " + timeoutMs + " ms");
        }
    }

    /** Kills the leader of partition 0 `kills` times; returns the milliseconds each recovery took. */
    public static List<Long> failover(FaultDriver driver, List<String> bootstrap, List<String> admins, int kills, String topic) throws Exception {
        List<Long> millis = new ArrayList<>();
        try (ClusterClient client = new ClusterClient(clientConfig(bootstrap))) {
            awaitReady(admins, 120_000);
            try {
                client.createTopic(topic, 1);
            } catch (EmberException e) {
                if (e.errorCode() != ErrorCode.TOPIC_ALREADY_EXISTS) throw e;
            }
            awaitTopicReady(client, topic, 1);
            for (int cycle = 1; cycle <= kills; cycle++) {
                awaitReady(admins, 120_000);
                produceOne(client, topic, "warm-" + cycle);                          // routes and leader cache are warm, the cluster is healthy
                int leader = leaderOf(client, topic);
                long killedAt = System.nanoTime();
                driver.crash(leader);
                long recoveredAt;
                while (true) {
                    long started = System.nanoTime();
                    try {
                        produceOne(client, topic, "after-" + cycle);
                        recoveredAt = System.nanoTime();
                        if (started >= killedAt) break;                              // a produce that was already in flight at the kill does not count
                    } catch (EmberException | IOException e) {
                        if (System.nanoTime() - killedAt > 120_000_000_000L) throw new IllegalStateException("no recovery within 120 s after killing broker " + leader, e);
                    }
                }
                millis.add((recoveredAt - killedAt) / 1_000_000);
                driver.restart(leader);
                awaitReady(admins, 120_000);
            }
        }
        return millis;
    }

    /** createTopic returns when the topic is COMMITTED; its partitions still need to be created on every broker and to elect leaders. */
    private static void awaitTopicReady(ClusterClient client, String topic, int partitions) throws IOException {
        long deadline = System.nanoTime() + 60_000_000_000L;
        while (System.nanoTime() < deadline) {
            ClusterMetadataResponse m = client.clusterMetadata(topic);
            if (!m.topics().isEmpty() && m.topics().get(0).partitions().size() == partitions
                    && m.topics().get(0).partitions().stream().allMatch(p -> p.leaderId() >= 0)) {
                return;
            }
            java.util.concurrent.locks.LockSupport.parkNanos(50_000_000L);
        }
        throw new IllegalStateException("topic " + topic + " did not get leaders for all " + partitions + " partitions within 60 s");
    }

    private static void produceOne(ClusterClient client, String topic, String value) throws IOException {
        client.produce(topic, 0, List.of(new ProducerRecord(null, value.getBytes(StandardCharsets.UTF_8))));
    }

    private static int leaderOf(ClusterClient client, String topic) throws IOException {
        ClusterMetadataResponse m = client.clusterMetadata(topic);
        return m.topics().get(0).partitions().get(0).leaderId();
    }

    public record CatchUp(long records, long millis, long victim) {
        @Override
        public String toString() {
            return String.format(Locale.ROOT, "broker %d, missed %,d records: ready and holding them all %d ms after the restart command", victim, records, millis);
        }
    }

    /**
     * Kills `victim`, produces `records` records without it (3 partitions), restarts it and times how long until its admin
     * endpoint says ready AND its partitions hold every record.
     */
    public static CatchUp catchUp(FaultDriver driver, List<String> bootstrap, List<String> admins, int victim, long records, String topic) throws Exception {
        try (ClusterClient client = new ClusterClient(clientConfig(bootstrap))) {
            awaitReady(admins, 120_000);
            try {
                client.createTopic(topic, 3);
            } catch (EmberException e) {
                if (e.errorCode() != ErrorCode.TOPIC_ALREADY_EXISTS) throw e;
            }
            awaitTopicReady(client, topic, 3);
            long baseline = totalEnd(admins.get(victim == 1 ? 1 : 0), topic);
            driver.crash(victim);
            byte[] value = new byte[100];
            java.util.Arrays.fill(value, (byte) 'x');
            com.ember.protocol.Partitioner partitioner = new com.ember.protocol.Partitioner();
            long sent = 0;
            while (sent < records) {
                Map<Integer, List<ProducerRecord>> perPartition = new java.util.TreeMap<>();
                for (int i = 0; i < 500 && sent + i < records; i++) {
                    byte[] key = ("k" + (sent + i)).getBytes(StandardCharsets.UTF_8);
                    perPartition.computeIfAbsent(partitioner.partition(key, 3), p -> new ArrayList<>()).add(new ProducerRecord(key, value));
                }
                for (Map.Entry<Integer, List<ProducerRecord>> e : perPartition.entrySet()) {
                    client.produce(topic, e.getKey(), e.getValue());
                    sent += e.getValue().size();
                }
            }
            long expected = baseline + records;
            String victimAdmin = admins.get(victim - 1);
            long start = System.nanoTime();
            driver.restart(victim);
            long deadline = System.nanoTime() + 300_000_000_000L;
            while (System.nanoTime() < deadline) {
                try {
                    if (AdminClient.ready(victimAdmin) && totalEnd(victimAdmin, topic) >= expected) {
                        return new CatchUp(records, (System.nanoTime() - start) / 1_000_000, victim);
                    }
                } catch (Exception notYet) {
                    // the admin port is not up yet
                }
                java.util.concurrent.locks.LockSupport.parkNanos(20_000_000L);
            }
            throw new IllegalStateException("broker " + victim + " did not catch up within 300 s");
        }
    }

    /** Sum of the end offsets of all partitions of the topic as this broker sees them (from its /status). */
    @SuppressWarnings("unchecked")
    static long totalEnd(String adminAddress, String topic) throws Exception {
        Map<String, Object> status = Json.parseObject(AdminClient.get(adminAddress, "/status").body());
        long total = 0;
        Object topics = status.get("topics");
        if (topics instanceof List<?> list) {
            for (Object t : list) {
                Map<String, Object> topicJson = (Map<String, Object>) t;
                if (topic.equals(topicJson.get("name"))) {
                    for (Object p : (List<Object>) topicJson.get("partitions")) {
                        total += ((Number) ((Map<String, Object>) p).get("logEndOffset")).longValue();
                    }
                }
            }
        }
        return total;
    }
}
