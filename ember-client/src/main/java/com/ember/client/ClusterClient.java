package com.ember.client;

import com.ember.protocol.ClusterMetadataResponse;
import com.ember.protocol.ErrorCode;
import com.ember.protocol.ProducerRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Talks to a whole cluster: finds the brokers through METADATA v2, sends PRODUCE and FETCH to the partition leader and
 * CREATE_TOPIC to the controller, and on a retriable error or a lost connection refreshes the metadata, backs off and
 * retries. Retries are AT-LEAST-ONCE for PRODUCE: after a timeout or a lost connection the record may already be
 * committed, so a retry can create a duplicate.
 *
 * Thread-safe through synchronization: one request in flight at a time per ClusterClient.
 */
public final class ClusterClient implements Router {

    private interface Call<T> {
        T run(EmberClient connection) throws IOException;
    }

    private static final int MAX_UNKNOWN_RETRIES = 3;      // a topic that does not exist should fail fast

    private final ClientConfig config;
    private final List<String> bootstrap = new ArrayList<>();
    private final Map<Integer, String[]> brokers = new HashMap<>();             // nodeId -> {host, port}
    private final Map<Integer, EmberClient> connections = new HashMap<>();
    private final Map<String, Map<Integer, Integer>> leaders = new HashMap<>();  // topic -> partition -> leader
    private int controllerId = -1;
    private final AtomicLong retried = new AtomicLong();
    private boolean closed;

    public ClusterClient(ClientConfig config) {
        this.config = config;
        if (config.bootstrap().isEmpty()) {
            throw new IllegalArgumentException("a bootstrap list (host:port,...) is needed");
        }
        bootstrap.addAll(config.bootstrap());
    }

    public ClusterClient(String host, int port, ClientConfig config) {
        this(config.withBootstrap(merge(host + ":" + port, config.bootstrap())));
    }

    private static List<String> merge(String first, List<String> rest) {
        List<String> all = new ArrayList<>();
        all.add(first);
        for (String r : rest) if (!all.contains(r)) all.add(r);
        return all;
    }

    /** How many requests had to be retried so far. */
    public long retriedRequests() {
        return retried.get();
    }

    // ------------------------------------------------------------------ metadata

    /** Fresh cluster layout (for the CLI and tests). Tries known brokers first, then the bootstrap list. */
    public synchronized ClusterMetadataResponse clusterMetadata(String... topics) throws IOException {
        return refresh(topics);
    }

    /**
     * Asks brokers until the layout of the requested topics is COMPLETE (every partition has a known leader). One
     * broker's answer is not enough: a broker that was just restarted has not replayed the topic list yet, and a broker
     * cut off from the cluster no longer knows any leader. The answers are merged: a known leader wins over "unknown".
     */
    private ClusterMetadataResponse refresh(String... topics) throws IOException {
        IOException last = null;
        ClusterMetadataResponse best = null;
        int bestScore = -1;
        Map<String, Map<Integer, Integer>> merged = new HashMap<>();
        int controller = -1;
        List<String> candidates = new ArrayList<>();
        for (Map.Entry<Integer, String[]> b : brokers.entrySet()) candidates.add(b.getValue()[0] + ":" + b.getValue()[1]);
        for (String b : bootstrap) if (!candidates.contains(b)) candidates.add(b);
        for (String address : candidates) {
            try {
                ClusterMetadataResponse m = connectionTo(address).clusterMetadata(topics);
                brokers.clear();
                for (ClusterMetadataResponse.Broker b : m.brokers()) {
                    brokers.put(b.nodeId(), new String[]{b.host(), Integer.toString(b.port())});
                }
                if (m.controllerId() >= 0) controller = m.controllerId();
                int score = 0;
                for (ClusterMetadataResponse.Topic t : m.topics()) {
                    Map<Integer, Integer> perPartition = merged.computeIfAbsent(t.name(), k -> new HashMap<>());
                    for (ClusterMetadataResponse.Partition p : t.partitions()) {
                        perPartition.merge(p.partition(), p.leaderId(), (old, now) -> old >= 0 ? old : now);
                        score += 1 + (p.leaderId() >= 0 ? 1 : 0);
                    }
                }
                if (best == null || score > bestScore) {
                    best = m;
                    bestScore = score;
                }
                if (complete(m, topics)) {
                    break;
                }
            } catch (IOException e) {
                last = e;
                dropConnection(address);
            }
        }
        if (best == null) {
            throw last != null ? last : new IOException("no broker to ask");
        }
        controllerId = controller;
        for (Map.Entry<String, Map<Integer, Integer>> e : merged.entrySet()) leaders.put(e.getKey(), e.getValue());
        for (String name : topics) {
            if (!merged.containsKey(name)) leaders.remove(name);
        }
        return best;
    }

    private static boolean complete(ClusterMetadataResponse m, String[] requested) {
        if (requested.length == 0) {
            return m.controllerId() >= 0;
        }
        for (String name : requested) {
            ClusterMetadataResponse.Topic found = null;
            for (ClusterMetadataResponse.Topic t : m.topics()) if (t.name().equals(name)) found = t;
            if (found == null) return false;
            for (ClusterMetadataResponse.Partition p : found.partitions()) if (p.leaderId() < 0) return false;
        }
        return true;
    }

    // ------------------------------------------------------------------ connections

    private final Map<String, EmberClient> byAddress = new HashMap<>();

    private EmberClient connectionTo(String address) throws IOException {
        EmberClient c = byAddress.get(address);
        if (c != null) return c;
        int colon = address.lastIndexOf(':');
        c = new EmberClient(address.substring(0, colon), Integer.parseInt(address.substring(colon + 1)), config);
        byAddress.put(address, c);
        return c;
    }

    private void dropConnection(String address) {
        EmberClient c = byAddress.remove(address);
        if (c != null) {
            try {
                c.close();
            } catch (IOException ignored) {
                // already broken
            }
        }
    }

    private EmberClient connectionToBroker(int nodeId) throws IOException {
        String[] b = brokers.get(nodeId);
        if (b == null) throw new IOException("unknown broker " + nodeId);
        return connectionTo(b[0] + ":" + b[1]);
    }

    private void dropBroker(int nodeId) {
        String[] b = brokers.get(nodeId);
        if (b != null) dropConnection(b[0] + ":" + b[1]);
    }

    // ------------------------------------------------------------------ routing and retries

    private enum Target {
        LEADER, CONTROLLER
    }

    private static boolean retriable(ErrorCode code) {
        return switch (code) {
            case UNKNOWN_TOPIC_OR_PARTITION, NOT_LEADER_OR_FOLLOWER, LEADER_NOT_AVAILABLE, NOT_CONTROLLER, BROKER_OVERLOADED,
                 REQUEST_TIMED_OUT -> true;
            default -> false;
        };
    }

    private <T> T route(Target target, String topic, int partition, Call<T> call) throws IOException {
        if (closed) throw new IOException("client is closed");
        int unknown = 0;
        long backoff = config.retryBackoffMs();
        Exception last = null;
        for (int attempt = 0; attempt <= config.retries(); attempt++) {
            if (attempt > 0) {
                retried.incrementAndGet();
                pause(backoff);
                backoff = Math.min(backoff * 2, config.maxRetryBackoffMs());
            }
            int node = -1;
            try {
                node = target == Target.CONTROLLER ? controllerId : leaderOf(topic, partition);
                if (node < 0) {
                    refresh(topic == null ? new String[0] : new String[]{topic});
                    node = target == Target.CONTROLLER ? controllerId : leaderOf(topic, partition);
                }
                if (node < 0 && target == Target.LEADER && !partitionKnown(topic, partition)) {
                    // the freshest metadata does not list this partition: probably it does not exist (fail fast after a few looks)
                    last = new EmberException(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION);
                    if (++unknown > MAX_UNKNOWN_RETRIES) throw (EmberException) last;
                    continue;
                }
                if (node < 0) {
                    last = new EmberException(target == Target.CONTROLLER ? ErrorCode.NOT_CONTROLLER : ErrorCode.LEADER_NOT_AVAILABLE);
                    continue;
                }
                return call.run(connectionToBroker(node));
            } catch (EmberException e) {
                if (!retriable(e.errorCode())) throw e;
                if (e.errorCode() == ErrorCode.UNKNOWN_TOPIC_OR_PARTITION && ++unknown > MAX_UNKNOWN_RETRIES) throw e;
                last = e;
                forgetRoute(target, topic, partition);
                refreshQuietly(topic);
            } catch (IOException e) {
                last = e;                                   // the broker is gone, or the connection broke
                if (node >= 0) dropBroker(node);
                forgetRoute(target, topic, partition);
                refreshQuietly(topic);
            }
        }
        if (last instanceof EmberException e) throw e;
        throw new IOException("gave up after " + config.retries() + " retries: " + last, last);
    }

    private boolean partitionKnown(String topic, int partition) {
        Map<Integer, Integer> m = leaders.get(topic);
        return m != null && m.containsKey(partition);
    }

    private int leaderOf(String topic, int partition) {
        Map<Integer, Integer> m = leaders.get(topic);
        Integer l = m == null ? null : m.get(partition);
        return l == null ? -1 : l;
    }

    private void forgetRoute(Target target, String topic, int partition) {
        if (target == Target.CONTROLLER) {
            controllerId = -1;
        } else {
            Map<Integer, Integer> m = leaders.get(topic);
            if (m != null) m.remove(partition);
        }
    }

    private void refreshQuietly(String topic) {
        try {
            refresh(topic == null ? new String[0] : new String[]{topic});
        } catch (IOException ignored) {
            // the next attempt will try again after its back-off
        }
    }

    private static void pause(long ms) throws IOException {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while backing off", e);
        }
    }

    // ------------------------------------------------------------------ Router

    @Override
    public synchronized long produce(String topic, int partition, List<ProducerRecord> records) throws IOException {
        return route(Target.LEADER, topic, partition, c -> c.produce(topic, partition, records));
    }

    @Override
    public synchronized FetchResult fetch(String topic, int partition, long offset, int maxBytes) throws IOException {
        return route(Target.LEADER, topic, partition, c -> c.fetch(topic, partition, offset, maxBytes));
    }

    @Override
    public synchronized int createTopic(String topic, int partitions) throws IOException {
        return route(Target.CONTROLLER, topic, 0, c -> c.createTopic(topic, partitions));
    }

    /**
     * The partitions of a topic. Each partition's offsets are taken from its LEADER's answer, because a follower may be
     * slightly behind; if a leader cannot be reached the answering broker's numbers are used.
     */
    @Override
    public synchronized List<PartitionView> describe(String topic) throws IOException {
        ClusterMetadataResponse m = refresh(topic);
        for (ClusterMetadataResponse.Topic t : m.topics()) {
            if (!t.name().equals(topic)) {
                continue;
            }
            Map<Integer, ClusterMetadataResponse> fromLeader = new HashMap<>();
            List<PartitionView> out = new ArrayList<>();
            for (ClusterMetadataResponse.Partition p : t.partitions()) {
                long start = p.logStartOffset();
                long end = p.logEndOffset();
                if (p.leaderId() >= 0) {
                    try {
                        ClusterMetadataResponse lm = fromLeader.get(p.leaderId());
                        if (lm == null) {
                            lm = connectionToBroker(p.leaderId()).clusterMetadata(topic);
                            fromLeader.put(p.leaderId(), lm);
                        }
                        for (ClusterMetadataResponse.Topic lt : lm.topics()) {
                            if (lt.name().equals(topic) && p.partition() < lt.partitions().size()) {
                                start = lt.partitions().get(p.partition()).logStartOffset();
                                end = lt.partitions().get(p.partition()).logEndOffset();
                            }
                        }
                    } catch (IOException e) {
                        dropBroker(p.leaderId());
                    }
                }
                out.add(new PartitionView(p.partition(), p.leaderId(), start, end));
            }
            return out;
        }
        return null;
    }

    @Override
    public synchronized void close() throws IOException {
        closed = true;
        for (String a : new ArrayList<>(byAddress.keySet())) dropConnection(a);
    }
}
