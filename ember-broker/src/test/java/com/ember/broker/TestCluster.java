package com.ember.broker;

import com.ember.storage.LogRecord;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * N real brokers in one JVM on ephemeral ports: bind them all, then start them with the full address map. A broker
 * that is stopped, crashed and restarted keeps its data directory AND its port (the others reconnect to it).
 */
final class TestCluster implements AutoCloseable {

    final Path base;
    final int n;
    final String[] extraFlags;
    final Clock clock;
    final BrokerServer[] servers;
    /** One injector per broker (nothing injected until a test configures it); the same object survives a restart. */
    final com.ember.replication.ConfigurableFaults[] faults;
    final int[] ports;
    final Map<Integer, InetSocketAddress> addresses = new TreeMap<>();
    final Map<Integer, InetSocketAddress> advertised = new TreeMap<>();

    TestCluster(Path base, int n, boolean fsync, String... extraFlags) throws Exception {
        this(base, n, fsync, Clock.systemUTC(), extraFlags);
    }

    TestCluster(Path base, int n, boolean fsync, Clock clock, String... extraFlags) throws Exception {
        this(base, n, fsync, clock, null, extraFlags);
    }

    /** advertisedOverrides: nodeId -> the address CLIENTS are told for that broker (the others keep their real address). */
    TestCluster(Path base, int n, boolean fsync, Map<Integer, InetSocketAddress> advertisedOverrides, String... extraFlags) throws Exception {
        this(base, n, fsync, Clock.systemUTC(), advertisedOverrides, extraFlags);
    }

    private TestCluster(Path base, int n, boolean fsync, Clock clock, Map<Integer, InetSocketAddress> advertisedOverrides, String... extraFlags) throws Exception {
        this.base = base;
        this.n = n;
        this.clock = clock;
        List<String> flags = new ArrayList<>(List.of(extraFlags));
        if (!fsync) flags.add("--unsafe-no-raft-fsync");
        this.extraFlags = flags.toArray(new String[0]);
        this.servers = new BrokerServer[n + 1];
        this.faults = new com.ember.replication.ConfigurableFaults[n + 1];
        for (int i = 1; i <= n; i++) faults[i] = new com.ember.replication.ConfigurableFaults();
        this.ports = new int[n + 1];
        for (int i = 1; i <= n; i++) {                       // phase 1: everybody binds, so every port is known
            servers[i] = new BrokerServer(config(i), clock, faults[i]);
            ports[i] = servers[i].bind();
            addresses.put(i, new InetSocketAddress("localhost", ports[i]));
        }
        this.advertised.putAll(addresses);
        if (advertisedOverrides != null) this.advertised.putAll(advertisedOverrides);
        for (int i = 1; i <= n; i++) {                       // phase 2: everybody joins
            servers[i].start(addresses, advertised);
        }
    }

    BrokerConfig config(int i) {
        List<String> flags = new ArrayList<>(List.of(
                "--node-id=" + i, "--port=" + ports[i], "--data-dir=" + base.resolve("broker-" + i),
                "--segment-bytes=4096", "--index-interval-bytes=256", "--maintenance-interval-ms=3600000",
                "--raft-election-min-ms=150", "--raft-election-max-ms=300", "--raft-heartbeat-ms=30",
                "--handler-threads=16", "--produce-timeout-ms=5000", "--data-flush=never", "--group-min-session-ms=100", "--admin-port=ephemeral"));
        flags.addAll(List.of(extraFlags));
        return BrokerConfig.parse(flags.toArray(new String[0]));
    }

    boolean isUp(int i) {
        return servers[i] != null;
    }

    /** Graceful stop. */
    void stop(int i) {
        if (servers[i] != null) {
            servers[i].close();
            servers[i] = null;
        }
    }

    /** Abrupt death (see BrokerServer.crash()). */
    void crash(int i) {
        if (servers[i] != null) {
            servers[i].crash();
            servers[i] = null;
        }
    }

    /** Starts a stopped/crashed broker again on the same data directory and the same port. */
    void restart(int i) throws Exception {
        if (servers[i] != null) return;
        BrokerServer s = new BrokerServer(config(i), clock, faults[i]);
        int port = s.bind();
        if (port != ports[i]) {
            throw new IllegalStateException("broker " + i + " came back on port " + port + " instead of " + ports[i]);
        }
        s.start(addresses, advertised);
        servers[i] = s;
    }

    /** The admin HTTP port of a broker that is up. */
    int adminPort(int i) {
        return servers[i].adminPort();
    }

    int port(int i) {
        return ports[i];
    }

    String bootstrap() {
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= n; i++) {
            if (sb.length() > 0) sb.append(',');
            sb.append("localhost:").append(ports[i]);
        }
        return sb.toString();
    }

    // ---- who leads what

    /** The up broker that is the READY leader of the partition, or -1. */
    int leaderOf(String topic, int partition) {
        for (int i = 1; i <= n; i++) {
            if (servers[i] == null) continue;
            var t = servers[i].replicaManager().topic(topic);
            var r = t == null ? null : t.replica(partition);
            if (r != null && r.group().isLeaderReady()) return i;
        }
        return -1;
    }

    int awaitLeader(String topic, int partition) {
        Await.until(20_000, "a leader for " + topic + "-" + partition, () -> leaderOf(topic, partition) > 0);
        return leaderOf(topic, partition);
    }

    int controller() {
        for (int i = 1; i <= n; i++) {
            if (servers[i] != null && servers[i].replicaManager().metaGroup().isLeaderReady()) return i;
        }
        return -1;
    }

    int awaitController() {
        Await.until(20_000, "a controller (META leader)", () -> controller() > 0);
        return controller();
    }

    /** Waits until every UP broker has applied the topic's creation. */
    void awaitTopicEverywhere(String topic, int partitions) {
        Await.until(20_000, "topic " + topic + " to exist on every up broker", () -> {
            for (int i = 1; i <= n; i++) {
                if (servers[i] == null) continue;
                var t = servers[i].replicaManager().topic(topic);
                if (t == null || t.partitionCount() != partitions) return false;
            }
            return true;
        });
    }

    // ---- consumer groups

    /** The up broker that is the READY leader of "__groups" (the coordinator), or -1. */
    int coordinator() {
        for (int i = 1; i <= n; i++) {
            if (servers[i] != null && servers[i].replicaManager().groupsGroup().isLeaderReady()) return i;
        }
        return -1;
    }

    int awaitCoordinator() {
        Await.until(20_000, "a coordinator (leader of __groups)\n" + describe(), () -> coordinator() > 0);
        return coordinator();
    }

    /** Waits until all up brokers hold the same replicated group state, and returns it (for assertions). */
    java.util.Map<String, com.ember.replication.GroupStateMachine.GroupData> awaitGroupStateIdentical() {
        Await.until(30_000, "identical __groups state on every up broker\n" + describe(), () -> {
            java.util.Map<String, com.ember.replication.GroupStateMachine.GroupData> reference = null;
            for (int i = 1; i <= n; i++) {
                if (servers[i] == null) continue;
                var snapshot = servers[i].replicaManager().groupState().snapshot();
                if (reference == null) reference = snapshot;
                else if (!reference.equals(snapshot)) return false;
            }
            return true;
        });
        for (int i = 1; i <= n; i++) {
            if (servers[i] != null) return servers[i].replicaManager().groupState().snapshot();
        }
        return java.util.Map.of();
    }

    // ---- network faults

    void isolate(int i) {
        servers[i].isolate();
    }

    void heal() {
        for (int i = 1; i <= n; i++) {
            if (servers[i] != null) servers[i].heal();
        }
    }

    // ---- reading replicas directly

    List<String> summary(int broker, String topic, int partition) throws IOException {
        var log = servers[broker].replicaManager().logFor(topic, partition);
        List<String> out = new ArrayList<>();
        if (log == null) return out;
        for (long o = log.startOffset(); o < log.nextOffset(); o++) {
            LogRecord r = log.read(o);
            out.add(r.offset() + ":" + r.timestamp() + ":" + (r.key() == null ? "null" : new String(r.key())) + ":" + new String(r.value()));
        }
        return out;
    }

    /** All segment files of the replica's data log, concatenated. */
    byte[] dataLogBytes(int broker, String topic, int partition) throws IOException {
        servers[broker].replicaManager().logFor(topic, partition).flush();
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        Path dir = base.resolve("broker-" + broker).resolve(topic + "-" + partition);
        try (var files = Files.list(dir)) {
            for (Path p : files.filter(f -> f.toString().endsWith(".log")).sorted().toList()) out.write(Files.readAllBytes(p));
        }
        return out.toByteArray();
    }

    /** A human-readable picture of every group on every up broker, for failure messages. */
    String describe() {
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= n; i++) {
            if (servers[i] == null) {
                sb.append("broker ").append(i).append(": DOWN\n");
                continue;
            }
            var rm = servers[i].replicaManager();
            var m = rm.metaGroup();
            sb.append("broker ").append(i).append(" meta: ").append(m.role()).append(" term=").append(m.term()).append(" leader=")
                    .append(m.leaderId()).append(" commit=").append(m.commitIndex()).append(" applied=").append(m.lastApplied())
                    .append(" last=").append(m.lastLogIndex()).append(" failed=").append(m.hasFailed()).append('\n');
            var gg = rm.groupsGroup();
            sb.append("   __groups: ").append(gg.role()).append(" term=").append(gg.term()).append(" leader=").append(gg.leaderId())
                    .append(" commit=").append(gg.commitIndex()).append(" applied=").append(gg.lastApplied()).append(" ready=").append(gg.isLeaderReady()).append('\n');
            rm.groupState().snapshot().forEach((id, data) -> {
                sb.append("   group ").append(id).append(" generation=").append(data.generation()).append(" members={");
                data.members().forEach((mid, member) -> sb.append(mid).append("->").append(member.assigned().size()).append("p "));
                sb.append("} offsets=").append(data.offsets()).append('\n');
            });
            for (var t : rm.topics()) {
                for (var r : t.replicas()) {
                    var g = r.group();
                    sb.append("   ").append(t.name()).append('-').append(r.partition()).append(": ").append(g.role()).append(" term=")
                            .append(g.term()).append(" leader=").append(g.leaderId()).append(" commit=").append(g.commitIndex())
                            .append(" applied=").append(g.lastApplied()).append(" last=").append(g.lastLogIndex())
                            .append(" ready=").append(g.isLeaderReady()).append(" failed=").append(g.hasFailed()).append('\n');
                }
            }
        }
        return sb.toString();
    }

    @Override
    public void close() {
        for (int i = 1; i <= n; i++) {
            if (servers[i] != null) {
                try {
                    servers[i].close();
                } catch (RuntimeException ignored) {
                    // best effort
                }
                servers[i] = null;
            }
        }
    }
}
