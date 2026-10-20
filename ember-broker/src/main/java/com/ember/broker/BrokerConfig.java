package com.ember.broker;

import com.ember.storage.FlushPolicy;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Every broker setting. Flags look like --name=value (no library needed). For the limit flags the word
 * "never" (or "unlimited") means "this limit is off".
 *
 * Cluster: --cluster=1@host:port,2@host:port,3@host:port (must contain this node; --node-id picks which one is
 * this broker and the listen port is taken from its entry). No --cluster = a cluster of one.
 */
public record BrokerConfig(
        int nodeId,
        String host,                    // the ADVERTISED host, returned by METADATA
        int port,                       // 0 = pick a free port (tests)
        Path dataDir,
        long segmentBytes,
        int indexIntervalBytes,
        long flushMessages,             // legacy flags (Phase 2/3); see dataFlush()
        long flushMs,
        long retentionMs,
        long retentionBytes,
        int maxFrameBytes,
        int maxFetchBytes,
        long idleTimeoutMs,
        boolean autoCreateTopics,
        int handlerThreads,
        long maintenanceIntervalMs,
        int defaultPartitions,          // partitions of an auto-created topic
        int maxPartitionsPerTopic,
        List<ClusterMember> cluster,    // empty = single-broker mode
        long raftElectionMinMs,
        long raftElectionMaxMs,
        long raftHeartbeatMs,
        long produceTimeoutMs,
        int maxPendingProduces,
        int peerQueueSize,
        boolean unsafeNoRaftFsync,
        FlushPolicy dataFlush,          // flush policy of the DATA logs; the Raft logs are the durable truth
        boolean dataFlushExplicit,
        int groupMinSessionMs,          // consumer-group session timeout bounds (Phase 6)
        int groupMaxSessionMs,
        int adminPort,                  // -1 = port + 1000 (disabled if port is 0), 0 = disabled, -2 = any free port (tests)
        String adminHost,
        boolean enableDebugEndpoints,
        String advertiseHost,           // --advertise (null = none given)
        int advertisePort) {

    /** host:port is where the BROKERS reach each other; advertisedHost:advertisedPort is what CLIENTS are told (default: the same). */
    public record ClusterMember(int nodeId, String host, int port, String advertisedHost, int advertisedPort) {
        public ClusterMember(int nodeId, String host, int port) {
            this(nodeId, host, port, host, port);
        }
    }

    public static BrokerConfig defaults() {
        return parse();
    }

    /** Legacy (Phase 2/3 LogManager) flush policy. */
    public FlushPolicy flushPolicy() {
        return new FlushPolicy(flushMessages, flushMs);
    }

    /** The admin port to bind: 0 = none, -2 means "any free port". */
    public int effectiveAdminPort() {
        if (adminPort == -1) {
            return port > 0 ? port + 1000 : 0;
        }
        return adminPort;
    }

    public boolean singleBroker() {
        return cluster.size() <= 1;
    }

    public static BrokerConfig parse(String... args) {
        int nodeId = 0;
        String host = "localhost";
        int port = 9099;
        boolean portGiven = false;
        Path dataDir = Path.of("./ember-data");
        long segmentBytes = 64L * 1024 * 1024;
        int indexIntervalBytes = 4096;
        long flushMessages = Long.MAX_VALUE;
        long flushMs = 1000;
        boolean legacyFlushGiven = false;
        long retentionMs = 7L * 24 * 60 * 60 * 1000;
        long retentionBytes = Long.MAX_VALUE;
        int maxFrameBytes = 8 * 1024 * 1024;
        int maxFetchBytes = 1024 * 1024;
        long idleTimeoutMs = 300_000;
        boolean autoCreateTopics = true;
        int handlerThreads = 16;        // a produce waits for its commit on a handler thread
        long maintenanceIntervalMs = 250;
        int defaultPartitions = 3;
        int maxPartitionsPerTopic = 256;
        List<ClusterMember> cluster = new ArrayList<>();
        long raftElectionMinMs = 300;
        long raftElectionMaxMs = 600;
        long raftHeartbeatMs = 100;
        long produceTimeoutMs = 5000;
        int maxPendingProduces = 1000;
        int peerQueueSize = 10_000;
        boolean unsafeNoRaftFsync = false;
        FlushPolicy dataFlush = FlushPolicy.never();
        boolean dataFlushGiven = false;
        int groupMinSessionMs = 1000;
        int groupMaxSessionMs = 60_000;
        int adminPort = -1;
        String adminHost = "127.0.0.1";
        boolean enableDebugEndpoints = false;
        String advertiseHost = null;
        int advertisePort = 0;

        for (String arg : args) {
            int eq = arg.indexOf('=');
            String name = arg.startsWith("--") ? (eq < 0 ? arg.substring(2) : arg.substring(2, eq)) : "";
            if (arg.equals("--unsafe-no-raft-fsync")) {                 // a bare switch
                unsafeNoRaftFsync = true;
                continue;
            }
            if (arg.equals("--enable-debug-endpoints")) {               // a bare switch: OFF unless asked for
                enableDebugEndpoints = true;
                continue;
            }
            if (!arg.startsWith("--") || eq < 3) {
                throw new IllegalArgumentException("expected --name=value but got: " + arg);
            }
            String value = arg.substring(eq + 1);
            try {
                switch (name) {
                    case "node-id" -> nodeId = Integer.parseInt(value);
                    case "host" -> host = value;
                    case "port" -> {
                        port = Integer.parseInt(value);
                        portGiven = true;
                    }
                    case "data-dir" -> dataDir = Path.of(value);
                    case "segment-bytes" -> segmentBytes = Long.parseLong(value);
                    case "index-interval-bytes" -> indexIntervalBytes = Integer.parseInt(value);
                    case "flush-messages" -> {
                        flushMessages = parseLimit(value);
                        legacyFlushGiven = true;
                    }
                    case "flush-ms" -> {
                        flushMs = parseLimit(value);
                        legacyFlushGiven = true;
                    }
                    case "retention-ms" -> retentionMs = parseLimit(value);
                    case "retention-bytes" -> retentionBytes = parseLimit(value);
                    case "max-frame-bytes" -> maxFrameBytes = Integer.parseInt(value);
                    case "max-fetch-bytes" -> maxFetchBytes = Integer.parseInt(value);
                    case "idle-timeout-ms" -> idleTimeoutMs = Long.parseLong(value);
                    case "auto-create-topics" -> autoCreateTopics = Boolean.parseBoolean(value);
                    case "handler-threads" -> handlerThreads = Integer.parseInt(value);
                    case "maintenance-interval-ms" -> maintenanceIntervalMs = Long.parseLong(value);
                    case "default-partitions" -> defaultPartitions = Integer.parseInt(value);
                    case "max-partitions" -> maxPartitionsPerTopic = Integer.parseInt(value);
                    case "cluster" -> cluster = parseCluster(value);
                    case "raft-election-min-ms" -> raftElectionMinMs = Long.parseLong(value);
                    case "raft-election-max-ms" -> raftElectionMaxMs = Long.parseLong(value);
                    case "raft-heartbeat-ms" -> raftHeartbeatMs = Long.parseLong(value);
                    case "produce-timeout-ms" -> produceTimeoutMs = Long.parseLong(value);
                    case "max-pending-produces" -> maxPendingProduces = Integer.parseInt(value);
                    case "peer-queue-size" -> peerQueueSize = Integer.parseInt(value);
                    case "group-min-session-ms" -> groupMinSessionMs = Integer.parseInt(value);
                    case "group-max-session-ms" -> groupMaxSessionMs = Integer.parseInt(value);
                    case "admin-port" -> adminPort = value.equals("ephemeral") ? -2 : Integer.parseInt(value);
                    case "admin-host" -> adminHost = value;
                    case "advertise" -> {
                        int colon = value.lastIndexOf(':');
                        if (colon < 1) {
                            throw new IllegalArgumentException("--advertise looks like host:port, got: " + value);
                        }
                        advertiseHost = value.substring(0, colon);
                        advertisePort = Integer.parseInt(value.substring(colon + 1));
                    }
                    case "data-flush" -> {
                        dataFlush = parseDataFlush(value);
                        dataFlushGiven = true;
                    }
                    default -> throw new IllegalArgumentException("unknown flag --" + name);
                }
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("bad value for --" + name + ": " + value);
            }
        }
        if (defaultPartitions < 1 || maxPartitionsPerTopic < 1 || defaultPartitions > maxPartitionsPerTopic) {
            throw new IllegalArgumentException("need 1 <= default-partitions <= max-partitions");
        }
        if (raftHeartbeatMs <= 0 || raftElectionMinMs <= raftHeartbeatMs || raftElectionMaxMs < raftElectionMinMs) {
            throw new IllegalArgumentException("need 0 < raft-heartbeat-ms < raft-election-min-ms <= raft-election-max-ms");
        }
        if (groupMinSessionMs < 1 || groupMaxSessionMs < groupMinSessionMs) {
            throw new IllegalArgumentException("need 1 <= group-min-session-ms <= group-max-session-ms");
        }
        if (!dataFlushGiven && legacyFlushGiven) {
            dataFlush = new FlushPolicy(flushMessages, flushMs);       // the Phase 3 flags still steer the data logs
        }
        if (!cluster.isEmpty()) {
            final int self = nodeId;
            ClusterMember me = cluster.stream().filter(m -> m.nodeId() == self).findFirst().orElseThrow(
                    () -> new IllegalArgumentException("--cluster must contain this broker (--node-id=" + self + ")"));
            if (portGiven && port != me.port()) {
                throw new IllegalArgumentException("--port=" + port + " disagrees with this broker's --cluster entry (port " + me.port() + ")");
            }
            port = me.port();
            host = me.host();
            if (advertiseHost != null) {                                  // --advertise overrides this broker's own third part
                List<ClusterMember> rewritten = new ArrayList<>();
                for (ClusterMember m : cluster) {
                    rewritten.add(m.nodeId() == self ? new ClusterMember(m.nodeId(), m.host(), m.port(), advertiseHost, advertisePort) : m);
                }
                cluster = rewritten;
            }
        }
        return new BrokerConfig(nodeId, host, port, dataDir, segmentBytes, indexIntervalBytes, flushMessages, flushMs,
                retentionMs, retentionBytes, maxFrameBytes, maxFetchBytes, idleTimeoutMs, autoCreateTopics, handlerThreads,
                maintenanceIntervalMs, defaultPartitions, maxPartitionsPerTopic, List.copyOf(cluster),
                raftElectionMinMs, raftElectionMaxMs, raftHeartbeatMs, produceTimeoutMs, maxPendingProduces, peerQueueSize,
                unsafeNoRaftFsync, dataFlush, dataFlushGiven || legacyFlushGiven, groupMinSessionMs, groupMaxSessionMs, adminPort, adminHost, enableDebugEndpoints, advertiseHost, advertisePort);
    }

    /** "1@host:9001,2@host:9002" */
    static List<ClusterMember> parseCluster(String value) {
        List<ClusterMember> members = new ArrayList<>();
        for (String entry : value.split(",")) {
            String internal = entry;
            String advertised = null;
            int slash = entry.indexOf('/');
            if (slash >= 0) {                                           // id@host:port/advertisedHost:advertisedPort
                internal = entry.substring(0, slash);
                advertised = entry.substring(slash + 1);
            }
            int at = internal.indexOf('@');
            int colon = internal.lastIndexOf(':');
            if (at < 1 || colon < at + 2 || colon == internal.length() - 1) {
                throw new IllegalArgumentException("bad --cluster entry (expected id@host:port[/advertisedHost:advertisedPort]): " + entry);
            }
            String host = internal.substring(at + 1, colon);
            int port = Integer.parseInt(internal.substring(colon + 1));
            String advHost = host;
            int advPort = port;
            if (advertised != null) {
                int advColon = advertised.lastIndexOf(':');
                if (advColon < 1 || advColon == advertised.length() - 1) {
                    throw new IllegalArgumentException("bad advertised address in --cluster entry (expected host:port): " + entry);
                }
                advHost = advertised.substring(0, advColon);
                advPort = Integer.parseInt(advertised.substring(advColon + 1));
            }
            members.add(new ClusterMember(Integer.parseInt(internal.substring(0, at)), host, port, advHost, advPort));
        }
        members.sort(Comparator.comparingInt(ClusterMember::nodeId));
        for (int i = 1; i < members.size(); i++) {
            if (members.get(i).nodeId() == members.get(i - 1).nodeId()) {
                throw new IllegalArgumentException("duplicate node id in --cluster: " + members.get(i).nodeId());
            }
        }
        return members;
    }

    /** never | message | <N>ms */
    private static FlushPolicy parseDataFlush(String value) {
        if (value.equals("never")) {
            return FlushPolicy.never();
        }
        if (value.equals("message")) {
            return FlushPolicy.everyMessage();
        }
        if (value.endsWith("ms")) {
            return FlushPolicy.afterMillis(Long.parseLong(value.substring(0, value.length() - 2)));
        }
        throw new IllegalArgumentException("--data-flush must be never, message or <N>ms");
    }

    private static long parseLimit(String value) {
        return value.equals("never") || value.equals("unlimited") ? Long.MAX_VALUE : Long.parseLong(value);
    }
}
