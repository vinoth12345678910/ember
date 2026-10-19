package com.ember.broker;

import com.ember.protocol.ProducerRecord;
import com.ember.protocol.TopicNames;
import com.ember.raft.RaftConfig;
import com.ember.raft.RaftMessage;
import com.ember.replication.*;
import com.ember.storage.Log;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Owns the META group (the cluster's topic list) and one Raft group per topic partition, plus the DATA log each
 * replica writes when an entry is committed and applied. Every broker holds every partition (replication factor =
 * cluster size).
 *
 * Layout under dataDir:  __raft/__meta-0, __raft/&lt;topic&gt;-&lt;p&gt; (Raft logs, the source of truth)
 *                        &lt;topic&gt;-&lt;p&gt;                       (data logs, rebuilt from the Raft log if lost)
 */
public final class ReplicaManager implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(ReplicaManager.class.getName());
    public static final String META_TOPIC = "__meta";
    public static final String GROUPS_TOPIC = "__groups";

    /** How a group sends a Raft message to a peer broker. Must never block. */
    public interface RaftSender {
        void send(int toNode, String topic, int partition, RaftMessage message);
    }

    public record PartitionReplica(String topic, int partition, Log dataLog, PartitionStateMachine stateMachine, GroupRuntime group) {
    }

    /** A topic as this broker knows it. partitions() are the local data logs (kept for the tests of earlier phases). */
    public static final class TopicState {
        private final String name;
        private final List<PartitionReplica> replicas;

        TopicState(String name, List<PartitionReplica> replicas) {
            this.name = name;
            this.replicas = List.copyOf(replicas);
        }

        public String name() {
            return name;
        }

        public int partitionCount() {
            return replicas.size();
        }

        public List<PartitionReplica> replicas() {
            return replicas;
        }

        public PartitionReplica replica(int partition) {
            return partition < 0 || partition >= replicas.size() ? null : replicas.get(partition);
        }

        public List<Log> partitions() {
            List<Log> logs = new ArrayList<>();
            for (PartitionReplica r : replicas) logs.add(r.dataLog());
            return logs;
        }
    }

    private final BrokerConfig config;
    private final int selfId;
    private final List<Integer> clusterIds;
    private final Map<Integer, InetSocketAddress> addresses;
    private final RaftSender sender;
    private final Path dataDir;
    private final Path raftDir;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1, r -> daemon(r, "ember-raft-timers"));
    private final ScheduledExecutorService sessions = Executors.newSingleThreadScheduledExecutor(r -> daemon(r, "ember-group-sessions"));
    private final ExecutorService workers = Executors.newFixedThreadPool(4, r -> daemon(r, "ember-raft-worker"));
    private final ScheduledExecutorService maintenance = Executors.newSingleThreadScheduledExecutor(r -> daemon(r, "ember-maintenance"));
    private final Map<String, TopicState> topics = new ConcurrentHashMap<>();
    private final Map<String, PartitionReplica> byKey = new ConcurrentHashMap<>();
    private final MetadataStateMachine metaStateMachine;
    private final GroupRuntime metaGroup;
    private final GroupStateMachine groupState = new GroupStateMachine();
    private final GroupRuntime groupsGroup;
    private final GroupCoordinator coordinator;
    private volatile boolean closed;

    public ReplicaManager(BrokerConfig config, Map<Integer, InetSocketAddress> addresses, RaftSender sender) throws IOException {
        this.config = config;
        this.selfId = config.nodeId();
        this.addresses = new TreeMap<>(addresses);
        this.clusterIds = new ArrayList<>(this.addresses.keySet());
        this.sender = sender;
        this.dataDir = config.dataDir();
        this.raftDir = dataDir.resolve("__raft");
        refuseLegacyLayout();
        Files.createDirectories(raftDir);

        this.metaStateMachine = new MetadataStateMachine(this::createPartitionGroups, config.maxPartitionsPerTopic());
        this.metaGroup = newGroup(META_TOPIC, 0, metaStateMachine);
        metaGroup.start();

        // consumer groups: ONE more Raft group on every broker; its leader is the coordinator for all groups.
        // Created empty when its directory does not exist, so Phase 5 data directories open unchanged.
        this.groupsGroup = newGroup(GROUPS_TOPIC, 0, groupState);
        this.coordinator = new GroupCoordinator("broker " + selfId, GroupLog.of(groupsGroup), groupState,
                topic -> {
                    TopicState t = topics.get(topic);
                    return t == null ? -1 : t.partitionCount();
                },
                GroupRuntime::monotonicMillis, () -> java.util.UUID.randomUUID().toString(),
                new GroupCoordinator.Config(config.groupMinSessionMs(), config.groupMaxSessionMs(), config.produceTimeoutMs(), 5));
        groupsGroup.start();
    }

    private static Thread daemon(Runnable r, String name) {
        Thread t = new Thread(r, name);
        t.setDaemon(true);
        return t;
    }

    /** Phase 2/3 directories hold data without a Raft log: adopting them would invent history. */
    private void refuseLegacyLayout() throws IOException {
        if (!Files.isDirectory(dataDir) || Files.exists(raftDir)) {
            return;
        }
        try (Stream<Path> entries = Files.list(dataDir)) {
            for (Path p : entries.toList()) {
                String name = p.getFileName().toString();
                if (Files.isDirectory(p) && (name.equals("_meta") || (!name.startsWith(".") && !name.startsWith("_")
                        && LogManager.parseDirectoryName(name) != null))) {
                    throw new IllegalStateException("legacy single-node data directory: " + dataDir + " contains '" + name
                            + "' but no __raft directory; Phase 5 needs a fresh data directory");
                }
            }
        }
    }

    private GroupRuntime newGroup(String topic, int partition, StateMachine stateMachine) {
        String name = topic + "-" + partition;
        FileRaftStorage storage = new FileRaftStorage(raftDir.resolve(name), !config.unsafeNoRaftFsync());
        // CheckQuorum on: a leader cut off from the cluster must notice and step down
        RaftConfig raftConfig = new RaftConfig(selfId, clusterIds, config.raftElectionMinMs(), config.raftElectionMaxMs(),
                config.raftHeartbeatMs(), 64, com.ember.raft.Bug.NONE, 1024 * 1024, true);
        // different brokers MUST draw different election timeouts, so the seed mixes in the node id and the time
        Random random = new Random(System.nanoTime() ^ (selfId * 0x9E3779B97F4A7C15L) ^ name.hashCode());
        GroupRuntime group = new GroupRuntime(name + "@" + selfId, raftConfig, storage, (to, m) -> sender.send(to, topic, partition, m),
                stateMachine, random, GroupRuntime::monotonicMillis, scheduler, workers, config.maxPendingProduces());
        group.addListener(new com.ember.raft.RaftListener() {          // role changes are rare: worth a log line each
            @Override
            public void onRoleChange(int nodeId, com.ember.raft.Role role, long term, int leaderId) {
                LOG.log(System.Logger.Level.INFO, "broker {0}: group {1} is now {2} in term {3} (leader {4})", selfId, name, role, term, leaderId);
            }

            @Override
            public void onCommitted(int nodeId, long index, com.ember.raft.LogEntry entry) {
            }
        });
        return group;
    }

    /** Called when a CREATE_TOPIC entry is applied (also during replay at startup): idempotent. */
    private synchronized void createPartitionGroups(String name, int count) {
        if (closed || topics.containsKey(name)) {
            return;
        }
        List<PartitionReplica> replicas = new ArrayList<>(count);
        try {
            for (int p = 0; p < count; p++) {
                Log log = new Log(dataDir.resolve(name + "-" + p), config.segmentBytes(), config.indexIntervalBytes(), config.dataFlush());
                PartitionStateMachine sm = new PartitionStateMachine(name + "-" + p, log);
                GroupRuntime group = newGroup(name, p, sm);
                replicas.add(new PartitionReplica(name, p, log, sm, group));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        for (PartitionReplica r : replicas) {
            byKey.put(name + "-" + r.partition(), r);
            r.group().start();
        }
        topics.put(name, new TopicState(name, replicas));         // published only when complete
        LOG.log(System.Logger.Level.INFO, "broker {0}: created topic {1} with {2} partition(s)", selfId, name, count);
    }

    // ------------------------------------------------------------------ lookups

    public TopicState topic(String name) {
        return topics.get(name);
    }

    public Log logFor(String topic, int partition) {
        TopicState t = topics.get(topic);
        PartitionReplica r = t == null ? null : t.replica(partition);
        return r == null ? null : r.dataLog();
    }

    public List<TopicState> topics() {
        List<TopicState> all = new ArrayList<>(topics.values());
        all.sort((a, b) -> a.name().compareTo(b.name()));
        return all;
    }

    public GroupRuntime metaGroup() {
        return metaGroup;
    }

    public GroupRuntime groupsGroup() {
        return groupsGroup;
    }

    public GroupCoordinator coordinator() {
        return coordinator;
    }

    /** Test hook: the replicated state of all consumer groups on THIS broker (compare snapshots across brokers). */
    public GroupStateMachine groupState() {
        return groupState;
    }

    /** The leader of "__groups" (this broker included) or -1. */
    public int coordinatorId() {
        return groupsGroup.isLeader() ? selfId : groupsGroup.leaderId();
    }

    public boolean isController() {
        return metaGroup.isLeader();
    }

    public int controllerId() {
        return metaGroup.isLeader() ? selfId : metaGroup.leaderId();
    }

    /** True when the cluster that was actually started has just this broker (the addresses, not the flags, decide). */
    public boolean isSingleBroker() {
        return clusterIds.size() == 1;
    }

    public int selfId() {
        return selfId;
    }

    public List<Integer> clusterIds() {
        return clusterIds;
    }

    public Map<Integer, InetSocketAddress> addresses() {
        return addresses;
    }

    /** Raft messages from peers: dropped silently if the group is not known here (yet). */
    public void deliver(String topic, int partition, int fromNode, RaftMessage message) {
        if (closed) {
            return;
        }
        if (topic.equals(META_TOPIC) && partition == 0) {
            metaGroup.deliver(fromNode, message);
            return;
        }
        if (topic.equals(GROUPS_TOPIC) && partition == 0) {
            groupsGroup.deliver(fromNode, message);
            return;
        }
        PartitionReplica r = byKey.get(topic + "-" + partition);
        if (r != null) {
            r.group().deliver(fromNode, message);
        } else {
            LOG.log(System.Logger.Level.DEBUG, "dropping raft message for unknown group {0}-{1}", topic, partition);
        }
    }

    // ------------------------------------------------------------------ topic creation

    /** Proposes CREATE_TOPIC through the META group. Completes with MetadataStateMachine.CreateResult. */
    public CompletableFuture<Object> createTopic(String name, int partitions) {
        return metaGroup.propose(Commands.createTopic(name, partitions), config.produceTimeoutMs());
    }

    public static boolean isReservedName(String topic) {
        return topic.startsWith("__");
    }

    public static boolean isValidClientTopic(String topic) {
        return TopicNames.isValid(topic) && !isReservedName(topic);
    }

    // ------------------------------------------------------------------ waiting (single-broker convenience)

    /** True once this broker leads the META group and has applied everything committed. */
    public boolean awaitMetaLeaderReady(long timeoutMs) {
        long deadline = System.nanoTime() + timeoutMs * 1_000_000L;
        while (!metaGroup.isLeaderReady()) {
            if (System.nanoTime() > deadline) {
                return false;
            }
            java.util.concurrent.locks.LockSupport.parkNanos(1_000_000);
        }
        return true;
    }

    // ------------------------------------------------------------------ maintenance

    /** fsync per the data-log flush policy, then retention, on every partition replica. */
    public void runMaintenance(long nowMs) {
        for (TopicState t : topics.values()) {
            for (PartitionReplica r : t.replicas()) {
                try {
                    r.dataLog().maybeFlush(nowMs);
                    r.dataLog().deleteOldSegments(nowMs, config.retentionMs(), config.retentionBytes());
                } catch (IOException | RuntimeException e) {
                    LOG.log(System.Logger.Level.ERROR, "maintenance failed for " + t.name() + "-" + r.partition(), e);
                }
            }
        }
    }

    public void startMaintenance(Clock clock) {
        maintenance.scheduleWithFixedDelay(() -> runMaintenance(clock.millis()),
                config.maintenanceIntervalMs(), config.maintenanceIntervalMs(), TimeUnit.MILLISECONDS);
        scheduleSessionExpiry();
    }

    /** Session expiry runs on its own cadence (a quarter of the shortest session in use), on its own thread. */
    private void scheduleSessionExpiry() {
        if (closed) {
            return;
        }
        try {
            sessions.schedule(() -> {
                try {
                    coordinator.expireSessions();
                } catch (RuntimeException e) {
                    LOG.log(System.Logger.Level.ERROR, "session expiry failed", e);
                }
                scheduleSessionExpiry();
            }, coordinator.tickIntervalMs(), TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.RejectedExecutionException e) {
            // shutting down
        }
    }

    // ------------------------------------------------------------------ shutdown

    /** Graceful: stop maintenance, close every group (storage included), then the data logs. */
    @Override
    public synchronized void close() {
        stop(true);
    }

    /** Test hook: stop everything abruptly (no goodbyes; data logs are closed only to release file handles). */
    public synchronized void crash() {
        stop(false);
    }

    private void stop(boolean graceful) {
        if (closed) {
            return;
        }
        closed = true;
        maintenance.shutdownNow();
        sessions.shutdownNow();
        for (TopicState t : topics.values()) {
            for (PartitionReplica r : t.replicas()) {
                if (graceful) r.group().close(); else r.group().abort();
            }
        }
        if (graceful) groupsGroup.close(); else groupsGroup.abort();
        if (graceful) metaGroup.close(); else metaGroup.abort();
        for (TopicState t : topics.values()) {
            for (PartitionReplica r : t.replicas()) {
                try {
                    r.dataLog().close();
                } catch (IOException e) {
                    LOG.log(System.Logger.Level.WARNING, "closing data log of " + t.name() + "-" + r.partition(), e);
                }
            }
        }
        scheduler.shutdownNow();
        workers.shutdownNow();
    }

    // ------------------------------------------------------------------ helpers for BrokerApis

    static byte[] produceCommand(long timestamp, List<ProducerRecord> records) {
        return Commands.produceBatch(timestamp, records);
    }
}
