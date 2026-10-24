package com.ember.chaos;

import com.ember.broker.ReplicaManager;
import com.ember.raft.LogEntry;
import com.ember.replication.FileRaftStorage;
import com.ember.replication.GroupRuntime;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

import static com.ember.chaos.HistoryRecorder.data;

/** Reads the replicas of an InProcessCluster directly: data logs, applied group/topic state, and (after a stop) the Raft logs. */
public final class InProcessCollector implements Collector {

    private final InProcessCluster cluster;
    private final String topic;
    private final int partitions;
    private final Map<String, Long> committedAtCollection = new LinkedHashMap<>();   // Raft group -> min commit index over replicas

    public InProcessCollector(InProcessCluster cluster, String topic, int partitions) {
        this.cluster = cluster;
        this.topic = topic;
        this.partitions = partitions;
    }

    private List<GroupRuntime> groupsOf(int broker) {
        ReplicaManager rm = cluster.server(broker).replicaManager();
        List<GroupRuntime> out = new ArrayList<>(List.of(rm.metaGroup(), rm.groupsGroup(), rm.acksGroup()));
        ReplicaManager.TopicState t = rm.topic(topic);
        if (t != null) for (ReplicaManager.PartitionReplica r : t.replicas()) out.add(r.group());
        return out;
    }

    @Override
    public boolean awaitConverged(long timeoutMs) {
        return InProcessCluster.await(timeoutMs, () -> {
            try {
                String reference = null;
                for (int i = 1; i <= cluster.n; i++) {
                    if (!cluster.isUp(i)) return false;
                    StringBuilder fingerprint = new StringBuilder();
                    for (GroupRuntime g : groupsOf(i)) {
                        if (g.lastApplied() < g.commitIndex() || g.commitIndex() < 1) return false;
                    }
                    for (int p = 0; p < partitions; p++) {
                        var log = cluster.server(i).replicaManager().logFor(topic, p);
                        if (log == null) return false;
                        fingerprint.append(log.startOffset()).append('-').append(log.nextOffset()).append(';');
                    }
                    fingerprint.append(cluster.server(i).replicaManager().groupState().snapshot());
                    fingerprint.append('|').append(cluster.server(i).replicaManager().ackState().fingerprint());
                    if (reference == null) reference = fingerprint.toString();
                    else if (!reference.equals(fingerprint.toString())) return false;
                }
                return true;
            } catch (RuntimeException e) {
                return false;
            }
        });
    }

    @Override
    public void collect(HistoryRecorder rec) throws Exception {
        for (int i = 1; i <= cluster.n; i++) {
            ReplicaManager rm = cluster.server(i).replicaManager();
            for (int p = 0; p < partitions; p++) {
                LogDigest.Digest d = LogDigest.of(rm.logFor(topic, p));
                rec.replica(data("scope", "data", "group", topic + "-" + p, "replica", Integer.toString(i), "start", d.start(), "end", d.end(), "crc", d.crc()));
            }
            rec.replica(data("scope", "groupstate", "group", "groups", "replica", Integer.toString(i), "digest", crc(rm.groupState().snapshot().toString())));
            rec.replica(data("scope", "ackstate", "group", "acks", "replica", Integer.toString(i), "digest", crc(rm.ackState().fingerprint())));
            StringBuilder meta = new StringBuilder();
            for (ReplicaManager.TopicState t : rm.topics()) meta.append(t.name()).append('=').append(t.partitionCount()).append(';');
            rec.replica(data("scope", "metastate", "group", "meta", "replica", Integer.toString(i), "digest", crc(meta.toString())));
            for (GroupRuntime g : groupsOf(i)) {
                String group = g.name().substring(0, g.name().indexOf('@'));
                committedAtCollection.merge(group, g.commitIndex(), Math::min);
            }
        }
    }

    /** Call AFTER the cluster was stopped gracefully: opens each replica's Raft log files and digests the committed prefix. */
    public void collectRaftLogs(HistoryRecorder rec) {
        for (Map.Entry<String, Long> group : committedAtCollection.entrySet()) {
            long upTo = group.getValue();
            for (int i = 1; i <= cluster.n; i++) {
                Path dir = cluster.dataDir(i).resolve("__raft").resolve(group.getKey());
                try (FileRaftStorage storage = new FileRaftStorage(dir, false)) {
                    CRC32 chain = new CRC32();
                    StringBuilder terms = new StringBuilder();
                    long previousTerm = -1;
                    for (long index = 1; index <= upTo; ) {
                        List<LogEntry> batch = storage.entries(index, 256);
                        if (batch.isEmpty()) break;
                        for (LogEntry e : batch) {
                            if (index > upTo) break;
                            if (e.term() != previousTerm) {
                                terms.append(terms.length() == 0 ? "" : ",").append(index).append(':').append(e.term());
                                previousTerm = e.term();
                            }
                            chain.update(ByteBuffer.allocate(9).putLong(e.term()).put((byte) e.type().ordinal()).array());
                            chain.update(e.command());
                            index++;
                        }
                    }
                    rec.replica(data("scope", "raftlog", "group", group.getKey(), "replica", Integer.toString(i), "index", upTo, "digest", chain.getValue(),
                            "terms", terms.toString()));
                }
            }
        }
    }

    static long crc(String text) {
        CRC32 crc = new CRC32();
        crc.update(text.getBytes(StandardCharsets.UTF_8));
        return crc.getValue();
    }
}
