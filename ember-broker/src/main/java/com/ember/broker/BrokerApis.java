package com.ember.broker;

import com.ember.protocol.*;
import com.ember.replication.Commands;
import com.ember.replication.GroupRuntime;
import com.ember.replication.MetadataStateMachine;
import com.ember.storage.Log;
import com.ember.storage.LogSlice;
import com.ember.storage.OffsetOutOfRangeException;

import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * What the broker DOES with each client request in a replicated cluster. No Netty types.
 *
 * PRODUCE : only the partition's Raft leader; one PRODUCE_BATCH entry; the reply waits until the entry is COMMITTED and
 *           APPLIED on this broker (never earlier).
 * FETCH   : only the partition's leader, only from the data log, which only ever holds applied (committed) entries.
 * CREATE_TOPIC : only the controller (leader of the META group).
 */
public class BrokerApis {

    public record Outcome<T>(ErrorCode error, T body) {
        static <T> Outcome<T> ok(T body) {
            return new Outcome<>(ErrorCode.NONE, body);
        }

        static <T> Outcome<T> error(ErrorCode code) {
            return new Outcome<>(code, null);
        }
    }

    /** slice is null when error != NONE. The caller MUST close the slice (unless it hands it to Netty). */
    public record FetchOutcome(ErrorCode error, String topic, int partition, LogSlice slice) {
        public long logStartOffset() {
            return slice.logStartOffset;
        }

        public long highWatermark() {
            return slice.logEndOffset;      // the data log's end = the committed end on this leader
        }
    }

    private final BrokerConfig config;
    private final ReplicaManager replicas;
    private final Clock clock;
    private final java.util.function.IntSupplier advertisedPort;

    public BrokerApis(BrokerConfig config, ReplicaManager replicas, Clock clock, java.util.function.IntSupplier advertisedPort) {
        this.config = config;
        this.replicas = replicas;
        this.clock = clock;
        this.advertisedPort = advertisedPort;
    }

    // ------------------------------------------------------------------ PRODUCE

    public Outcome<ProduceResponse> handleProduce(ProduceRequest request) {
        if (!ReplicaManager.isValidClientTopic(request.topic()) || request.records().isEmpty()) {
            return Outcome.error(ErrorCode.INVALID_REQUEST);
        }
        ReplicaManager.TopicState topic = replicas.topic(request.topic());
        if (topic == null) {
            topic = autoCreate(request);
            if (topic == null) {
                return Outcome.error(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION);
            }
        }
        ReplicaManager.PartitionReplica replica = topic.replica(request.partition());
        if (replica == null) {
            return Outcome.error(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION);
        }
        GroupRuntime group = replica.group();
        waitForLeaderIfAlone(group);
        if (!group.isLeader()) {
            return Outcome.error(group.leaderId() >= 0 ? ErrorCode.NOT_LEADER_OR_FOLLOWER : ErrorCode.LEADER_NOT_AVAILABLE);
        }
        // the leader's clock stamps the batch INSIDE the command, so every replica stores the same timestamp
        byte[] command = Commands.produceBatch(clock.millis(), request.records());
        try {
            Object baseOffset = group.propose(command, config.produceTimeoutMs()).get(config.produceTimeoutMs() + 2000, TimeUnit.MILLISECONDS);
            return Outcome.ok(new ProduceResponse(request.topic(), request.partition(), (Long) baseOffset, request.records().size()));
        } catch (ExecutionException e) {
            return Outcome.error(mapFailure(e.getCause(), ErrorCode.NOT_LEADER_OR_FOLLOWER));
        } catch (TimeoutException e) {
            return Outcome.error(ErrorCode.REQUEST_TIMED_OUT);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Outcome.error(ErrorCode.INTERNAL_ERROR);
        }
    }

    /** Auto-creation works only on the controller and only for partitions below defaultPartitions (3.4). */
    private ReplicaManager.TopicState autoCreate(ProduceRequest request) {
        if (!config.autoCreateTopics() || request.partition() < 0 || request.partition() >= config.defaultPartitions()) {
            return null;
        }
        if (replicas.isSingleBroker()) {
            replicas.awaitMetaLeaderReady(config.produceTimeoutMs());
        }
        if (!replicas.isController()) {
            return null;
        }
        try {
            replicas.createTopic(request.topic(), config.defaultPartitions()).get(config.produceTimeoutMs() + 2000, TimeUnit.MILLISECONDS);
        } catch (ExecutionException | TimeoutException e) {
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
        return replicas.topic(request.topic());
    }

    private static ErrorCode mapFailure(Throwable cause, ErrorCode notLeaderCode) {
        if (cause instanceof GroupRuntime.NotLeaderException) {
            return notLeaderCode;
        }
        if (cause instanceof GroupRuntime.OverloadedException) {
            return ErrorCode.BROKER_OVERLOADED;
        }
        if (cause instanceof GroupRuntime.ProposeTimeoutException) {
            return ErrorCode.REQUEST_TIMED_OUT;
        }
        return ErrorCode.INTERNAL_ERROR;
    }

    /**
     * A broker that is the only member of its cluster will win the election by itself within an election timeout, so it
     * waits for it (bounded). With peers an election may take long or fail: the client gets a retriable error instead.
     */
    private void waitForLeaderIfAlone(GroupRuntime group) {
        if (!replicas.isSingleBroker() || group.isLeaderReady()) {
            return;
        }
        long deadline = System.nanoTime() + config.produceTimeoutMs() * 1_000_000L;
        while (!group.isLeaderReady() && System.nanoTime() < deadline) {
            java.util.concurrent.locks.LockSupport.parkNanos(1_000_000);
        }
    }

    // ------------------------------------------------------------------ FETCH

    public FetchOutcome handleFetch(FetchRequest request) throws IOException {
        ReplicaManager.TopicState topic = replicas.topic(request.topic());
        ReplicaManager.PartitionReplica replica = topic == null ? null : topic.replica(request.partition());
        if (replica == null) {
            return new FetchOutcome(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, request.topic(), request.partition(), null);
        }
        GroupRuntime group = replica.group();
        waitForLeaderIfAlone(group);
        if (!group.isLeaderReady()) {
            // either a follower, or a leader that has not yet applied everything the previous leaders committed
            ErrorCode code = group.leaderId() >= 0 && !group.isLeader() ? ErrorCode.NOT_LEADER_OR_FOLLOWER : ErrorCode.LEADER_NOT_AVAILABLE;
            return new FetchOutcome(code, request.topic(), request.partition(), null);
        }
        int maxBytes = Math.min(request.maxBytes(), config.maxFetchBytes());
        try {
            LogSlice slice = replica.dataLog().openSlice(request.fetchOffset(), maxBytes);
            return new FetchOutcome(ErrorCode.NONE, request.topic(), request.partition(), slice);
        } catch (OffsetOutOfRangeException e) {
            return new FetchOutcome(ErrorCode.OFFSET_OUT_OF_RANGE, request.topic(), request.partition(), null);
        }
    }

    // ------------------------------------------------------------------ CREATE_TOPIC

    public Outcome<CreateTopicResponse> handleCreateTopic(CreateTopicRequest request) {
        if (!ReplicaManager.isValidClientTopic(request.topic())
                || request.partitionCount() < 1 || request.partitionCount() > config.maxPartitionsPerTopic()) {
            return Outcome.error(ErrorCode.INVALID_REQUEST);
        }
        if (replicas.isSingleBroker()) {
            replicas.awaitMetaLeaderReady(config.produceTimeoutMs());
        }
        if (!replicas.isController()) {
            return Outcome.error(ErrorCode.NOT_CONTROLLER);
        }
        try {
            Object result = replicas.createTopic(request.topic(), request.partitionCount())
                    .get(config.produceTimeoutMs() + 2000, TimeUnit.MILLISECONDS);
            return switch ((MetadataStateMachine.CreateResult) result) {
                case CREATED -> Outcome.ok(new CreateTopicResponse(request.topic(), request.partitionCount()));
                case EXISTS -> Outcome.error(ErrorCode.TOPIC_ALREADY_EXISTS);
                case INVALID -> Outcome.error(ErrorCode.INVALID_REQUEST);
            };
        } catch (ExecutionException e) {
            return Outcome.error(mapFailure(e.getCause(), ErrorCode.NOT_CONTROLLER));
        } catch (TimeoutException e) {
            return Outcome.error(ErrorCode.REQUEST_TIMED_OUT);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Outcome.error(ErrorCode.INTERNAL_ERROR);
        }
    }

    // ------------------------------------------------------------------ METADATA

    /** Versions 0 and 1: about THIS broker. v1 reports the real Raft leader of each partition (or -1). */
    public Outcome<MetadataResponse> handleMetadata(MetadataRequest request, short apiVersion) {
        List<ReplicaManager.TopicState> wanted = wantedTopics(request);
        List<MetadataResponse.TopicMetadata> topics = new ArrayList<>();
        for (ReplicaManager.TopicState topic : wanted) {
            List<MetadataResponse.PartitionMetadata> partitions = new ArrayList<>();
            for (ReplicaManager.PartitionReplica r : topic.replicas()) {
                int leader = apiVersion >= 1 ? r.group().leaderId() : MetadataResponse.PartitionMetadata.NO_LEADER;
                partitions.add(new MetadataResponse.PartitionMetadata(r.partition(), leader, r.dataLog().startOffset(), r.dataLog().nextOffset()));
            }
            topics.add(new MetadataResponse.TopicMetadata(topic.name(), partitions));
        }
        return Outcome.ok(new MetadataResponse(
                new MetadataResponse.BrokerInfo(config.nodeId(), config.host(), advertisedPort.getAsInt()), topics));
    }

    /** Version 2: the whole cluster as seen from here. */
    public Outcome<ClusterMetadataResponse> handleClusterMetadata(MetadataRequest request) {
        List<ClusterMetadataResponse.Broker> brokers = new ArrayList<>();
        for (var e : replicas.addresses().entrySet()) {
            brokers.add(new ClusterMetadataResponse.Broker(e.getKey(), e.getValue().getHostString(), e.getValue().getPort()));
        }
        List<ClusterMetadataResponse.Topic> topics = new ArrayList<>();
        for (ReplicaManager.TopicState topic : wantedTopics(request)) {
            List<ClusterMetadataResponse.Partition> partitions = new ArrayList<>();
            for (ReplicaManager.PartitionReplica r : topic.replicas()) {
                partitions.add(new ClusterMetadataResponse.Partition(r.partition(), r.group().leaderId(), replicas.clusterIds(),
                        r.dataLog().startOffset(), r.dataLog().nextOffset()));
            }
            topics.add(new ClusterMetadataResponse.Topic(topic.name(), partitions));
        }
        return Outcome.ok(new ClusterMetadataResponse(brokers, replicas.controllerId(), topics));
    }

    private List<ReplicaManager.TopicState> wantedTopics(MetadataRequest request) {
        List<ReplicaManager.TopicState> wanted = new ArrayList<>();
        if (request.topics().isEmpty()) {
            wanted.addAll(replicas.topics());
        } else {
            for (String name : new TreeSet<>(request.topics())) {
                ReplicaManager.TopicState t = replicas.topic(name);
                if (t != null) wanted.add(t);               // unknown topics are simply omitted
            }
        }
        return wanted;
    }

    Log dataLog(String topic, int partition) {
        return replicas.logFor(topic, partition);
    }
}
