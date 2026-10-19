package com.ember.broker;

import com.ember.protocol.*;
import com.ember.storage.KeyValue;
import com.ember.storage.Log;
import com.ember.storage.LogSlice;
import com.ember.storage.OffsetOutOfRangeException;

import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.function.IntSupplier;

/**
 * LEGACY (Phase 2/3): single-node request logic over the old LogManager. No longer used by the server since Phase 5
 * (see BrokerApis); kept, with its unit tests, as the executable record of the single-node semantics.
 *
 * What the broker DOES with each request. No sockets, no Netty types: pure input -> output,
 * so every rule can be unit-tested with a fake clock. IOExceptions propagate; the network
 * layer turns them into INTERNAL_ERROR.
 */
public class LocalBrokerApis {

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
            return slice.logEndOffset;      // Phase 2: = log end offset. Phase 5: replicated commit point.
        }
    }

    private final BrokerConfig config;
    private final LogManager logs;
    private final Clock clock;
    private final IntSupplier advertisedPort;

    public LocalBrokerApis(BrokerConfig config, LogManager logs, Clock clock, IntSupplier advertisedPort) {
        this.config = config;
        this.logs = logs;
        this.clock = clock;
        this.advertisedPort = advertisedPort;
    }

    public Outcome<ProduceResponse> handleProduce(ProduceRequest request) throws IOException {
        if (!TopicNames.isValid(request.topic()) || request.records().isEmpty()) {
            return Outcome.error(ErrorCode.INVALID_REQUEST);
        }
        // creates the topic (with defaultPartitions) when auto-creation applies; null = no such topic/partition
        Log log = logs.getOrAutoCreate(request.topic(), request.partition());
        if (log == null) {
            return Outcome.error(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION);
        }
        List<KeyValue> batch = new ArrayList<>(request.records().size());
        for (ProducerRecord r : request.records()) {
            batch.add(new KeyValue(r.key(), r.value()));
        }
        long timestamp = clock.millis();                // one timestamp per request
        long baseOffset = log.appendAll(timestamp, batch);
        return Outcome.ok(new ProduceResponse(request.topic(), request.partition(), baseOffset, batch.size()));
    }

    public FetchOutcome handleFetch(FetchRequest request) throws IOException {
        Log log = logs.logFor(request.topic(), request.partition());
        if (log == null) {
            return new FetchOutcome(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, request.topic(), request.partition(), null);
        }
        int maxBytes = Math.min(request.maxBytes(), config.maxFetchBytes());
        try {
            LogSlice slice = log.openSlice(request.fetchOffset(), maxBytes);
            return new FetchOutcome(ErrorCode.NONE, request.topic(), request.partition(), slice);
        } catch (OffsetOutOfRangeException e) {
            return new FetchOutcome(ErrorCode.OFFSET_OUT_OF_RANGE, request.topic(), request.partition(), null);
        }
    }

    public Outcome<CreateTopicResponse> handleCreateTopic(CreateTopicRequest request) throws IOException {
        if (!TopicNames.isValid(request.topic())
                || request.partitionCount() < 1 || request.partitionCount() > config.maxPartitionsPerTopic()) {
            return Outcome.error(ErrorCode.INVALID_REQUEST);
        }
        try {
            Topic topic = logs.createTopic(request.topic(), request.partitionCount());
            return Outcome.ok(new CreateTopicResponse(topic.name(), topic.partitionCount()));
        } catch (TopicAlreadyExistsException e) {
            return Outcome.error(ErrorCode.TOPIC_ALREADY_EXISTS);
        }
    }

    /** METADATA version 0 (no leader information). */
    public Outcome<MetadataResponse> handleMetadata(MetadataRequest request) {
        return handleMetadata(request, (short) 0);
    }

    /** Version 1 adds the leader of each partition: always this broker in Phase 3. */
    public Outcome<MetadataResponse> handleMetadata(MetadataRequest request, short apiVersion) {
        List<Topic> wanted = new ArrayList<>();
        if (request.topics().isEmpty()) {
            wanted.addAll(logs.topics());
        } else {
            for (String name : new TreeSet<>(request.topics())) {
                Topic topic = logs.topic(name);
                if (topic != null) {                    // unknown topics are simply omitted
                    wanted.add(topic);
                }
            }
        }
        int leader = apiVersion >= 1 ? config.nodeId() : MetadataResponse.PartitionMetadata.NO_LEADER;
        List<MetadataResponse.TopicMetadata> topics = new ArrayList<>();
        for (Topic topic : wanted) {
            List<MetadataResponse.PartitionMetadata> partitions = new ArrayList<>();
            for (int p = 0; p < topic.partitionCount(); p++) {
                Log log = topic.partitions().get(p);
                partitions.add(new MetadataResponse.PartitionMetadata(p, leader, log.startOffset(), log.nextOffset()));
            }
            topics.add(new MetadataResponse.TopicMetadata(topic.name(), partitions));
        }
        return Outcome.ok(new MetadataResponse(
                new MetadataResponse.BrokerInfo(config.nodeId(), config.host(), advertisedPort.getAsInt()), topics));
    }
}
