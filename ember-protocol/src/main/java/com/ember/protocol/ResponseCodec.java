package com.ember.protocol;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static com.ember.protocol.WireFormat.*;

/** encode/decode work on the frame BODY, i.e. the bytes after the INT32 frameLength. */
public final class ResponseCodec {

    /** body is null when errorCode != NONE (an error response ends right after the error code). */
    public record Decoded(int correlationId, ErrorCode errorCode, Response body) {
    }

    private ResponseCodec() {
    }

    public static ByteBuffer encodeFrame(int correlationId, ErrorCode error, Response body) {
        return encodeFrame(correlationId, error, body, (short) 0);
    }

    /** apiVersion is the version the REQUEST used: a response is always encoded in its request's version. */
    public static ByteBuffer encodeFrame(int correlationId, ErrorCode error, Response body, short apiVersion) {
        ByteBuffer inner = encode(correlationId, error, body, apiVersion);
        ByteBuffer frame = ByteBuffer.allocate(4 + inner.remaining());
        frame.putInt(inner.remaining()).put(inner).flip();
        return frame;
    }

    public static ByteBuffer encodeError(int correlationId, ErrorCode error) {
        return encode(correlationId, error, null);
    }

    public static ByteBuffer encode(int correlationId, ErrorCode error, Response body) {
        return encode(correlationId, error, body, (short) 0);
    }

    public static ByteBuffer encode(int correlationId, ErrorCode error, Response body, short apiVersion) {
        if ((error == ErrorCode.NONE) != (body != null)) {
            throw new IllegalArgumentException("a body is present exactly when errorCode is NONE");
        }
        int size = 4 + 2 + (body == null ? 0 : switch (body) {
            case ProduceResponse p -> stringSize(p.topic()) + 4 + 8 + 4;
            case FetchResponse f -> stringSize(f.topic()) + 4 + 8 + 8 + 4 + f.records().remaining();
            case MetadataResponse m -> metadataSize(m, apiVersion);
            case CreateTopicResponse c -> stringSize(c.topic()) + 4;
            case ClusterMetadataResponse c -> clusterMetadataSize(c);
            default -> GroupCodec.responseSize(body);
        });
        ByteBuffer buf = ByteBuffer.allocate(size);
        buf.putInt(correlationId);
        buf.putShort(error.code());
        if (body != null) {
            switch (body) {
                case ProduceResponse p -> {
                    putString(buf, p.topic());
                    buf.putInt(p.partition());
                    buf.putLong(p.baseOffset());
                    buf.putInt(p.recordCount());
                }
                case FetchResponse f -> {
                    putString(buf, f.topic());
                    buf.putInt(f.partition());
                    buf.putLong(f.logStartOffset());
                    buf.putLong(f.highWatermark());
                    buf.putInt(f.records().remaining());
                    buf.put(f.records().duplicate());
                }
                case CreateTopicResponse c -> {
                    putString(buf, c.topic());
                    buf.putInt(c.partitionCount());
                }
                case ClusterMetadataResponse c -> {
                    buf.putInt(c.brokers().size());
                    for (ClusterMetadataResponse.Broker b : c.brokers()) {
                        buf.putInt(b.nodeId());
                        putString(buf, b.host());
                        buf.putInt(b.port());
                    }
                    buf.putInt(c.controllerId());
                    buf.putInt(c.topics().size());
                    for (ClusterMetadataResponse.Topic t : c.topics()) {
                        putString(buf, t.name());
                        buf.putInt(t.partitions().size());
                        for (ClusterMetadataResponse.Partition p : t.partitions()) {
                            buf.putInt(p.partition()).putInt(p.leaderId()).putInt(p.replicas().size());
                            for (int replica : p.replicas()) {
                                buf.putInt(replica);
                            }
                            buf.putLong(p.logStartOffset()).putLong(p.logEndOffset());
                        }
                    }
                }
                case MetadataResponse m -> {
                    buf.putInt(m.broker().nodeId());
                    putString(buf, m.broker().host());
                    buf.putInt(m.broker().port());
                    buf.putInt(m.topics().size());
                    for (MetadataResponse.TopicMetadata t : m.topics()) {
                        putString(buf, t.topic());
                        buf.putInt(t.partitions().size());
                        for (MetadataResponse.PartitionMetadata p : t.partitions()) {
                            buf.putInt(p.partition());
                            if (apiVersion >= 1) {
                                buf.putInt(p.leaderNodeId());
                            }
                            buf.putLong(p.logStartOffset());
                            buf.putLong(p.logEndOffset());
                        }
                    }
                }
                default -> GroupCodec.encodeResponse(buf, body);
            }
        }
        buf.flip();
        return buf;
    }

    private static int clusterMetadataSize(ClusterMetadataResponse c) {
        int size = 4 + 4 + 4;
        for (ClusterMetadataResponse.Broker b : c.brokers()) {
            size += 4 + stringSize(b.host()) + 4;
        }
        for (ClusterMetadataResponse.Topic t : c.topics()) {
            size += stringSize(t.name()) + 4;
            for (ClusterMetadataResponse.Partition p : t.partitions()) {
                size += 4 + 4 + 4 + 4 * p.replicas().size() + 8 + 8;
            }
        }
        return size;
    }

    private static int metadataSize(MetadataResponse m, short apiVersion) {
        int size = 4 + stringSize(m.broker().host()) + 4 + 4;
        for (MetadataResponse.TopicMetadata t : m.topics()) {
            size += stringSize(t.topic()) + 4 + t.partitions().size() * partitionBytes(apiVersion);
        }
        return size;
    }

    /** partition(4) [+ leader(4) in v1] + logStart(8) + logEnd(8) */
    private static int partitionBytes(short apiVersion) {
        return apiVersion >= 1 ? 24 : 20;
    }

    /** The response frame does not say which API it answers, so the caller passes it in. */
    public static Decoded decode(ApiKey apiKey, ByteBuffer frameBody) {
        return decode(apiKey, (short) 0, frameBody);
    }

    public static Decoded decode(ApiKey apiKey, short apiVersion, ByteBuffer frameBody) {
        ByteBuffer buf = frameBody.slice();
        int correlationId = getInt(buf, "correlationId");
        ErrorCode error = ErrorCode.fromCode(getShort(buf, "errorCode"));
        if (error != ErrorCode.NONE) {
            requireNoTrailingBytes(buf);
            return new Decoded(correlationId, error, null);
        }
        Response body = switch (apiKey) {
            case PRODUCE -> new ProduceResponse(getString(buf, "topic"), getInt(buf, "partition"),
                    getLong(buf, "baseOffset"), getInt(buf, "recordCount"));
            case FETCH -> decodeFetch(buf);
            case METADATA -> apiVersion >= 2 ? decodeClusterMetadata(buf) : decodeMetadata(buf, apiVersion);
            case RAFT_MESSAGE -> throw new ProtocolException("RAFT_MESSAGE is one-way: there is no response frame");
            case CREATE_TOPIC -> new CreateTopicResponse(getString(buf, "topic"), getInt(buf, "partitionCount"));
            case FIND_COORDINATOR, JOIN_GROUP, HEARTBEAT, LEAVE_GROUP, OFFSET_COMMIT, OFFSET_FETCH, DESCRIBE_GROUP ->
                    GroupCodec.decodeResponse(apiKey, buf);
        };
        requireNoTrailingBytes(buf);
        return new Decoded(correlationId, error, body);
    }

    private static FetchResponse decodeFetch(ByteBuffer buf) {
        String topic = getString(buf, "topic");
        int partition = getInt(buf, "partition");
        long logStart = getLong(buf, "logStartOffset");
        long highWatermark = getLong(buf, "highWatermark");
        int recordsLength = getInt(buf, "recordsLength");
        if (recordsLength < 0 || recordsLength > buf.remaining()) {
            throw new ProtocolException("recordsLength " + recordsLength + " but only " + buf.remaining() + " bytes remain");
        }
        ByteBuffer records = buf.slice(buf.position(), recordsLength);
        buf.position(buf.position() + recordsLength);
        return new FetchResponse(topic, partition, logStart, highWatermark, records);
    }

    private static ClusterMetadataResponse decodeClusterMetadata(ByteBuffer buf) {
        int brokerCount = getCount(buf, 10, "broker");
        List<ClusterMetadataResponse.Broker> brokers = new ArrayList<>(brokerCount);
        for (int i = 0; i < brokerCount; i++) {
            brokers.add(new ClusterMetadataResponse.Broker(getInt(buf, "nodeId"), getString(buf, "host"), getInt(buf, "port")));
        }
        int controller = getInt(buf, "controllerId");
        int topicCount = getCount(buf, 6, "topic");
        List<ClusterMetadataResponse.Topic> topics = new ArrayList<>(topicCount);
        for (int i = 0; i < topicCount; i++) {
            String name = getString(buf, "topic");
            int partitionCount = getCount(buf, 28, "partition");
            List<ClusterMetadataResponse.Partition> partitions = new ArrayList<>(partitionCount);
            for (int p = 0; p < partitionCount; p++) {
                int partition = getInt(buf, "partition");
                int leader = getInt(buf, "leaderId");
                int replicaCount = getCount(buf, 4, "replica");
                List<Integer> replicas = new ArrayList<>(replicaCount);
                for (int r = 0; r < replicaCount; r++) {
                    replicas.add(getInt(buf, "replica"));
                }
                partitions.add(new ClusterMetadataResponse.Partition(partition, leader, replicas,
                        getLong(buf, "logStartOffset"), getLong(buf, "logEndOffset")));
            }
            topics.add(new ClusterMetadataResponse.Topic(name, partitions));
        }
        return new ClusterMetadataResponse(brokers, controller, topics);
    }

    private static MetadataResponse decodeMetadata(ByteBuffer buf, short apiVersion) {
        int nodeId = getInt(buf, "nodeId");
        String host = getString(buf, "host");
        int port = getInt(buf, "port");
        int topicCount = getCount(buf, 6, "topic");
        List<MetadataResponse.TopicMetadata> topics = new ArrayList<>(topicCount);
        for (int i = 0; i < topicCount; i++) {
            String topic = getString(buf, "topic");
            int partitionCount = getCount(buf, partitionBytes(apiVersion), "partition");
            List<MetadataResponse.PartitionMetadata> partitions = new ArrayList<>(partitionCount);
            for (int p = 0; p < partitionCount; p++) {
                int partition = getInt(buf, "partition");
                int leader = apiVersion >= 1 ? getInt(buf, "leaderNodeId") : MetadataResponse.PartitionMetadata.NO_LEADER;
                partitions.add(new MetadataResponse.PartitionMetadata(
                        partition, leader, getLong(buf, "logStartOffset"), getLong(buf, "logEndOffset")));
            }
            topics.add(new MetadataResponse.TopicMetadata(topic, partitions));
        }
        return new MetadataResponse(new MetadataResponse.BrokerInfo(nodeId, host, port), topics);
    }
}
