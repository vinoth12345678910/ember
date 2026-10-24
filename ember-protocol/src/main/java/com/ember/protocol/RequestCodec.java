package com.ember.protocol;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static com.ember.protocol.WireFormat.*;

/** encode/decode work on the frame BODY, i.e. the bytes after the INT32 frameLength. */
public final class RequestCodec {

    /** Header + body decoded together. */
    public record Decoded(RequestHeader header, Request request) {
    }

    private RequestCodec() {
    }

    /** The whole frame including the leading INT32 length (what a client writes to the socket). */
    public static ByteBuffer encodeFrame(RequestHeader header, Request request) {
        ByteBuffer body = encode(header, request);
        ByteBuffer frame = ByteBuffer.allocate(4 + body.remaining());
        frame.putInt(body.remaining()).put(body).flip();
        return frame;
    }

    public static ByteBuffer encode(RequestHeader header, Request request) {
        if (header.apiKey() != request.apiKey()) {
            throw new IllegalArgumentException("header apiKey " + header.apiKey() + " does not match " + request);
        }
        int size = RequestHeader.SIZE + switch (request) {
            case ProduceRequest p -> produceSize(p);
            case FetchRequest f -> stringSize(f.topic()) + 4 + 8 + 4;
            case MetadataRequest m -> metadataSize(m);
            case CreateTopicRequest c -> stringSize(c.topic()) + 4;
            case RaftMessageRequest r -> stringSize(r.groupTopic()) + 4 + 4 + RaftMessageCodec.size(r.message());
            default -> QueueCodec.handles(request) ? QueueCodec.requestSize(request) : GroupCodec.requestSize(request);
        };
        ByteBuffer buf = ByteBuffer.allocate(size);
        buf.putShort(header.apiKey().code());
        buf.putShort(header.apiVersion());
        buf.putInt(header.correlationId());
        switch (request) {
            case ProduceRequest p -> {
                requireValidTopic(p.topic());
                putString(buf, p.topic());
                buf.putInt(p.partition());
                buf.putInt(p.records().size());
                for (ProducerRecord r : p.records()) {
                    buf.putInt(r.key() == null ? -1 : r.key().length);
                    if (r.key() != null) buf.put(r.key());
                    buf.putInt(r.value().length);
                    buf.put(r.value());
                }
            }
            case FetchRequest f -> {
                requireValidTopic(f.topic());
                putString(buf, f.topic());
                buf.putInt(f.partition());
                buf.putLong(f.fetchOffset());
                buf.putInt(f.maxBytes());
            }
            case CreateTopicRequest c -> {
                requireValidTopic(c.topic());
                putString(buf, c.topic());
                buf.putInt(c.partitionCount());
            }
            case RaftMessageRequest r -> {
                requireValidTopic(r.groupTopic());
                putString(buf, r.groupTopic());
                buf.putInt(r.groupPartition()).putInt(r.fromNodeId());
                RaftMessageCodec.encode(buf, r.message());
            }
            case MetadataRequest m -> {
                buf.putInt(m.topics().size());
                for (String t : m.topics()) {
                    requireValidTopic(t);
                    putString(buf, t);
                }
            }
            default -> {
                if (QueueCodec.handles(request)) QueueCodec.encodeRequest(buf, request);
                else GroupCodec.encodeRequest(buf, request);
            }
        }
        buf.flip();
        return buf;
    }

    private static int produceSize(ProduceRequest p) {
        int size = stringSize(p.topic()) + 4 + 4;
        for (ProducerRecord r : p.records()) {
            size += 4 + (r.key() == null ? 0 : r.key().length) + 4 + r.value().length;
        }
        return size;
    }

    private static int metadataSize(MetadataRequest m) {
        int size = 4;
        for (String t : m.topics()) {
            size += stringSize(t);
        }
        return size;
    }

    /**
     * Throws ProtocolException with the right ErrorCode and (when the 8 header bytes were
     * readable) the correlation id, so the broker can answer instead of just hanging up.
     */
    public static Decoded decode(ByteBuffer frameBody) {
        ByteBuffer buf = frameBody.slice();               // never disturb the caller's position
        if (buf.remaining() < RequestHeader.SIZE) {
            throw new ProtocolException("request header needs " + RequestHeader.SIZE + " bytes, got " + buf.remaining());
        }
        short apiKeyCode = buf.getShort();
        short version = buf.getShort();
        int correlationId = buf.getInt();
        try {
            ApiKey apiKey = ApiKey.fromCode(apiKeyCode);
            if (apiKey == null) {
                throw new ProtocolException("unknown apiKey " + apiKeyCode, ErrorCode.UNSUPPORTED_API, correlationId);
            }
            if (!apiKey.supportsVersion(version)) {
                throw new ProtocolException("unsupported apiVersion " + version, ErrorCode.UNSUPPORTED_VERSION, correlationId);
            }
            Request request = switch (apiKey) {
                case PRODUCE -> decodeProduce(buf);
                case FETCH -> decodeFetch(buf);
                case METADATA -> decodeMetadata(buf);
                case CREATE_TOPIC -> decodeCreateTopic(buf);
                case RAFT_MESSAGE -> decodeRaftMessage(buf);
                case FIND_COORDINATOR, JOIN_GROUP, HEARTBEAT, LEAVE_GROUP, OFFSET_COMMIT, OFFSET_FETCH, DESCRIBE_GROUP ->
                        GroupCodec.decodeRequest(apiKey, buf);
                case QUEUE_FIND, QUEUE_CREATE, QUEUE_CLAIM, QUEUE_ACK, QUEUE_RELEASE, QUEUE_EXTEND, QUEUE_DESCRIBE ->
                        QueueCodec.decodeRequest(apiKey, buf);
            };
            requireNoTrailingBytes(buf);
            return new Decoded(new RequestHeader(apiKey, version, correlationId), request);
        } catch (ProtocolException e) {
            throw e.correlationId() != null ? e : e.withCorrelationId(correlationId);
        } catch (BufferUnderflowException e) {
            throw new ProtocolException("truncated request", ErrorCode.INVALID_REQUEST, correlationId);
        }
    }

    private static ProduceRequest decodeProduce(ByteBuffer buf) {
        String topic = getString(buf, "topic");
        requireValidTopic(topic);
        int partition = getInt(buf, "partition");
        int count = getCount(buf, 8, "record");          // a record is at least keyLength + valueLength
        if (count < 1) {
            throw new ProtocolException("recordCount must be >= 1");
        }
        List<ProducerRecord> records = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            int keyLength = getInt(buf, "keyLength");
            if (keyLength < -1) {
                throw new ProtocolException("bad keyLength " + keyLength);
            }
            byte[] key = keyLength == -1 ? null : getBytes(buf, keyLength, "key");
            int valueLength = getInt(buf, "valueLength");
            if (valueLength < 0) {
                throw new ProtocolException("bad valueLength " + valueLength);
            }
            byte[] value = getBytes(buf, valueLength, "value");
            records.add(new ProducerRecord(key, value));
        }
        return new ProduceRequest(topic, partition, records);
    }

    private static FetchRequest decodeFetch(ByteBuffer buf) {
        String topic = getString(buf, "topic");
        requireValidTopic(topic);
        int partition = getInt(buf, "partition");
        long fetchOffset = getLong(buf, "fetchOffset");
        int maxBytes = getInt(buf, "maxBytes");
        if (maxBytes < 1) {
            throw new ProtocolException("maxBytes must be >= 1");
        }
        return new FetchRequest(topic, partition, fetchOffset, maxBytes);
    }

    private static RaftMessageRequest decodeRaftMessage(ByteBuffer buf) {
        String topic = getString(buf, "groupTopic");
        requireValidTopic(topic);
        int partition = getInt(buf, "groupPartition");
        if (partition < 0) {
            throw new ProtocolException("negative groupPartition");
        }
        int from = getInt(buf, "fromNodeId");
        return new RaftMessageRequest(topic, partition, from, RaftMessageCodec.decode(buf));
    }

    private static CreateTopicRequest decodeCreateTopic(ByteBuffer buf) {
        String topic = getString(buf, "topic");
        requireValidTopic(topic);
        int partitionCount = getInt(buf, "partitionCount");
        if (partitionCount < 1) {
            throw new ProtocolException("partitionCount must be >= 1");
        }
        return new CreateTopicRequest(topic, partitionCount);
    }

    private static MetadataRequest decodeMetadata(ByteBuffer buf) {
        int count = getCount(buf, 2, "topic");
        List<String> topics = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String t = getString(buf, "topic");
            requireValidTopic(t);
            topics.add(t);
        }
        return new MetadataRequest(topics);
    }
}
