package com.ember.protocol;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static com.ember.protocol.WireFormat.*;

/** Bodies of the ack-queue APIs (apiKeys 20..26). Same rules as GroupCodec: bound every count, check bytes before reading. */
final class QueueCodec {

    static final int MAX_HANDLES = 10_000;
    static final int MAX_CLAIM = 10_000;
    private static final int HANDLE_BYTES = 4 + 8 + 4;
    private static final int MIN_MESSAGE_BYTES = 4 + 8 + 4 + 8 + 4 + 4;

    private QueueCodec() {
    }

    static boolean handles(Request r) {
        return r instanceof QueueFindRequest || r instanceof QueueCreateRequest || r instanceof QueueClaimRequest
                || r instanceof QueueSettleRequest || r instanceof QueueDescribeRequest;
    }

    static boolean handles(Response r) {
        return r instanceof QueueCreateResponse || r instanceof QueueClaimResponse || r instanceof QueueSettleResponse || r instanceof QueueDescribeResponse;
    }

    private static String name(ByteBuffer buf, String what) {
        String s = getString(buf, what);
        if (s.isEmpty() || s.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > GroupCodec.MAX_ID_BYTES) {
            throw new ProtocolException(what + " must be 1 to " + GroupCodec.MAX_ID_BYTES + " UTF-8 bytes");
        }
        return s;
    }

    private static String checkName(String s, String what) {
        if (s == null || s.isEmpty() || s.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > GroupCodec.MAX_ID_BYTES) {
            throw new ProtocolException(what + " must be 1 to " + GroupCodec.MAX_ID_BYTES + " UTF-8 bytes");
        }
        return s;
    }

    // ------------------------------------------------------------------ requests

    static int requestSize(Request r) {
        return switch (r) {
            case QueueFindRequest x -> 0;
            case QueueCreateRequest x -> stringSize(x.subscription()) + stringSize(x.topic()) + 1 + 4;
            case QueueClaimRequest x -> stringSize(x.subscription()) + stringSize(x.worker()) + 4 + 4 + 4;
            case QueueSettleRequest x -> stringSize(x.subscription()) + stringSize(x.worker()) + 4 + 4 + x.handles().size() * HANDLE_BYTES;
            case QueueDescribeRequest x -> stringSize(x.subscription());
            default -> throw new IllegalArgumentException("not a queue request: " + r);
        };
    }

    static void encodeRequest(ByteBuffer buf, Request r) {
        switch (r) {
            case QueueFindRequest x -> {
            }
            case QueueCreateRequest x -> {
                requireValidTopic(x.topic());
                putString(buf, checkName(x.subscription(), "subscription"));
                putString(buf, x.topic());
                buf.put((byte) (x.fromStart() ? 1 : 0));
                buf.putInt(x.leaseMs());
            }
            case QueueClaimRequest x -> {
                putString(buf, checkName(x.subscription(), "subscription"));
                putString(buf, checkName(x.worker(), "worker"));
                buf.putInt(x.maxMessages()).putInt(x.leaseMs()).putInt(x.maxBytes());
            }
            case QueueSettleRequest x -> {
                if (x.handles().size() > MAX_HANDLES) {
                    throw new ProtocolException("handle count " + x.handles().size() + " is above the limit of " + MAX_HANDLES);
                }
                putString(buf, checkName(x.subscription(), "subscription"));
                putString(buf, checkName(x.worker(), "worker"));
                buf.putInt(x.leaseMs());
                buf.putInt(x.handles().size());
                for (QueueHandle h : x.handles()) {
                    buf.putInt(h.partition()).putLong(h.offset()).putInt(h.attempt());
                }
            }
            case QueueDescribeRequest x -> putString(buf, checkName(x.subscription(), "subscription"));
            default -> throw new IllegalArgumentException("not a queue request: " + r);
        }
    }

    static Request decodeRequest(ApiKey key, ByteBuffer buf) {
        return switch (key) {
            case QUEUE_FIND -> new QueueFindRequest();
            case QUEUE_CREATE -> {
                String sub = name(buf, "subscription");
                String topic = getString(buf, "topic");
                requireValidTopic(topic);
                boolean fromStart = getBoolean(buf, "fromStart");
                yield new QueueCreateRequest(sub, topic, fromStart, getInt(buf, "leaseMs"));
            }
            case QUEUE_CLAIM -> {
                String sub = name(buf, "subscription");
                String worker = name(buf, "worker");
                int max = getInt(buf, "maxMessages");
                int lease = getInt(buf, "leaseMs");
                int bytes = getInt(buf, "maxBytes");
                if (max < 1 || max > MAX_CLAIM || lease < 0 || bytes < 1) {
                    throw new ProtocolException("bad claim limits");
                }
                yield new QueueClaimRequest(sub, worker, max, lease, bytes);
            }
            case QUEUE_ACK, QUEUE_RELEASE, QUEUE_EXTEND -> {
                String sub = name(buf, "subscription");
                String worker = name(buf, "worker");
                int lease = getInt(buf, "leaseMs");
                int count = getCount(buf, HANDLE_BYTES, "handle");
                if (count > MAX_HANDLES) {
                    throw new ProtocolException("handle count " + count + " is above the limit of " + MAX_HANDLES);
                }
                List<QueueHandle> handles = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    handles.add(new QueueHandle(getInt(buf, "partition"), getLong(buf, "offset"), getInt(buf, "attempt")));
                }
                yield new QueueSettleRequest(key, sub, worker, handles, lease);
            }
            case QUEUE_DESCRIBE -> new QueueDescribeRequest(name(buf, "subscription"));
            default -> throw new IllegalArgumentException("not a queue api: " + key);
        };
    }

    // ------------------------------------------------------------------ responses (the body after the error code)

    static int responseSize(Response r) {
        return switch (r) {
            case QueueCreateResponse x -> 4;
            case QueueClaimResponse x -> {
                int n = 4;
                for (QueueMessage m : x.messages()) {
                    n += 4 + 8 + 4 + 8 + 4 + (m.key() == null ? 0 : m.key().length) + 4 + m.value().length;
                }
                yield n;
            }
            case QueueSettleResponse x -> 8;
            case QueueDescribeResponse x -> stringSize(x.topic()) + 4 + 4 + x.partitions().size() * (4 + 8 + 8 + 8 + 4 + 4 + 8);
            default -> throw new IllegalArgumentException("not a queue response: " + r);
        };
    }

    static void encodeResponse(ByteBuffer buf, Response r) {
        switch (r) {
            case QueueCreateResponse x -> buf.putInt(x.partitionCount());
            case QueueClaimResponse x -> {
                buf.putInt(x.messages().size());
                for (QueueMessage m : x.messages()) {
                    buf.putInt(m.partition()).putLong(m.offset()).putInt(m.attempt()).putLong(m.timestamp());
                    buf.putInt(m.key() == null ? -1 : m.key().length);
                    if (m.key() != null) buf.put(m.key());
                    buf.putInt(m.value().length);
                    buf.put(m.value());
                }
            }
            case QueueSettleResponse x -> buf.putInt(x.settled()).putInt(x.rejected());
            case QueueDescribeResponse x -> {
                putString(buf, x.topic());
                buf.putInt(x.leaseMs());
                buf.putInt(x.partitions().size());
                for (QueueDescribeResponse.PartitionState p : x.partitions()) {
                    buf.putInt(p.partition()).putLong(p.floor()).putLong(p.next()).putLong(p.logEnd())
                            .putInt(p.inFlight()).putInt(p.redeliver()).putLong(p.doneAbove());
                }
            }
            default -> throw new IllegalArgumentException("not a queue response: " + r);
        }
    }

    static Response decodeResponse(ApiKey key, ByteBuffer buf) {
        return switch (key) {
            case QUEUE_FIND -> new FindCoordinatorResponse(getInt(buf, "nodeId"), getString(buf, "host"), getInt(buf, "port"));
            case QUEUE_CREATE -> new QueueCreateResponse(getInt(buf, "partitionCount"));
            case QUEUE_CLAIM -> {
                int count = getCount(buf, MIN_MESSAGE_BYTES, "message");
                List<QueueMessage> messages = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    int partition = getInt(buf, "partition");
                    long offset = getLong(buf, "offset");
                    int attempt = getInt(buf, "attempt");
                    long timestamp = getLong(buf, "timestamp");
                    int keyLength = getInt(buf, "keyLength");
                    if (keyLength < -1) {
                        throw new ProtocolException("bad keyLength " + keyLength);
                    }
                    byte[] k = keyLength == -1 ? null : getBytes(buf, keyLength, "key");
                    int valueLength = getInt(buf, "valueLength");
                    if (valueLength < 0) {
                        throw new ProtocolException("bad valueLength " + valueLength);
                    }
                    messages.add(new QueueMessage(partition, offset, attempt, timestamp, k, getBytes(buf, valueLength, "value")));
                }
                yield new QueueClaimResponse(messages);
            }
            case QUEUE_ACK, QUEUE_RELEASE, QUEUE_EXTEND -> new QueueSettleResponse(getInt(buf, "settled"), getInt(buf, "rejected"));
            case QUEUE_DESCRIBE -> {
                String topic = getString(buf, "topic");
                int lease = getInt(buf, "leaseMs");
                int count = getCount(buf, 4 + 8 + 8 + 8 + 4 + 4 + 8, "partition");
                List<QueueDescribeResponse.PartitionState> parts = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    parts.add(new QueueDescribeResponse.PartitionState(getInt(buf, "partition"), getLong(buf, "floor"), getLong(buf, "next"),
                            getLong(buf, "logEnd"), getInt(buf, "inFlight"), getInt(buf, "redeliver"), getLong(buf, "doneAbove")));
                }
                yield new QueueDescribeResponse(topic, lease, parts);
            }
            default -> throw new IllegalArgumentException("not a queue api: " + key);
        };
    }
}
