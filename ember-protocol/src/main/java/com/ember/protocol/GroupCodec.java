package com.ember.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static com.ember.protocol.WireFormat.*;

/**
 * Bodies of the consumer-group APIs (apiKeys 11..17). Same rules as every other codec: check bytes remaining BEFORE
 * reading, bound every count (a hostile "10 billion elements" must not allocate anything), reject bad ids.
 */
final class GroupCodec {

    static final int MAX_ID_BYTES = 255;
    static final int MAX_LIST = 10_000;

    private GroupCodec() {
    }

    // ------------------------------------------------------------------ validation

    private static String requireId(String s, String what, boolean allowEmpty) {
        int n = s.getBytes(StandardCharsets.UTF_8).length;
        if ((n == 0 && !allowEmpty) || n > MAX_ID_BYTES) {
            throw new ProtocolException(what + " must be " + (allowEmpty ? "at most " : "1 to ") + MAX_ID_BYTES + " UTF-8 bytes");
        }
        return s;
    }

    private static String getId(ByteBuffer buf, String what, boolean allowEmpty) {
        return requireId(getString(buf, what), what, allowEmpty);
    }

    private static int getListCount(ByteBuffer buf, int minItemBytes, String what) {
        int count = getCount(buf, minItemBytes, what);
        if (count > MAX_LIST) {
            throw new ProtocolException(what + " count " + count + " is above the limit of " + MAX_LIST);
        }
        return count;
    }

    private static void requireListSize(int size, String what) {
        if (size > MAX_LIST) {
            throw new ProtocolException(what + " count " + size + " is above the limit of " + MAX_LIST);
        }
    }

    // ------------------------------------------------------------------ requests

    static boolean handles(Request r) {
        return switch (r) {
            case FindCoordinatorRequest x -> true;
            case JoinGroupRequest x -> true;
            case HeartbeatRequest x -> true;
            case LeaveGroupRequest x -> true;
            case OffsetCommitRequest x -> true;
            case OffsetFetchRequest x -> true;
            case DescribeGroupRequest x -> true;
            default -> false;
        };
    }

    static int requestSize(Request r) {
        return switch (r) {
            case FindCoordinatorRequest x -> stringSize(x.groupId());
            case JoinGroupRequest x -> stringSize(x.groupId()) + stringSize(x.memberId()) + stringSize(x.clientId()) + 4 + 4 + stringsSize(x.topics());
            case HeartbeatRequest x -> stringSize(x.groupId()) + stringSize(x.memberId()) + 4;
            case LeaveGroupRequest x -> stringSize(x.groupId()) + stringSize(x.memberId());
            case OffsetCommitRequest x -> stringSize(x.groupId()) + stringSize(x.memberId()) + 4 + 4 + offsetsSize(x.offsets());
            case OffsetFetchRequest x -> stringSize(x.groupId()) + 4 + refsSize(x.partitions());
            case DescribeGroupRequest x -> stringSize(x.groupId());
            default -> throw new IllegalArgumentException("not a group request: " + r);
        };
    }

    static void encodeRequest(ByteBuffer buf, Request r) {
        switch (r) {
            case FindCoordinatorRequest x -> putString(buf, requireId(x.groupId(), "groupId", false));
            case JoinGroupRequest x -> {
                putString(buf, requireId(x.groupId(), "groupId", false));
                putString(buf, requireId(x.memberId(), "memberId", true));
                putString(buf, requireId(x.clientId(), "clientId", false));
                buf.putInt(x.sessionTimeoutMs());
                requireListSize(x.topics().size(), "topic");
                buf.putInt(x.topics().size());
                for (String t : x.topics()) {
                    requireValidTopic(t);
                    putString(buf, t);
                }
            }
            case HeartbeatRequest x -> {
                putString(buf, requireId(x.groupId(), "groupId", false));
                putString(buf, requireId(x.memberId(), "memberId", false));
                buf.putInt(x.generation());
            }
            case LeaveGroupRequest x -> {
                putString(buf, requireId(x.groupId(), "groupId", false));
                putString(buf, requireId(x.memberId(), "memberId", false));
            }
            case OffsetCommitRequest x -> {
                putString(buf, requireId(x.groupId(), "groupId", false));
                putString(buf, requireId(x.memberId(), "memberId", false));
                buf.putInt(x.generation());
                requireListSize(x.offsets().size(), "offset");
                buf.putInt(x.offsets().size());
                for (PartitionOffset o : x.offsets()) {
                    requireValidTopic(o.topic());
                    putString(buf, o.topic());
                    buf.putInt(o.partition()).putLong(o.offset());
                }
            }
            case OffsetFetchRequest x -> {
                putString(buf, requireId(x.groupId(), "groupId", false));
                putRefs(buf, x.partitions());
            }
            case DescribeGroupRequest x -> putString(buf, requireId(x.groupId(), "groupId", false));
            default -> throw new IllegalArgumentException("not a group request: " + r);
        }
    }

    static Request decodeRequest(ApiKey key, ByteBuffer buf) {
        return switch (key) {
            case FIND_COORDINATOR -> new FindCoordinatorRequest(getId(buf, "groupId", false));
            case JOIN_GROUP -> {
                String group = getId(buf, "groupId", false);
                String member = getId(buf, "memberId", true);
                String client = getId(buf, "clientId", false);
                int session = getInt(buf, "sessionTimeoutMs");
                int count = getListCount(buf, 2, "topic");
                List<String> topics = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    String t = getString(buf, "topic");
                    requireValidTopic(t);
                    topics.add(t);
                }
                yield new JoinGroupRequest(group, member, client, session, topics);
            }
            case HEARTBEAT -> new HeartbeatRequest(getId(buf, "groupId", false), getId(buf, "memberId", false), getInt(buf, "generation"));
            case LEAVE_GROUP -> new LeaveGroupRequest(getId(buf, "groupId", false), getId(buf, "memberId", false));
            case OFFSET_COMMIT -> {
                String group = getId(buf, "groupId", false);
                String member = getId(buf, "memberId", false);
                int generation = getInt(buf, "generation");
                int count = getListCount(buf, 14, "offset");
                List<PartitionOffset> offsets = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    offsets.add(new PartitionOffset(getTopic(buf), getInt(buf, "partition"), getLong(buf, "offset")));
                }
                yield new OffsetCommitRequest(group, member, generation, offsets);
            }
            case OFFSET_FETCH -> new OffsetFetchRequest(getId(buf, "groupId", false), getRefs(buf));
            case DESCRIBE_GROUP -> new DescribeGroupRequest(getId(buf, "groupId", false));
            default -> throw new IllegalArgumentException("not a group api: " + key);
        };
    }

    // ------------------------------------------------------------------ responses (the body after the error code)

    static boolean handles(Response r) {
        return switch (r) {
            case FindCoordinatorResponse x -> true;
            case JoinGroupResponse x -> true;
            case HeartbeatResponse x -> true;
            case LeaveGroupResponse x -> true;
            case OffsetCommitResponse x -> true;
            case OffsetFetchResponse x -> true;
            case DescribeGroupResponse x -> true;
            default -> false;
        };
    }

    static int responseSize(Response r) {
        return switch (r) {
            case FindCoordinatorResponse x -> 4 + stringSize(x.host()) + 4;
            case JoinGroupResponse x -> 4 + stringSize(x.memberId()) + 4 + refsSize(x.assignment());
            case HeartbeatResponse x -> 0;
            case LeaveGroupResponse x -> 0;
            case OffsetCommitResponse x -> 0;
            case OffsetFetchResponse x -> 4 + offsetsSize(x.offsets());
            case DescribeGroupResponse x -> describeSize(x);
            default -> throw new IllegalArgumentException("not a group response: " + r);
        };
    }

    static void encodeResponse(ByteBuffer buf, Response r) {
        switch (r) {
            case FindCoordinatorResponse x -> {
                buf.putInt(x.nodeId());
                putString(buf, x.host());
                buf.putInt(x.port());
            }
            case JoinGroupResponse x -> {
                buf.putInt(x.generation());
                putString(buf, x.memberId());
                putRefsNoCountCheck(buf, x.assignment());
            }
            case HeartbeatResponse x -> {
            }
            case LeaveGroupResponse x -> {
            }
            case OffsetCommitResponse x -> {
            }
            case OffsetFetchResponse x -> {
                buf.putInt(x.offsets().size());
                for (PartitionOffset o : x.offsets()) {
                    putString(buf, o.topic());
                    buf.putInt(o.partition()).putLong(o.offset());
                }
            }
            case DescribeGroupResponse x -> {
                buf.putInt(x.generation());
                buf.put((byte) x.state());
                buf.putInt(x.members().size());
                for (DescribeGroupResponse.Member m : x.members()) {
                    putString(buf, m.memberId());
                    putString(buf, m.clientId());
                    buf.putInt(m.sessionTimeoutMs());
                    buf.putInt(m.topics().size());
                    for (String t : m.topics()) {
                        putString(buf, t);
                    }
                    putRefsNoCountCheck(buf, m.assignment());
                }
                buf.putInt(x.offsets().size());
                for (PartitionOffset o : x.offsets()) {
                    putString(buf, o.topic());
                    buf.putInt(o.partition()).putLong(o.offset());
                }
            }
            default -> throw new IllegalArgumentException("not a group response: " + r);
        }
    }

    static Response decodeResponse(ApiKey key, ByteBuffer buf) {
        return switch (key) {
            case FIND_COORDINATOR -> new FindCoordinatorResponse(getInt(buf, "nodeId"), getString(buf, "host"), getInt(buf, "port"));
            case JOIN_GROUP -> new JoinGroupResponse(getInt(buf, "generation"), getId(buf, "memberId", false), getRefs(buf));
            case HEARTBEAT -> new HeartbeatResponse();
            case LEAVE_GROUP -> new LeaveGroupResponse();
            case OFFSET_COMMIT -> new OffsetCommitResponse();
            case OFFSET_FETCH -> new OffsetFetchResponse(getOffsets(buf));
            case DESCRIBE_GROUP -> decodeDescribe(buf);
            default -> throw new IllegalArgumentException("not a group api: " + key);
        };
    }

    private static DescribeGroupResponse decodeDescribe(ByteBuffer buf) {
        int generation = getInt(buf, "generation");
        int state = getByte(buf, "state");
        if (state < 0 || state > 2) {
            throw new ProtocolException("bad group state " + state);
        }
        int memberCount = getListCount(buf, 16, "member");
        List<DescribeGroupResponse.Member> members = new ArrayList<>(memberCount);
        for (int i = 0; i < memberCount; i++) {
            String id = getId(buf, "memberId", false);
            String client = getId(buf, "clientId", false);
            int session = getInt(buf, "sessionTimeoutMs");
            int topicCount = getListCount(buf, 2, "topic");
            List<String> topics = new ArrayList<>(topicCount);
            for (int t = 0; t < topicCount; t++) {
                topics.add(getString(buf, "topic"));
            }
            members.add(new DescribeGroupResponse.Member(id, client, session, topics, getRefs(buf)));
        }
        return new DescribeGroupResponse(generation, state, members, getOffsets(buf));
    }

    // ------------------------------------------------------------------ shared pieces

    private static String getTopic(ByteBuffer buf) {
        String t = getString(buf, "topic");
        requireValidTopic(t);
        return t;
    }

    private static List<PartitionRef> getRefs(ByteBuffer buf) {
        int count = getListCount(buf, 6, "partition");
        List<PartitionRef> out = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            out.add(new PartitionRef(getTopic(buf), getInt(buf, "partition")));
        }
        return out;
    }

    private static List<PartitionOffset> getOffsets(ByteBuffer buf) {
        int count = getListCount(buf, 14, "offset");
        List<PartitionOffset> out = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            out.add(new PartitionOffset(getTopic(buf), getInt(buf, "partition"), getLong(buf, "offset")));
        }
        return out;
    }

    private static void putRefs(ByteBuffer buf, List<PartitionRef> refs) {
        requireListSize(refs.size(), "partition");
        for (PartitionRef r : refs) {
            requireValidTopic(r.topic());
        }
        putRefsNoCountCheck(buf, refs);
    }

    private static void putRefsNoCountCheck(ByteBuffer buf, List<PartitionRef> refs) {
        buf.putInt(refs.size());
        for (PartitionRef r : refs) {
            putString(buf, r.topic());
            buf.putInt(r.partition());
        }
    }

    private static int stringsSize(List<String> strings) {
        int size = 0;
        for (String s : strings) {
            size += stringSize(s);
        }
        return size;
    }

    private static int refsSize(List<PartitionRef> refs) {
        int size = 4;
        for (PartitionRef r : refs) {
            size += stringSize(r.topic()) + 4;
        }
        return size;
    }

    private static int offsetsSize(List<PartitionOffset> offsets) {
        int size = 0;
        for (PartitionOffset o : offsets) {
            size += stringSize(o.topic()) + 4 + 8;
        }
        return size;
    }

    private static int describeSize(DescribeGroupResponse d) {
        int size = 4 + 1 + 4 + 4 + offsetsSize(d.offsets());
        for (DescribeGroupResponse.Member m : d.members()) {
            size += stringSize(m.memberId()) + stringSize(m.clientId()) + 4 + 4 + stringsSize(m.topics()) + refsSize(m.assignment());
        }
        return size;
    }
}
