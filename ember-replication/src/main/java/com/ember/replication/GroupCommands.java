package com.ember.replication;

import com.ember.protocol.PartitionOffset;
import com.ember.protocol.PartitionRef;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Payloads of the Raft COMMAND entries of the "__groups" group. This group has its OWN command space, so
 * type numbers 1 and 2 do not clash with the partition (PRODUCE_BATCH) or META (CREATE_TOPIC) groups.
 *
 * Everything non-deterministic (member ids, the assignment, the new generation) is decided by the coordinator
 * leader BEFORE proposing; the codec and the state machine are pure.
 *
 *   SET_MEMBERSHIP [1][STRING group][INT32 base][INT32 new][INT32 memberCount]
 *                  member: [STRING id][STRING client][INT32 sessionMs][INT32 topicCount][STRING topic]*
 *                          [INT32 assignedCount]([STRING topic][INT32 partition])*
 *   OFFSET_COMMIT  [2][STRING group][STRING member][INT32 generation][INT32 n]([STRING topic][INT32 partition][INT64 offset])*
 *   STRING = INT16 length + UTF-8
 */
public final class GroupCommands {

    public static final byte SET_MEMBERSHIP = 1;
    public static final byte OFFSET_COMMIT = 2;
    /** Same payload as OFFSET_COMMIT, but it may also store partitions that are UNOWNED right now (a cooperative handoff's last words). */
    public static final byte OFFSET_COMMIT_REVOKED = 3;

    private static final int MAX_LIST = 10_000;

    public record Member(String memberId, String clientId, int sessionTimeoutMs, List<String> topics, List<PartitionRef> assigned) {
        public Member {
            topics = List.copyOf(topics);
            assigned = List.copyOf(assigned);
        }
    }

    public record SetMembership(String groupId, int baseGeneration, int newGeneration, List<Member> members) {
        public SetMembership {
            members = List.copyOf(members);
        }
    }

    public record OffsetCommit(String groupId, String memberId, int generation, List<PartitionOffset> offsets) {
        public OffsetCommit {
            offsets = List.copyOf(offsets);
        }
    }

    private GroupCommands() {
    }

    public static byte type(byte[] command) {
        if (command.length == 0) {
            throw new IllegalArgumentException("empty command");
        }
        return command[0];
    }

    // ------------------------------------------------------------------ encode

    public static byte[] encode(SetMembership c) {
        ByteBuffer buf = ByteBuffer.allocate(size(c));
        buf.put(SET_MEMBERSHIP);
        putString(buf, c.groupId());
        buf.putInt(c.baseGeneration()).putInt(c.newGeneration()).putInt(c.members().size());
        for (Member m : c.members()) {
            putString(buf, m.memberId());
            putString(buf, m.clientId());
            buf.putInt(m.sessionTimeoutMs()).putInt(m.topics().size());
            for (String t : m.topics()) {
                putString(buf, t);
            }
            buf.putInt(m.assigned().size());
            for (PartitionRef p : m.assigned()) {
                putString(buf, p.topic());
                buf.putInt(p.partition());
            }
        }
        return buf.array();
    }

    public static byte[] encode(OffsetCommit c) {
        ByteBuffer buf = ByteBuffer.allocate(size(c));
        buf.put(OFFSET_COMMIT);
        putString(buf, c.groupId());
        putString(buf, c.memberId());
        buf.putInt(c.generation()).putInt(c.offsets().size());
        for (PartitionOffset o : c.offsets()) {
            putString(buf, o.topic());
            buf.putInt(o.partition()).putLong(o.offset());
        }
        return buf.array();
    }

    private static int size(SetMembership c) {
        int size = 1 + strSize(c.groupId()) + 4 + 4 + 4;
        for (Member m : c.members()) {
            size += strSize(m.memberId()) + strSize(m.clientId()) + 4 + 4 + 4;
            for (String t : m.topics()) {
                size += strSize(t);
            }
            for (PartitionRef p : m.assigned()) {
                size += strSize(p.topic()) + 4;
            }
        }
        return size;
    }

    private static int size(OffsetCommit c) {
        int size = 1 + strSize(c.groupId()) + strSize(c.memberId()) + 4 + 4;
        for (PartitionOffset o : c.offsets()) {
            size += strSize(o.topic()) + 4 + 8;
        }
        return size;
    }

    // ------------------------------------------------------------------ decode (strict: IllegalArgumentException on anything odd)

    public static SetMembership decodeSetMembership(byte[] command) {
        ByteBuffer buf = ByteBuffer.wrap(command);
        expectType(buf, SET_MEMBERSHIP);
        String group = getString(buf);
        int base = getInt(buf);
        int next = getInt(buf);
        int count = getCount(buf, 18);
        List<Member> members = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String id = getString(buf);
            String client = getString(buf);
            int session = getInt(buf);
            int topicCount = getCount(buf, 2);
            List<String> topics = new ArrayList<>(topicCount);
            for (int t = 0; t < topicCount; t++) {
                topics.add(getString(buf));
            }
            int assignedCount = getCount(buf, 6);
            List<PartitionRef> assigned = new ArrayList<>(assignedCount);
            for (int a = 0; a < assignedCount; a++) {
                assigned.add(new PartitionRef(getString(buf), getInt(buf)));
            }
            members.add(new Member(id, client, session, topics, assigned));
        }
        require(!buf.hasRemaining(), "trailing bytes after SET_MEMBERSHIP");
        return new SetMembership(group, base, next, members);
    }

    /** The same bytes as encode(c) but marked as a last-words commit of revoked partitions. */
    public static byte[] encodeRevoked(OffsetCommit c) {
        byte[] bytes = encode(c);
        bytes[0] = OFFSET_COMMIT_REVOKED;
        return bytes;
    }

    public static OffsetCommit decodeOffsetCommit(byte[] command) {
        ByteBuffer buf = ByteBuffer.wrap(command);
        require(buf.hasRemaining() && (command[0] == OFFSET_COMMIT || command[0] == OFFSET_COMMIT_REVOKED), "wrong command type");
        buf.get();
        String group = getString(buf);
        String member = getString(buf);
        int generation = getInt(buf);
        int count = getCount(buf, 14);
        List<PartitionOffset> offsets = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            offsets.add(new PartitionOffset(getString(buf), getInt(buf), getLong(buf)));
        }
        require(!buf.hasRemaining(), "trailing bytes after OFFSET_COMMIT");
        return new OffsetCommit(group, member, generation, offsets);
    }

    // ------------------------------------------------------------------ helpers

    private static void expectType(ByteBuffer buf, byte type) {
        require(buf.hasRemaining(), "empty command");
        require(buf.get() == type, "wrong command type");
    }

    private static int getCount(ByteBuffer buf, int minItemBytes) {
        int count = getInt(buf);
        require(count >= 0 && count <= MAX_LIST && (long) count * minItemBytes <= buf.remaining(), "bad count " + count);
        return count;
    }

    private static int getInt(ByteBuffer buf) {
        require(buf.remaining() >= 4, "truncated INT32");
        return buf.getInt();
    }

    private static long getLong(ByteBuffer buf) {
        require(buf.remaining() >= 8, "truncated INT64");
        return buf.getLong();
    }

    private static String getString(ByteBuffer buf) {
        require(buf.remaining() >= 2, "truncated string length");
        int length = buf.getShort();
        require(length >= 0 && length <= buf.remaining(), "bad string length " + length);
        byte[] bytes = new byte[length];
        buf.get(bytes);
        try {
            return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException("string is not valid UTF-8");
        }
    }

    private static void putString(ByteBuffer buf, String s) {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        buf.putShort((short) bytes.length).put(bytes);
    }

    private static int strSize(String s) {
        return 2 + s.getBytes(StandardCharsets.UTF_8).length;
    }

    private static void require(boolean ok, String message) {
        if (!ok) {
            throw new IllegalArgumentException(message);
        }
    }
}
