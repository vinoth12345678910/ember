package com.ember.replication;

import com.ember.protocol.QueueHandle;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Payloads of the Raft COMMAND entries of the "__acks" group (its own command space, like "__groups").
 *
 * The leader decides everything non-deterministic (which offsets, the deadline, the clock reading) BEFORE proposing; the codec
 * and the state machine are pure.
 *
 *   CREATE   [1][STRING sub][STRING topic][INT32 leaseMs][INT32 n]([INT64 start])*          one start offset per partition
 *   CLAIM    [2][STRING sub][INT32 n]( [INT32 partition][STRING worker][INT64 deadline][INT64 freshFrom][INT32 freshCount]
 *                                      [INT32 m]([INT64 offset])* )*
 *   ACK      [3][STRING sub][STRING worker][INT64 0][INT32 n]([INT32 partition][INT64 offset][INT32 attempt])*
 *   RELEASE  [4]  same layout as ACK
 *   EXTEND   [5]  same layout as ACK, the INT64 is the new deadline
 *   EXPIRE   [6][STRING sub][INT64 now][INT32 n]([INT32 partition][INT64 offset][INT32 attempt])*
 *   DELETE   [7][STRING sub]
 */
public final class AckCommands {

    public static final byte CREATE = 1;
    public static final byte CLAIM = 2;
    public static final byte ACK = 3;
    public static final byte RELEASE = 4;
    public static final byte EXTEND = 5;
    public static final byte EXPIRE = 6;
    public static final byte DELETE = 7;

    static final int MAX_LIST = 100_000;

    public record Create(String subscription, String topic, int leaseMs, List<Long> starts) {
        public Create {
            starts = List.copyOf(starts);
        }
    }

    /** What one worker gets from one partition: fresh offsets freshFrom..freshFrom+freshCount-1, plus these redelivered offsets. */
    public record Allotment(int partition, String worker, long deadline, long freshFrom, int freshCount, List<Long> redeliver) {
        public Allotment {
            redeliver = List.copyOf(redeliver);
        }
    }

    public record Claim(String subscription, List<Allotment> allotments) {
        public Claim {
            allotments = List.copyOf(allotments);
        }
    }

    /** ACK, RELEASE or EXTEND (type says which). deadline is only used by EXTEND. */
    public record Settle(byte type, String subscription, String worker, long deadline, List<QueueHandle> handles) {
        public Settle {
            handles = List.copyOf(handles);
        }
    }

    public record Expire(String subscription, long now, List<QueueHandle> handles) {
        public Expire {
            handles = List.copyOf(handles);
        }
    }

    public record Delete(String subscription) {
    }

    private AckCommands() {
    }

    public static byte type(byte[] command) {
        if (command.length == 0) {
            throw new IllegalArgumentException("empty command");
        }
        return command[0];
    }

    // ------------------------------------------------------------------ encode

    public static byte[] encode(Create c) {
        ByteBuffer buf = ByteBuffer.allocate(1 + strSize(c.subscription()) + strSize(c.topic()) + 4 + 4 + 8 * c.starts().size());
        buf.put(CREATE);
        putString(buf, c.subscription());
        putString(buf, c.topic());
        buf.putInt(c.leaseMs()).putInt(c.starts().size());
        for (long s : c.starts()) {
            buf.putLong(s);
        }
        return buf.array();
    }

    public static byte[] encode(Claim c) {
        int size = 1 + strSize(c.subscription()) + 4;
        for (Allotment a : c.allotments()) {
            size += 4 + strSize(a.worker()) + 8 + 8 + 4 + 4 + 8 * a.redeliver().size();
        }
        ByteBuffer buf = ByteBuffer.allocate(size);
        buf.put(CLAIM);
        putString(buf, c.subscription());
        buf.putInt(c.allotments().size());
        for (Allotment a : c.allotments()) {
            buf.putInt(a.partition());
            putString(buf, a.worker());
            buf.putLong(a.deadline()).putLong(a.freshFrom()).putInt(a.freshCount()).putInt(a.redeliver().size());
            for (long o : a.redeliver()) {
                buf.putLong(o);
            }
        }
        return buf.array();
    }

    public static byte[] encode(Settle s) {
        if (s.type() != ACK && s.type() != RELEASE && s.type() != EXTEND) {
            throw new IllegalArgumentException("not a settle command type: " + s.type());
        }
        ByteBuffer buf = ByteBuffer.allocate(1 + strSize(s.subscription()) + strSize(s.worker()) + 8 + 4 + 16 * s.handles().size());
        buf.put(s.type());
        putString(buf, s.subscription());
        putString(buf, s.worker());
        buf.putLong(s.deadline()).putInt(s.handles().size());
        for (QueueHandle h : s.handles()) {
            buf.putInt(h.partition()).putLong(h.offset()).putInt(h.attempt());
        }
        return buf.array();
    }

    public static byte[] encode(Expire e) {
        ByteBuffer buf = ByteBuffer.allocate(1 + strSize(e.subscription()) + 8 + 4 + 16 * e.handles().size());
        buf.put(EXPIRE);
        putString(buf, e.subscription());
        buf.putLong(e.now()).putInt(e.handles().size());
        for (QueueHandle h : e.handles()) {
            buf.putInt(h.partition()).putLong(h.offset()).putInt(h.attempt());
        }
        return buf.array();
    }

    public static byte[] encode(Delete d) {
        ByteBuffer buf = ByteBuffer.allocate(1 + strSize(d.subscription()));
        buf.put(DELETE);
        putString(buf, d.subscription());
        return buf.array();
    }

    // ------------------------------------------------------------------ decode (strict: IllegalArgumentException on anything odd)

    public static Create decodeCreate(byte[] command) {
        ByteBuffer buf = start(command, CREATE);
        String sub = getString(buf);
        String topic = getString(buf);
        int lease = getInt(buf);
        int n = getCount(buf, 8);
        List<Long> starts = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            starts.add(getLong(buf));
        }
        end(buf);
        return new Create(sub, topic, lease, starts);
    }

    public static Claim decodeClaim(byte[] command) {
        ByteBuffer buf = start(command, CLAIM);
        String sub = getString(buf);
        int n = getCount(buf, 30);
        List<Allotment> allotments = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            int partition = getInt(buf);
            String worker = getString(buf);
            long deadline = getLong(buf);
            long freshFrom = getLong(buf);
            int freshCount = getInt(buf);
            int m = getCount(buf, 8);
            List<Long> redeliver = new ArrayList<>(m);
            for (int j = 0; j < m; j++) {
                redeliver.add(getLong(buf));
            }
            allotments.add(new Allotment(partition, worker, deadline, freshFrom, freshCount, redeliver));
        }
        end(buf);
        return new Claim(sub, allotments);
    }

    public static Settle decodeSettle(byte[] command) {
        byte type = command.length == 0 ? 0 : command[0];
        require(type == ACK || type == RELEASE || type == EXTEND, "wrong command type");
        ByteBuffer buf = start(command, type);
        String sub = getString(buf);
        String worker = getString(buf);
        long deadline = getLong(buf);
        List<QueueHandle> handles = getHandles(buf);
        end(buf);
        return new Settle(type, sub, worker, deadline, handles);
    }

    public static Expire decodeExpire(byte[] command) {
        ByteBuffer buf = start(command, EXPIRE);
        String sub = getString(buf);
        long now = getLong(buf);
        List<QueueHandle> handles = getHandles(buf);
        end(buf);
        return new Expire(sub, now, handles);
    }

    public static Delete decodeDelete(byte[] command) {
        ByteBuffer buf = start(command, DELETE);
        String sub = getString(buf);
        end(buf);
        return new Delete(sub);
    }

    private static List<QueueHandle> getHandles(ByteBuffer buf) {
        int n = getCount(buf, 16);
        List<QueueHandle> handles = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            handles.add(new QueueHandle(getInt(buf), getLong(buf), getInt(buf)));
        }
        return handles;
    }

    // ------------------------------------------------------------------ helpers

    private static ByteBuffer start(byte[] command, byte type) {
        ByteBuffer buf = ByteBuffer.wrap(command);
        require(buf.hasRemaining(), "empty command");
        require(buf.get() == type, "wrong command type");
        return buf;
    }

    private static void end(ByteBuffer buf) {
        require(!buf.hasRemaining(), "trailing bytes");
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
