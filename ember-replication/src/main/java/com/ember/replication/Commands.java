package com.ember.replication;

import com.ember.protocol.ProducerRecord;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * The payloads of Raft COMMAND entries. Every replica decodes and applies them identically, so
 * everything non-deterministic (the timestamp!) is decided by the leader BEFORE the entry is proposed.
 */
public final class Commands {

    public static final byte PRODUCE_BATCH = 1;
    public static final byte CREATE_TOPIC = 2;

    public record ProduceBatch(long timestamp, List<ProducerRecord> records) {
    }

    public record CreateTopic(String name, int partitionCount) {
    }

    private Commands() {
    }

    public static byte type(byte[] command) {
        if (command.length == 0) {
            throw new IllegalArgumentException("empty command");
        }
        return command[0];
    }

    // ---- PRODUCE_BATCH: [1][INT64 timestamp][INT32 count][records: INT32 keyLen(-1 null) key INT32 valueLen value]

    public static byte[] produceBatch(long timestamp, List<ProducerRecord> records) {
        int size = 1 + 8 + 4;
        for (ProducerRecord r : records) {
            size += 4 + (r.key() == null ? 0 : r.key().length) + 4 + r.value().length;
        }
        ByteBuffer buf = ByteBuffer.allocate(size);
        buf.put(PRODUCE_BATCH).putLong(timestamp).putInt(records.size());
        for (ProducerRecord r : records) {
            buf.putInt(r.key() == null ? -1 : r.key().length);
            if (r.key() != null) buf.put(r.key());
            buf.putInt(r.value().length).put(r.value());
        }
        return buf.array();
    }

    public static ProduceBatch decodeProduceBatch(byte[] command) {
        ByteBuffer buf = ByteBuffer.wrap(command);
        require(buf.remaining() >= 13, "PRODUCE_BATCH too short");
        if (buf.get() != PRODUCE_BATCH) {
            throw new IllegalArgumentException("not a PRODUCE_BATCH command");
        }
        long timestamp = buf.getLong();
        int count = buf.getInt();
        require(count >= 0 && (long) count * 8 <= buf.remaining(), "bad record count " + count);
        List<ProducerRecord> records = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            require(buf.remaining() >= 4, "truncated key length");
            int keyLength = buf.getInt();
            require(keyLength >= -1 && keyLength <= buf.remaining(), "bad key length " + keyLength);
            byte[] key = null;
            if (keyLength >= 0) {
                key = new byte[keyLength];
                buf.get(key);
            }
            require(buf.remaining() >= 4, "truncated value length");
            int valueLength = buf.getInt();
            require(valueLength >= 0 && valueLength <= buf.remaining(), "bad value length " + valueLength);
            byte[] value = new byte[valueLength];
            buf.get(value);
            records.add(new ProducerRecord(key, value));
        }
        require(!buf.hasRemaining(), "trailing bytes after the records");
        return new ProduceBatch(timestamp, records);
    }

    // ---- CREATE_TOPIC: [2][INT16 nameLen][name][INT32 partitionCount]

    public static byte[] createTopic(String name, int partitionCount) {
        byte[] n = name.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.allocate(1 + 2 + n.length + 4).put(CREATE_TOPIC).putShort((short) n.length).put(n).putInt(partitionCount).array();
    }

    public static CreateTopic decodeCreateTopic(byte[] command) {
        ByteBuffer buf = ByteBuffer.wrap(command);
        require(buf.remaining() >= 3, "CREATE_TOPIC too short");
        if (buf.get() != CREATE_TOPIC) {
            throw new IllegalArgumentException("not a CREATE_TOPIC command");
        }
        int length = buf.getShort();
        require(length >= 0 && length + 4 <= buf.remaining(), "bad topic name length " + length);
        byte[] n = new byte[length];
        buf.get(n);
        String name;
        try {
            name = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(n)).toString();
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException("topic name is not valid UTF-8");
        }
        int partitions = buf.getInt();
        require(!buf.hasRemaining(), "trailing bytes after CREATE_TOPIC");
        return new CreateTopic(name, partitions);
    }

    private static void require(boolean ok, String message) {
        if (!ok) {
            throw new IllegalArgumentException(message);
        }
    }
}
