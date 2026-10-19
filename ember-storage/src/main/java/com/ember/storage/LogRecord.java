package com.ember.storage;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

public record LogRecord(long offset, long timestamp, byte[] key, byte[] value) {

    // length(4) + crc(4) + offset(8) + timestamp(8) + keyLength(4)
    public static final int HEADER_SIZE = 4 + 4 + 8 + 8 + 4;

    public int sizeInBytes() {
        int keyLen = key == null ? 0 : key.length;
        return HEADER_SIZE + keyLen + value.length;
    }

    public ByteBuffer encode() {
        ByteBuffer buf = ByteBuffer.allocate(sizeInBytes());

        buf.putInt(sizeInBytes() - 4);        // length = everything after this field
        buf.putInt(0);                        // crc placeholder, filled in below
        buf.putLong(offset);
        buf.putLong(timestamp);
        buf.putInt(key == null ? -1 : key.length);
        if (key != null) buf.put(key);
        buf.put(value);

        CRC32 crc = new CRC32();
        crc.update(buf.array(), 8, buf.capacity() - 8);   // everything after length + crc
        buf.putInt(4, (int) crc.getValue());              // write crc at index 4

        buf.flip();                                       // ready for reading from position 0
        return buf;
    }

    public static LogRecord decode(ByteBuffer buf) {
        if (buf.remaining() < 4) {
            throw new CorruptRecordException("not enough bytes to read length");
        }
        int length = buf.getInt(buf.position());          // peek, don't consume yet
        if (length < HEADER_SIZE - 4 || buf.remaining() < 4 + length) {
            throw new CorruptRecordException("record truncated or bad length: " + length);
        }

        buf.getInt();                                     // consume length
        int storedCrc = buf.getInt();
        byte[] body = new byte[length - 4];
        buf.get(body);

        CRC32 crc = new CRC32();
        crc.update(body);
        if ((int) crc.getValue() != storedCrc) {
            throw new CorruptRecordException("crc mismatch");
        }

        ByteBuffer b = ByteBuffer.wrap(body);
        long offset = b.getLong();
        long timestamp = b.getLong();
        int keyLen = b.getInt();
        byte[] key = null;
        if (keyLen >= 0) {
            key = new byte[keyLen];
            b.get(key);
        }
        byte[] value = new byte[b.remaining()];
        b.get(value);

        return new LogRecord(offset, timestamp, key, value);
    }
}