package com.ember.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

/** Small helpers shared by the codecs. Every read checks the bytes remaining FIRST. */
final class WireFormat {

    static final int MAX_STRING_BYTES = 32767;

    private WireFormat() {
    }

    static boolean isValidTopic(String topic) {
        return TopicNames.isValid(topic);
    }

    static void requireValidTopic(String topic) {
        if (!isValidTopic(topic)) {
            throw new ProtocolException("invalid topic name (1..128 chars of [a-zA-Z0-9._-])");
        }
    }

    static void need(ByteBuffer buf, long bytes, String what) {
        if (bytes < 0 || buf.remaining() < bytes) {
            throw new ProtocolException("truncated " + what + ": need " + bytes + " bytes, have " + buf.remaining());
        }
    }

    static int getInt(ByteBuffer buf, String what) {
        need(buf, 4, what);
        return buf.getInt();
    }

    static long getLong(ByteBuffer buf, String what) {
        need(buf, 8, what);
        return buf.getLong();
    }

    static byte getByte(ByteBuffer buf, String what) {
        need(buf, 1, what);
        return buf.get();
    }

    static boolean getBoolean(ByteBuffer buf, String what) {
        byte b = getByte(buf, what);
        if (b != 0 && b != 1) {
            throw new ProtocolException(what + " must be 0 or 1, got " + b);
        }
        return b == 1;
    }

    static short getShort(ByteBuffer buf, String what) {
        need(buf, 2, what);
        return buf.getShort();
    }

    /** A count of items that each take at least minItemBytes on the wire. */
    static int getCount(ByteBuffer buf, int minItemBytes, String what) {
        int count = getInt(buf, what + " count");
        if (count < 0 || (long) count * minItemBytes > buf.remaining()) {
            throw new ProtocolException("bad " + what + " count " + count + " for " + buf.remaining() + " remaining bytes");
        }
        return count;
    }

    static byte[] getBytes(ByteBuffer buf, int length, String what) {
        need(buf, length, what);
        byte[] out = new byte[length];
        buf.get(out);
        return out;
    }

    static String getString(ByteBuffer buf, String what) {
        short len = getShort(buf, what + " length");
        if (len < 0) {
            throw new ProtocolException("negative " + what + " length " + len);
        }
        byte[] bytes = getBytes(buf, len, what);
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException e) {
            throw new ProtocolException(what + " is not valid UTF-8");
        }
    }

    static void putString(ByteBuffer buf, String s) {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        buf.putShort((short) bytes.length);
        buf.put(bytes);
    }

    static int stringSize(String s) {
        int n = s.getBytes(StandardCharsets.UTF_8).length;
        if (n > MAX_STRING_BYTES) {
            throw new ProtocolException("string longer than " + MAX_STRING_BYTES + " bytes");
        }
        return 2 + n;
    }

    static void requireNoTrailingBytes(ByteBuffer buf) {
        if (buf.hasRemaining()) {
            throw new ProtocolException(buf.remaining() + " trailing bytes after the body");
        }
    }
}
