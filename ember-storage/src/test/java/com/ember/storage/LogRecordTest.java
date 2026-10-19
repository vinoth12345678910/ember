package com.ember.storage;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class LogRecordTest {

    private LogRecord sample() {
        return new LogRecord(5310, 1_700_000_000_000L, "user-42".getBytes(), "order-paid".getBytes());
    }

    @Test
    void roundTrip() {
        LogRecord back = LogRecord.decode(sample().encode());
        assertEquals(5310, back.offset());
        assertEquals(1_700_000_000_000L, back.timestamp());
        assertArrayEquals("user-42".getBytes(), back.key());
        assertArrayEquals("order-paid".getBytes(), back.value());
    }

    @Test
    void nullKeyStaysNull() {
        LogRecord r = new LogRecord(1, 1, null, "x".getBytes());
        assertNull(LogRecord.decode(r.encode()).key());
    }

    @Test
    void flippedByteIsDetected() {
        ByteBuffer buf = sample().encode();
        int last = buf.capacity() - 1;
        buf.put(last, (byte) (buf.get(last) ^ 0xFF));
        assertThrows(CorruptRecordException.class, () -> LogRecord.decode(buf));
    }

    @Test
    void truncatedRecordIsDetected() {
        ByteBuffer full = sample().encode();
        ByteBuffer cut = ByteBuffer.wrap(Arrays.copyOf(full.array(), full.capacity() - 3));
        assertThrows(CorruptRecordException.class, () -> LogRecord.decode(cut));
    }
}