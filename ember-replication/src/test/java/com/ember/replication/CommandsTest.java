package com.ember.replication;

import com.ember.protocol.ProducerRecord;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CommandsTest {

    @Test
    void produceBatchRoundTripsWithNullKeysAndEmptyValues() {
        List<ProducerRecord> records = List.of(new ProducerRecord("k".getBytes(), "v".getBytes()), new ProducerRecord(null, new byte[0]),
                new ProducerRecord(new byte[0], new byte[]{0, (byte) 0xFF}));
        byte[] bytes = Commands.produceBatch(1_700_000_000_000L, records);
        assertEquals(Commands.PRODUCE_BATCH, Commands.type(bytes));
        Commands.ProduceBatch back = Commands.decodeProduceBatch(bytes);
        assertEquals(1_700_000_000_000L, back.timestamp());
        assertEquals(3, back.records().size());
        assertArrayEquals("k".getBytes(), back.records().get(0).key());
        assertNull(back.records().get(1).key());
        assertEquals(0, back.records().get(1).value().length);
        assertEquals(0, back.records().get(2).key().length);
        assertArrayEquals(new byte[]{0, (byte) 0xFF}, back.records().get(2).value());
    }

    @Test
    void createTopicRoundTrips() {
        byte[] bytes = Commands.createTopic("my-topic.v2", 12);
        assertEquals(Commands.CREATE_TOPIC, Commands.type(bytes));
        assertEquals(new Commands.CreateTopic("my-topic.v2", 12), Commands.decodeCreateTopic(bytes));
    }

    @Test
    void produceBatchMalformedCases() {
        byte[] ok = Commands.produceBatch(1, List.of(new ProducerRecord("k".getBytes(), "v".getBytes())));
        assertThrows(IllegalArgumentException.class, () -> Commands.decodeProduceBatch(new byte[0]));
        assertThrows(IllegalArgumentException.class, () -> Commands.decodeProduceBatch(java.util.Arrays.copyOf(ok, ok.length - 1)));
        assertThrows(IllegalArgumentException.class, () -> Commands.decodeProduceBatch(java.util.Arrays.copyOf(ok, ok.length + 1)));
        byte[] hugeCount = ByteBuffer.allocate(13).put(Commands.PRODUCE_BATCH).putLong(1).putInt(Integer.MAX_VALUE).array();
        assertThrows(IllegalArgumentException.class, () -> Commands.decodeProduceBatch(hugeCount));
        byte[] negativeCount = ByteBuffer.allocate(13).put(Commands.PRODUCE_BATCH).putLong(1).putInt(-1).array();
        assertThrows(IllegalArgumentException.class, () -> Commands.decodeProduceBatch(negativeCount));
        byte[] keyPastEnd = ByteBuffer.allocate(13 + 8).put(Commands.PRODUCE_BATCH).putLong(1).putInt(1).putInt(500).putInt(0).array();
        assertThrows(IllegalArgumentException.class, () -> Commands.decodeProduceBatch(keyPastEnd));
        byte[] wrongType = ok.clone();
        wrongType[0] = 7;
        assertThrows(IllegalArgumentException.class, () -> Commands.decodeProduceBatch(wrongType));
    }

    @Test
    void createTopicMalformedCases() {
        byte[] ok = Commands.createTopic("abc", 3);
        assertThrows(IllegalArgumentException.class, () -> Commands.decodeCreateTopic(java.util.Arrays.copyOf(ok, ok.length - 1)));
        assertThrows(IllegalArgumentException.class, () -> Commands.decodeCreateTopic(java.util.Arrays.copyOf(ok, ok.length + 1)));
        assertThrows(IllegalArgumentException.class, () -> Commands.decodeCreateTopic(new byte[]{2, 0x7f, 0x7f, 1, 2, 3, 4}));
        assertThrows(IllegalArgumentException.class, () -> Commands.decodeCreateTopic(new byte[]{2, 0, 2, (byte) 0xC3, 0x28, 0, 0, 0, 1}));
        assertThrows(IllegalArgumentException.class, () -> Commands.type(new byte[0]));
    }
}
