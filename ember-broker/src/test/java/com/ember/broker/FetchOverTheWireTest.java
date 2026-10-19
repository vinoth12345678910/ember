package com.ember.broker;

import com.ember.protocol.*;
import com.ember.storage.LogRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class FetchOverTheWireTest {

    private static int corr = 100;

    private static void produce(RawClient c, String topic, int count, int valueBytes, int firstIndex) throws IOException {
        List<ProducerRecord> batch = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            byte[] value = new byte[valueBytes];
            byte[] text = ("v" + (firstIndex + i) + ":").getBytes();
            System.arraycopy(text, 0, value, 0, Math.min(text.length, value.length));
            batch.add(new ProducerRecord(("k" + (firstIndex + i)).getBytes(), value));
        }
        c.send(corr, new ProduceRequest(topic, 0, batch));
        ResponseCodec.Decoded d = c.read(ApiKey.PRODUCE);
        assertEquals(corr++, d.correlationId());
        assertEquals(ErrorCode.NONE, d.errorCode());
    }

    private static ResponseCodec.Decoded fetch(RawClient c, String topic, long offset, int maxBytes) throws IOException {
        c.send(corr, new FetchRequest(topic, 0, offset, maxBytes));
        ResponseCodec.Decoded d = c.read(ApiKey.FETCH);
        assertEquals(corr++, d.correlationId());
        return d;
    }

    @Test
    void fetchLoopAcrossSegmentsReturnsEveryRecordOnceInOrder(@TempDir Path dir) throws Exception {
        try (BrokerServer server = ServerFixture.start(ServerFixture.config(dir), Clock.systemUTC());
             RawClient c = new RawClient(server.port())) {
            for (int batch = 0; batch < 30; batch++) {
                produce(c, "t", 10, 100, batch * 10);
            }
            assertTrue(server.logManager().logFor("t", 0).segmentCount() > 3, "the test needs several segments");

            long next = 0;
            int responses = 0;
            while (true) {
                ResponseCodec.Decoded d = fetch(c, "t", next, 1_000_000);
                assertEquals(ErrorCode.NONE, d.errorCode());
                FetchResponse f = (FetchResponse) d.body();
                assertEquals(300, f.highWatermark());
                List<LogRecord> records = RecordDecoder.decodeAll(f.records());
                if (records.isEmpty()) {
                    break;
                }
                for (LogRecord r : records) {
                    assertEquals(next, r.offset());
                    assertArrayEquals(("k" + next).getBytes(), r.key());
                    assertTrue(new String(r.value()).startsWith("v" + next + ":"));
                    next++;
                }
                responses++;
            }
            assertEquals(300, next);
            assertTrue(responses > 3, "a response never crosses a segment, so there must be several");
        }
    }

    @Test
    void fetchFromTheMiddleAndAtTheEnd(@TempDir Path dir) throws Exception {
        try (BrokerServer server = ServerFixture.start(ServerFixture.config(dir), Clock.systemUTC());
             RawClient c = new RawClient(server.port())) {
            produce(c, "t", 5, 10, 0);

            FetchResponse middle = (FetchResponse) fetch(c, "t", 2, 100_000).body();
            assertEquals(List.of(2L, 3L, 4L), RecordDecoder.decodeAll(middle.records()).stream().map(LogRecord::offset).toList());
            assertEquals(0, middle.logStartOffset());

            FetchResponse end = (FetchResponse) fetch(c, "t", 5, 100_000).body();
            assertEquals(0, end.records().remaining());
            assertEquals(5, end.highWatermark());
        }
    }

    @Test
    void errorCases(@TempDir Path dir) throws Exception {
        try (BrokerServer server = ServerFixture.start(ServerFixture.config(dir), Clock.systemUTC());
             RawClient c = new RawClient(server.port())) {
            produce(c, "t", 3, 10, 0);
            assertEquals(ErrorCode.OFFSET_OUT_OF_RANGE, fetch(c, "t", 4, 100).errorCode());
            assertEquals(ErrorCode.OFFSET_OUT_OF_RANGE, fetch(c, "t", -1, 100).errorCode());
            assertEquals(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, fetch(c, "nope", 0, 100).errorCode());
            c.send(corr, new FetchRequest("t", 2, 0, 100));
            assertEquals(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, c.read(ApiKey.FETCH).errorCode());
            corr++;
            assertEquals(ErrorCode.NONE, fetch(c, "t", 0, 100).errorCode());   // same connection still fine
        }
    }

    @Test
    void maxBytesIsRespectedButTheFirstRecordIsAlwaysReturned(@TempDir Path dir) throws Exception {
        try (BrokerServer server = ServerFixture.start(ServerFixture.config(dir, "--segment-bytes=1000000"), Clock.systemUTC());
             RawClient c = new RawClient(server.port())) {
            produce(c, "t", 10, 100, 0);                     // each record: 28 + 2 + 100 = 130 bytes
            FetchResponse f = (FetchResponse) fetch(c, "t", 0, 400).body();
            assertEquals(390, f.records().remaining());       // 3 whole records fit, 4 would be 520
            assertEquals(3, RecordDecoder.decodeAll(f.records()).size());

            FetchResponse tiny = (FetchResponse) fetch(c, "t", 3, 1).body();
            assertEquals(130, tiny.records().remaining());    // bigger than maxBytes, still returned
            assertEquals(3, RecordDecoder.decodeAll(tiny.records()).get(0).offset());
        }
    }

    @Test
    void responseBytesAreIdenticalToTheFileBytes(@TempDir Path dir) throws Exception {
        try (BrokerServer server = ServerFixture.start(ServerFixture.config(dir, "--segment-bytes=1000000"), Clock.systemUTC());
             RawClient c = new RawClient(server.port())) {
            produce(c, "t", 20, 50, 0);
            server.logManager().logFor("t", 0).flush();
            byte[] onDisk = Files.readAllBytes(dir.resolve("t-0").resolve(String.format("%020d.log", 0)));

            FetchResponse f = (FetchResponse) fetch(c, "t", 0, 1_000_000).body();
            byte[] sent = new byte[f.records().remaining()];
            f.records().get(sent);
            assertArrayEquals(onDisk, sent);
        }
    }

    @Test
    void aLargeResponseIsStreamedCompletely(@TempDir Path dir) throws Exception {
        try (BrokerServer server = ServerFixture.start(ServerFixture.config(dir, "--segment-bytes=16777216"), Clock.systemUTC());
             RawClient c = new RawClient(server.port())) {
            for (int i = 0; i < 40; i++) {
                produce(c, "big", 1, 50_000, i);              // ~2 MB in one segment
            }
            FetchResponse f = (FetchResponse) fetch(c, "big", 0, 1_000_000).body();
            int len = f.records().remaining();
            assertTrue(len > 900_000 && len <= 1_000_000, "len=" + len);
            List<LogRecord> records = RecordDecoder.decodeAll(f.records());
            for (int i = 0; i < records.size(); i++) {
                assertEquals(i, records.get(i).offset());
                assertEquals(50_000, records.get(i).value().length);
            }
        }
    }

    @Test
    void emptyValuesAndNullKeysSurvive(@TempDir Path dir) throws Exception {
        try (BrokerServer server = ServerFixture.start(ServerFixture.config(dir), Clock.systemUTC());
             RawClient c = new RawClient(server.port())) {
            c.send(corr, new ProduceRequest("t", 0, List.of(new ProducerRecord(null, new byte[0]),
                    new ProducerRecord(new byte[0], "x".getBytes()))));
            c.read(ApiKey.PRODUCE);
            corr++;
            List<LogRecord> r = RecordDecoder.decodeAll(((FetchResponse) fetch(c, "t", 0, 1000).body()).records());
            assertNull(r.get(0).key());
            assertEquals(0, r.get(0).value().length);
            assertNotNull(r.get(1).key());
            assertEquals(0, r.get(1).key().length);
        }
    }
}
