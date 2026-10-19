package com.ember.broker;

import com.ember.protocol.*;
import com.ember.storage.LogRecord;
import com.ember.storage.LogSlice;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class BrokerApisTest {

    private MutableClock clock;
    private LogManager logs;
    private LocalBrokerApis apis;

    @BeforeEach
    void setUp(@TempDir Path dir) throws IOException {
        start(dir, "--max-fetch-bytes=1000");
    }

    private void start(Path dir, String... extra) throws IOException {
        // Phase 2 tests assume one partition per topic, so auto-created topics get exactly one
        String[] flags = new String[extra.length + 4];
        flags[0] = "--data-dir=" + dir;
        flags[1] = "--host=example.org";
        flags[2] = "--node-id=7";
        flags[3] = "--default-partitions=1";
        System.arraycopy(extra, 0, flags, 4, extra.length);
        BrokerConfig config = BrokerConfig.parse(flags);
        clock = new MutableClock(5_000);
        logs = new LogManager(config);
        apis = new LocalBrokerApis(config, logs, clock, () -> 4321);
    }

    @AfterEach
    void tearDown() {
        logs.close();
    }

    private List<String> names() {
        return logs.topics().stream().map(Topic::name).toList();
    }

    private static ProducerRecord rec(String key, String value) {
        return new ProducerRecord(key == null ? null : key.getBytes(), value.getBytes());
    }

    private LocalBrokerApis.Outcome<ProduceResponse> produce(String topic, int partition, ProducerRecord... records) throws IOException {
        return apis.handleProduce(new ProduceRequest(topic, partition, List.of(records)));
    }

    @Test
    void produceAutoCreatesTheTopicAndAssignsOffsets() throws IOException {
        var first = produce("orders", 0, rec("k", "a"), rec(null, "b"), rec("k", "c"));
        assertEquals(ErrorCode.NONE, first.error());
        assertEquals(new ProduceResponse("orders", 0, 0, 3), first.body());
        var second = produce("orders", 0, rec(null, "d"));
        assertEquals(3, second.body().baseOffset());
        assertEquals(List.of("orders"), names());
    }

    @Test
    void invalidTopicNamesAreInvalidRequests() throws IOException {
        assertEquals(ErrorCode.INVALID_REQUEST, produce("bad/name", 0, rec(null, "x")).error());
        assertEquals(ErrorCode.INVALID_REQUEST, produce("", 0, rec(null, "x")).error());
        assertEquals(ErrorCode.INVALID_REQUEST, produce("..", 0, rec(null, "x")).error());
        assertEquals(List.of(), names());
    }

    @Test
    void nonZeroPartitionIsUnknown() throws IOException {
        assertEquals(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, produce("t", 1, rec(null, "x")).error());
        assertEquals(List.of(), names());
    }

    @Test
    void emptyBatchIsInvalid() throws IOException {
        assertEquals(ErrorCode.INVALID_REQUEST, produce("t", 0).error());
    }

    @Test
    void autoCreateCanBeSwitchedOff(@TempDir Path dir) throws IOException {
        logs.close();
        start(dir, "--auto-create-topics=false");
        assertEquals(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, produce("t", 0, rec(null, "x")).error());
    }

    @Test
    void timestampComesFromTheClockOncePerRequest() throws IOException {
        clock.set(111_000);
        produce("t", 0, rec(null, "a"), rec(null, "b"));
        clock.set(222_000);
        produce("t", 0, rec(null, "c"));
        assertEquals(111_000, logs.logFor("t", 0).read(0).timestamp());
        assertEquals(111_000, logs.logFor("t", 0).read(1).timestamp());
        assertEquals(222_000, logs.logFor("t", 0).read(2).timestamp());
    }

    @Test
    void metadataForAllSpecificAndUnknownTopics() throws IOException {
        produce("b", 0, rec(null, "1"), rec(null, "2"));
        produce("a", 0, rec(null, "1"));

        MetadataResponse all = apis.handleMetadata(new MetadataRequest(List.of())).body();
        assertEquals(new MetadataResponse.BrokerInfo(7, "example.org", 4321), all.broker());
        assertEquals(List.of("a", "b"), all.topics().stream().map(MetadataResponse.TopicMetadata::topic).toList());
        assertEquals(new MetadataResponse.PartitionMetadata(0, 0, 2), all.topics().get(1).partitions().get(0));

        MetadataResponse some = apis.handleMetadata(new MetadataRequest(List.of("b", "nope", "b"))).body();
        assertEquals(List.of("b"), some.topics().stream().map(MetadataResponse.TopicMetadata::topic).toList());

        assertTrue(apis.handleMetadata(new MetadataRequest(List.of("nope"))).body().topics().isEmpty());
    }

    @Test
    void fetchReturnsASliceOfWholeRecords() throws IOException {
        produce("t", 0, rec("k", "v1"), rec("k", "v2"), rec("k", "v3"));
        LocalBrokerApis.FetchOutcome out = apis.handleFetch(new FetchRequest("t", 0, 1, 10_000));
        try (LogSlice slice = out.slice()) {
            assertEquals(ErrorCode.NONE, out.error());
            assertEquals(0, out.logStartOffset());
            assertEquals(3, out.highWatermark());
            assertEquals(2, slice.recordCount);
            ByteBuffer bytes = ByteBuffer.allocate(slice.length);
            slice.channel.read(bytes, slice.position);
            bytes.flip();
            assertEquals(List.of(1L, 2L), RecordDecoder.decodeAll(bytes).stream().map(LogRecord::offset).toList());
        }
    }

    @Test
    void fetchAtTheEndIsEmptyButSuccessful() throws IOException {
        produce("t", 0, rec(null, "v"));
        LocalBrokerApis.FetchOutcome out = apis.handleFetch(new FetchRequest("t", 0, 1, 100));
        assertEquals(ErrorCode.NONE, out.error());
        assertEquals(0, out.slice().length);
        assertNull(out.slice().channel);
    }

    @Test
    void fetchErrors() throws IOException {
        produce("t", 0, rec(null, "v"));
        assertEquals(ErrorCode.OFFSET_OUT_OF_RANGE, apis.handleFetch(new FetchRequest("t", 0, 2, 100)).error());
        assertEquals(ErrorCode.OFFSET_OUT_OF_RANGE, apis.handleFetch(new FetchRequest("t", 0, -1, 100)).error());
        assertEquals(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, apis.handleFetch(new FetchRequest("nope", 0, 0, 100)).error());
        assertEquals(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, apis.handleFetch(new FetchRequest("t", 3, 0, 100)).error());
        assertNull(apis.handleFetch(new FetchRequest("t", 0, 99, 100)).slice());
        assertEquals(List.of("t"), names());                  // fetch never creates a topic
        assertNull(logs.topic("nope"));
    }

    @Test
    void fetchIsCappedAtTheBrokersMaxFetchBytes() throws IOException {
        for (int i = 0; i < 100; i++) {
            produce("t", 0, rec(null, "0123456789"));                // 38 bytes each
        }
        try (LogSlice slice = apis.handleFetch(new FetchRequest("t", 0, 0, 1_000_000)).slice()) {
            assertEquals(26, slice.recordCount);                     // 26 * 38 = 988 <= 1000 < 27 * 38
        }
    }
}
