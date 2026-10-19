package com.ember.broker;

import com.ember.protocol.*;
import com.ember.storage.LogSlice;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** Request-level rules for multi-partition topics, with no sockets. */
class BrokerApisPartitionsTest {

    private Path dir;
    private BrokerConfig config;
    private LogManager logs;
    private LocalBrokerApis apis;

    @BeforeEach
    void setUp(@TempDir Path tempDir) throws IOException {
        dir = tempDir;
        open("--default-partitions=3", "--max-partitions=16");
    }

    private void open(String... extra) throws IOException {
        String[] flags = new String[extra.length + 3];
        flags[0] = "--data-dir=" + dir;
        flags[1] = "--node-id=5";
        flags[2] = "--host=example.org";
        System.arraycopy(extra, 0, flags, 3, extra.length);
        config = BrokerConfig.parse(flags);
        logs = new LogManager(config);
        apis = new LocalBrokerApis(config, logs, new MutableClock(1_000), () -> 4242);
    }

    @AfterEach
    void tearDown() {
        logs.close();
    }

    private LocalBrokerApis.Outcome<ProduceResponse> produce(String topic, int partition, String... values) throws IOException {
        List<ProducerRecord> records = new ArrayList<>();
        for (String v : values) records.add(new ProducerRecord(null, v.getBytes()));
        return apis.handleProduce(new ProduceRequest(topic, partition, records));
    }

    private ErrorCode create(String topic, int partitions) throws IOException {
        return apis.handleCreateTopic(new CreateTopicRequest(topic, partitions)).error();
    }

    // ---- CREATE_TOPIC

    @Test
    void createTopicReturnsTheCount() throws IOException {
        var out = apis.handleCreateTopic(new CreateTopicRequest("orders", 4));
        assertEquals(ErrorCode.NONE, out.error());
        assertEquals(new CreateTopicResponse("orders", 4), out.body());
        assertEquals(4, logs.topic("orders").partitionCount());
    }

    @Test
    void duplicateCreateGivesTopicAlreadyExists() throws IOException {
        assertEquals(ErrorCode.NONE, create("t", 2));
        assertEquals(ErrorCode.TOPIC_ALREADY_EXISTS, create("t", 2));
        assertEquals(ErrorCode.TOPIC_ALREADY_EXISTS, create("t", 9));       // even with a different count
        assertEquals(2, logs.topic("t").partitionCount());
    }

    @Test
    void createRejectsInvalidNamesAndCounts() throws IOException {
        assertEquals(ErrorCode.INVALID_REQUEST, create("bad name", 1));
        assertEquals(ErrorCode.INVALID_REQUEST, create("..", 1));
        assertEquals(ErrorCode.INVALID_REQUEST, create("t", 0));
        assertEquals(ErrorCode.INVALID_REQUEST, create("t", -3));
        assertEquals(ErrorCode.INVALID_REQUEST, create("t", 17));           // over maxPartitionsPerTopic=16
        assertEquals(ErrorCode.NONE, create("t", 16));
        assertEquals(List.of("t"), logs.topics().stream().map(Topic::name).toList());
    }

    @Test
    void sixteenConcurrentCreatesOfTheSameTopicYieldExactlyOneSuccess() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(16);
        CountDownLatch go = new CountDownLatch(1);
        AtomicInteger ok = new AtomicInteger(), exists = new AtomicInteger();
        List<Future<?>> fs = new ArrayList<>();
        for (int i = 0; i < 16; i++) {
            final int count = 1 + i % 5;
            fs.add(pool.submit(() -> {
                go.await();
                ErrorCode code = create("contended", count);
                if (code == ErrorCode.NONE) ok.incrementAndGet();
                else if (code == ErrorCode.TOPIC_ALREADY_EXISTS) exists.incrementAndGet();
                else fail("unexpected " + code);
                return null;
            }));
        }
        go.countDown();
        for (Future<?> f : fs) f.get(30, TimeUnit.SECONDS);
        pool.shutdown();
        assertEquals(1, ok.get());
        assertEquals(15, exists.get());
    }

    // ---- PRODUCE / FETCH partition rules

    @Test
    void autoCreateUsesDefaultPartitionsAndOffsetsAreIndependentPerPartition() throws IOException {
        assertEquals(0, produce("auto", 0, "a", "b", "c").body().baseOffset());
        assertEquals(3, logs.topic("auto").partitionCount());
        assertEquals(0, produce("auto", 2, "x").body().baseOffset());       // partition 2 starts at its own 0
        assertEquals(3, produce("auto", 0, "d").body().baseOffset());
        assertEquals(2, produce("auto", 2, "y").body().partition());        // the response names the partition
    }

    @Test
    void produceToAMissingPartitionIsUnknown() throws IOException {
        create("t", 2);
        assertEquals(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, produce("t", 2, "x").error());
        assertEquals(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, produce("t", -1, "x").error());
        assertEquals(ErrorCode.NONE, produce("t", 1, "x").error());
    }

    @Test
    void autoCreateOnlyWhenThePartitionIsBelowTheDefault() throws IOException {
        assertEquals(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, produce("nope", 3, "x").error());
        assertNull(logs.topic("nope"));
        assertEquals(ErrorCode.NONE, produce("nope", 2, "x").error());
    }

    @Test
    void fetchValidatesThePartition() throws IOException {
        create("t", 2);
        produce("t", 1, "hello");
        assertEquals(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, apis.handleFetch(new FetchRequest("t", 2, 0, 100)).error());
        assertEquals(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, apis.handleFetch(new FetchRequest("t", -1, 0, 100)).error());
        assertEquals(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, apis.handleFetch(new FetchRequest("zzz", 0, 0, 100)).error());
        LocalBrokerApis.FetchOutcome ok = apis.handleFetch(new FetchRequest("t", 1, 0, 100));
        try (LogSlice slice = ok.slice()) {
            assertEquals(ErrorCode.NONE, ok.error());
            assertEquals(1, ok.partition());
            assertEquals(1, slice.recordCount);
        }
        LocalBrokerApis.FetchOutcome empty0 = apis.handleFetch(new FetchRequest("t", 0, 0, 100));
        assertEquals(ErrorCode.NONE, empty0.error());                       // partition 0 exists and is empty
        assertEquals(0, empty0.slice().length);
        assertEquals(ErrorCode.OFFSET_OUT_OF_RANGE, apis.handleFetch(new FetchRequest("t", 1, 5, 100)).error());
    }

    // ---- METADATA v0 / v1

    @Test
    void metadataV1ListsEveryPartitionWithTheLeader() throws IOException {
        create("b", 3);
        create("a", 2);
        produce("b", 1, "x", "y");
        MetadataResponse m = apis.handleMetadata(new MetadataRequest(List.of()), (short) 1).body();
        assertEquals(new MetadataResponse.BrokerInfo(5, "example.org", 4242), m.broker());
        assertEquals(List.of("a", "b"), m.topics().stream().map(MetadataResponse.TopicMetadata::topic).toList());
        assertEquals(List.of(0, 1, 2), m.topics().get(1).partitions().stream().map(MetadataResponse.PartitionMetadata::partition).toList());
        assertEquals(new MetadataResponse.PartitionMetadata(1, 5, 0, 2), m.topics().get(1).partitions().get(1));
        assertEquals(new MetadataResponse.PartitionMetadata(0, 5, 0, 0), m.topics().get(0).partitions().get(0));
    }

    @Test
    void metadataV0HasNoLeaderAndOtherwiseTheSameNumbers() throws IOException {
        create("t", 2);
        produce("t", 0, "x");
        MetadataResponse v0 = apis.handleMetadata(new MetadataRequest(List.of("t"))).body();
        assertEquals(new MetadataResponse.PartitionMetadata(0, 0, 1), v0.topics().get(0).partitions().get(0));
        assertEquals(MetadataResponse.PartitionMetadata.NO_LEADER, v0.topics().get(0).partitions().get(0).leaderNodeId());
        MetadataResponse explicit = apis.handleMetadata(new MetadataRequest(List.of("t")), (short) 0).body();
        assertEquals(v0, explicit);
    }

    @Test
    void metadataForSpecificAndUnknownTopics() throws IOException {
        create("a", 1);
        create("b", 1);
        MetadataResponse some = apis.handleMetadata(new MetadataRequest(List.of("b", "ghost", "b")), (short) 1).body();
        assertEquals(List.of("b"), some.topics().stream().map(MetadataResponse.TopicMetadata::topic).toList());
        assertTrue(apis.handleMetadata(new MetadataRequest(List.of("ghost")), (short) 1).body().topics().isEmpty());
    }

    // ---- restart

    @Test
    void restartKeepsCountsAndData() throws IOException {
        create("keep", 4);
        for (int p = 0; p < 4; p++) {
            produce("keep", p, "p" + p + "-a", "p" + p + "-b");
        }
        logs.close();
        open("--default-partitions=9");                                       // a different default must not matter
        MetadataResponse m = apis.handleMetadata(new MetadataRequest(List.of()), (short) 1).body();
        assertEquals(4, m.topics().get(0).partitions().size());
        for (var p : m.topics().get(0).partitions()) {
            assertEquals(2, p.logEndOffset());
        }
        assertEquals("p3-b", new String(logs.logFor("keep", 3).read(1).value()));
        assertEquals(ErrorCode.TOPIC_ALREADY_EXISTS, create("keep", 4));
    }
}
