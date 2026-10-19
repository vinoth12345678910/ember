package com.ember.protocol;

import com.ember.raft.EntryType;
import com.ember.raft.LogEntry;
import com.ember.raft.RaftMessage;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class Phase5CodecTest {

    private static final int CORR = 9;

    private static RaftMessageRequest roundTrip(RaftMessage m) {
        RaftMessageRequest in = new RaftMessageRequest("orders", 3, 2, m);
        RequestCodec.Decoded d = RequestCodec.decode(RequestCodec.encode(new RequestHeader(ApiKey.RAFT_MESSAGE, (short) 0, 0), in));
        assertEquals(ApiKey.RAFT_MESSAGE, d.header().apiKey());
        return (RaftMessageRequest) d.request();
    }

    private static ByteBuffer raw(int apiKey, int version, java.util.function.Consumer<ByteBuffer> w) {
        ByteBuffer buf = ByteBuffer.allocate(512);
        buf.putShort((short) apiKey).putShort((short) version).putInt(CORR);
        w.accept(buf);
        buf.flip();
        return buf;
    }

    private static void str(ByteBuffer buf, String s) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        buf.putShort((short) b.length).put(b);
    }

    private static ProtocolException fails(ByteBuffer b) {
        return assertThrows(ProtocolException.class, () -> RequestCodec.decode(b));
    }

    // ---- RAFT_MESSAGE round trips

    @Test
    void requestVote() {
        RaftMessage.RequestVote m = new RaftMessage.RequestVote(7, 2, 100, 6);
        RaftMessageRequest r = roundTrip(m);
        assertEquals(new RaftMessageRequest("orders", 3, 2, m), r);
    }

    @Test
    void requestVoteResponse() {
        assertEquals(new RaftMessage.RequestVoteResponse(7, true), roundTrip(new RaftMessage.RequestVoteResponse(7, true)).message());
        assertEquals(new RaftMessage.RequestVoteResponse(0, false), roundTrip(new RaftMessage.RequestVoteResponse(0, false)).message());
    }

    @Test
    void appendEntriesWithNoopAndCommandEntries() {
        List<LogEntry> entries = List.of(LogEntry.noop(5), LogEntry.command(5, "hello".getBytes()),
                LogEntry.command(6, new byte[0]), LogEntry.command(6, new byte[]{0, (byte) 0xFF, 7}));
        RaftMessage.AppendEntries m = new RaftMessage.AppendEntries(6, 1, 41, 5, entries, 40);
        RaftMessage back = roundTrip(m).message();
        assertEquals(m, back);
        assertEquals(EntryType.NOOP, ((RaftMessage.AppendEntries) back).entries().get(0).type());
    }

    @Test
    void aHeartbeatHasNoEntries() {
        RaftMessage.AppendEntries m = new RaftMessage.AppendEntries(2, 1, 0, 0, List.of(), 0);
        assertEquals(m, roundTrip(m).message());
    }

    @Test
    void appendEntriesResponseSuccessAndFailureHints() {
        RaftMessage.AppendEntriesResponse ok = new RaftMessage.AppendEntriesResponse(3, true, 99, 0, 0);
        RaftMessage.AppendEntriesResponse fail = new RaftMessage.AppendEntriesResponse(3, false, 0, 17, 2);
        assertEquals(ok, roundTrip(ok).message());
        assertEquals(fail, roundTrip(fail).message());
    }

    @Test
    void theMetaGroupNameIsAValidGroupTopic() {
        RaftMessageRequest in = new RaftMessageRequest("__meta", 0, 1, new RaftMessage.RequestVoteResponse(1, true));
        RequestCodec.Decoded d = RequestCodec.decode(RequestCodec.encode(new RequestHeader(ApiKey.RAFT_MESSAGE, (short) 0, 0), in));
        assertEquals(in, d.request());
    }

    // ---- RAFT_MESSAGE malformed

    @Test
    void unknownMessageTypeAndTrailingBytesAndTruncation() {
        assertEquals(ErrorCode.INVALID_REQUEST, fails(raw(10, 0, b -> { str(b, "t"); b.putInt(0).putInt(1).put((byte) 9); })).errorCode());
        assertEquals(ErrorCode.INVALID_REQUEST, fails(raw(10, 0, b -> { str(b, "t"); b.putInt(0).putInt(1).put((byte) 2).putLong(1).put((byte) 1).put((byte) 0); })).errorCode());
        assertEquals(ErrorCode.INVALID_REQUEST, fails(raw(10, 0, b -> { str(b, "t"); b.putInt(0).putInt(1).put((byte) 2).putLong(1); })).errorCode());
        assertEquals(CORR, fails(raw(10, 0, b -> { str(b, "t"); b.putInt(0); })).correlationId());
    }

    @Test
    void booleansMustBeZeroOrOne() {
        assertEquals(ErrorCode.INVALID_REQUEST, fails(raw(10, 0, b -> { str(b, "t"); b.putInt(0).putInt(1).put((byte) 2).putLong(1).put((byte) 2); })).errorCode());
        assertEquals(ErrorCode.INVALID_REQUEST, fails(raw(10, 0, b -> { str(b, "t"); b.putInt(0).putInt(1).put((byte) 4).putLong(1).put((byte) 7).putLong(1).putLong(1).putLong(1); })).errorCode());
    }

    @Test
    void aHugeEntryCountIsRejectedBeforeAllocating() {
        assertEquals(ErrorCode.INVALID_REQUEST, fails(raw(10, 0, b -> {
            str(b, "t"); b.putInt(0).putInt(1).put((byte) 3).putLong(1).putInt(1).putLong(0).putLong(0).putLong(0).putInt(Integer.MAX_VALUE);
        })).errorCode());
        assertEquals(ErrorCode.INVALID_REQUEST, fails(raw(10, 0, b -> {
            str(b, "t"); b.putInt(0).putInt(1).put((byte) 3).putLong(1).putInt(1).putLong(0).putLong(0).putLong(0).putInt(-1);
        })).errorCode());
    }

    @Test
    void anEntryLengthPastTheEndOrNegativeOrOfAnUnknownTypeIsRejected() {
        assertEquals(ErrorCode.INVALID_REQUEST, fails(raw(10, 0, b -> {
            str(b, "t"); b.putInt(0).putInt(1).put((byte) 3).putLong(1).putInt(1).putLong(0).putLong(0).putLong(0).putInt(1)
                    .putLong(1).put((byte) 1).putInt(5000).put((byte) 1);
        })).errorCode());
        assertEquals(ErrorCode.INVALID_REQUEST, fails(raw(10, 0, b -> {
            str(b, "t"); b.putInt(0).putInt(1).put((byte) 3).putLong(1).putInt(1).putLong(0).putLong(0).putLong(0).putInt(1)
                    .putLong(1).put((byte) 1).putInt(-3);
        })).errorCode());
        assertEquals(ErrorCode.INVALID_REQUEST, fails(raw(10, 0, b -> {
            str(b, "t"); b.putInt(0).putInt(1).put((byte) 3).putLong(1).putInt(1).putLong(0).putLong(0).putLong(0).putInt(1)
                    .putLong(1).put((byte) 5).putInt(0);
        })).errorCode());
    }

    @Test
    void negativeTermsIndexesAndPartitionsAreRejected() {
        assertEquals(ErrorCode.INVALID_REQUEST, fails(raw(10, 0, b -> { str(b, "t"); b.putInt(0).putInt(1).put((byte) 1).putLong(-1).putInt(1).putLong(0).putLong(0); })).errorCode());
        assertEquals(ErrorCode.INVALID_REQUEST, fails(raw(10, 0, b -> { str(b, "t"); b.putInt(-1).putInt(1).put((byte) 2).putLong(1).put((byte) 1); })).errorCode());
    }

    @Test
    void aBadGroupTopicIsRejected() {
        assertEquals(ErrorCode.INVALID_REQUEST, fails(raw(10, 0, b -> { str(b, "bad topic!"); b.putInt(0).putInt(1).put((byte) 2).putLong(1).put((byte) 1); })).errorCode());
        assertThrows(ProtocolException.class, () -> RequestCodec.encode(new RequestHeader(ApiKey.RAFT_MESSAGE, (short) 0, 0),
                new RaftMessageRequest("bad topic", 0, 1, new RaftMessage.RequestVoteResponse(1, true))));
    }

    @Test
    void raftMessageOnlyExistsInVersionZeroAndTheNextApiKeyIsUnknown() {
        assertEquals(ErrorCode.UNSUPPORTED_VERSION, fails(raw(10, 1, b -> { })).errorCode());
        assertEquals(ErrorCode.UNSUPPORTED_API, fails(raw(11, 0, b -> { })).errorCode());
        assertThrows(ProtocolException.class, () -> ResponseCodec.decode(ApiKey.RAFT_MESSAGE, ByteBuffer.allocate(6)));
    }

    // ---- error codes

    @Test
    void newErrorCodesRoundTripThroughAnErrorFrame() {
        int[] codes = {9, 10, 11, 12, 13};
        ErrorCode[] expected = {ErrorCode.NOT_LEADER_OR_FOLLOWER, ErrorCode.REQUEST_TIMED_OUT, ErrorCode.LEADER_NOT_AVAILABLE,
                ErrorCode.NOT_CONTROLLER, ErrorCode.BROKER_OVERLOADED};
        for (int i = 0; i < codes.length; i++) {
            assertEquals(codes[i], expected[i].code());
            ResponseCodec.Decoded d = ResponseCodec.decode(ApiKey.PRODUCE, ResponseCodec.encodeError(5, expected[i]));
            assertEquals(expected[i], d.errorCode());
            assertNull(d.body());
        }
        assertThrows(ProtocolException.class, () -> ErrorCode.fromCode((short) 14));
    }

    // ---- METADATA v2

    private static ClusterMetadataResponse sample() {
        return new ClusterMetadataResponse(
                List.of(new ClusterMetadataResponse.Broker(1, "h1", 9001), new ClusterMetadataResponse.Broker(2, "h2", 9002),
                        new ClusterMetadataResponse.Broker(3, "h3", 9003)),
                2,
                List.of(new ClusterMetadataResponse.Topic("a", List.of(
                                new ClusterMetadataResponse.Partition(0, 1, List.of(1, 2, 3), 0, 10),
                                new ClusterMetadataResponse.Partition(1, -1, List.of(1, 2, 3), 5, 6))),
                        new ClusterMetadataResponse.Topic("b", List.of())));
    }

    @Test
    void metadataV2RoundTrips() {
        ClusterMetadataResponse in = sample();
        ResponseCodec.Decoded d = ResponseCodec.decode(ApiKey.METADATA, (short) 2, ResponseCodec.encode(4, ErrorCode.NONE, in, (short) 2));
        assertEquals(in, d.body());
        ClusterMetadataResponse empty = new ClusterMetadataResponse(List.of(), -1, List.of());
        assertEquals(empty, ResponseCodec.decode(ApiKey.METADATA, (short) 2, ResponseCodec.encode(4, ErrorCode.NONE, empty, (short) 2)).body());
    }

    @Test
    void metadataRequestVersionTwoIsTheSameBodyAsVersionZero() {
        MetadataRequest req = new MetadataRequest(List.of("a"));
        ByteBuffer v0 = RequestCodec.encode(new RequestHeader(ApiKey.METADATA, (short) 0, 1), req);
        ByteBuffer v2 = RequestCodec.encode(new RequestHeader(ApiKey.METADATA, (short) 2, 1), req);
        assertEquals(v0.remaining(), v2.remaining());
        assertEquals(2, RequestCodec.decode(v2).header().apiVersion());
        assertEquals(ErrorCode.UNSUPPORTED_VERSION, fails(raw(2, 3, b -> b.putInt(0))).errorCode());
    }

    @Test
    void metadataV2MalformedCounts() {
        ByteBuffer hugeBrokers = ByteBuffer.allocate(40).putInt(1).putShort(ErrorCode.NONE.code()).putInt(Integer.MAX_VALUE).flip();
        assertThrows(ProtocolException.class, () -> ResponseCodec.decode(ApiKey.METADATA, (short) 2, hugeBrokers));
        ByteBuffer ok = ResponseCodec.encode(1, ErrorCode.NONE, sample(), (short) 2);
        ByteBuffer longer = ByteBuffer.allocate(ok.remaining() + 1).put(ok).put((byte) 1).flip();
        assertThrows(ProtocolException.class, () -> ResponseCodec.decode(ApiKey.METADATA, (short) 2, longer));
        ByteBuffer ok2 = ResponseCodec.encode(1, ErrorCode.NONE, sample(), (short) 2);
        ByteBuffer cut = ok2.duplicate();
        cut.limit(cut.limit() - 3);
        assertThrows(ProtocolException.class, () -> ResponseCodec.decode(ApiKey.METADATA, (short) 2, cut));
    }

    @Test
    void aHugeReplicaCountIsRejectedBeforeAllocating() {
        ByteBuffer b = ByteBuffer.allocate(100);
        b.putInt(1).putShort(ErrorCode.NONE.code());
        b.putInt(0);                              // no brokers
        b.putInt(-1);                             // controller
        b.putInt(1);                              // one topic
        str(b, "t");
        b.putInt(1);                              // one partition
        b.putInt(0).putInt(0).putInt(Integer.MAX_VALUE);
        b.flip();
        assertThrows(ProtocolException.class, () -> ResponseCodec.decode(ApiKey.METADATA, (short) 2, b));
    }

    @Test
    void versionsZeroAndOneStillDecode() {
        MetadataResponse v1 = new MetadataResponse(new MetadataResponse.BrokerInfo(1, "h", 2), List.of(
                new MetadataResponse.TopicMetadata("a", List.of(new MetadataResponse.PartitionMetadata(0, 1, 0, 3)))));
        assertEquals(v1, ResponseCodec.decode(ApiKey.METADATA, (short) 1, ResponseCodec.encode(1, ErrorCode.NONE, v1, (short) 1)).body());
        MetadataResponse v0 = (MetadataResponse) ResponseCodec.decode(ApiKey.METADATA, (short) 0, ResponseCodec.encode(1, ErrorCode.NONE, v1, (short) 0)).body();
        assertEquals(MetadataResponse.PartitionMetadata.NO_LEADER, v0.topics().get(0).partitions().get(0).leaderNodeId());
    }
}
