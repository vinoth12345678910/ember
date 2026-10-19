package com.ember.protocol;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

class Phase6CodecTest {

    private static final int CORR = 77;

    private static final List<Request> REQUESTS = List.of(
            new FindCoordinatorRequest("g1"),
            new JoinGroupRequest("g1", "", "client-a", 10_000, List.of("orders", "payments")),
            new JoinGroupRequest("g1", "client-a-123", "client-a", 10_000, List.of("orders")),
            new HeartbeatRequest("g1", "m1", 7),
            new LeaveGroupRequest("g1", "m1"),
            new OffsetCommitRequest("g1", "m1", 3, List.of(new PartitionOffset("orders", 0, 42), new PartitionOffset("orders", 5, 0))),
            new OffsetCommitRequest("g1", "m1", 3, List.of()),
            new OffsetFetchRequest("g1", List.of(new PartitionRef("orders", 0), new PartitionRef("payments", 2))),
            new OffsetFetchRequest("g1", List.of()),
            new DescribeGroupRequest("g1"));

    private static final List<Response> RESPONSES = List.of(
            new FindCoordinatorResponse(2, "localhost", 19002),
            new JoinGroupResponse(4, "client-a-123", List.of(new PartitionRef("orders", 1), new PartitionRef("orders", 4))),
            new JoinGroupResponse(1, "m", List.of()),
            new HeartbeatResponse(),
            new LeaveGroupResponse(),
            new OffsetCommitResponse(),
            new OffsetFetchResponse(List.of(new PartitionOffset("orders", 0, 42), new PartitionOffset("orders", 1, -1))),
            new DescribeGroupResponse(5, DescribeGroupResponse.STABLE, List.of(
                    new DescribeGroupResponse.Member("m1", "c1", 10_000, List.of("orders"), List.of(new PartitionRef("orders", 0))),
                    new DescribeGroupResponse.Member("m2", "c2", 5_000, List.of("orders", "payments"), List.of())),
                    List.of(new PartitionOffset("orders", 0, 9))),
            new DescribeGroupResponse(0, DescribeGroupResponse.UNKNOWN, List.of(), List.of()));

    private static ApiKey keyOf(Response r) {
        return switch (r) {
            case FindCoordinatorResponse x -> ApiKey.FIND_COORDINATOR;
            case JoinGroupResponse x -> ApiKey.JOIN_GROUP;
            case HeartbeatResponse x -> ApiKey.HEARTBEAT;
            case LeaveGroupResponse x -> ApiKey.LEAVE_GROUP;
            case OffsetCommitResponse x -> ApiKey.OFFSET_COMMIT;
            case OffsetFetchResponse x -> ApiKey.OFFSET_FETCH;
            case DescribeGroupResponse x -> ApiKey.DESCRIBE_GROUP;
            default -> throw new IllegalArgumentException();
        };
    }

    private static ByteBuffer encode(Request r) {
        return RequestCodec.encode(new RequestHeader(r.apiKey(), (short) 0, CORR), r);
    }

    private static ByteBuffer raw(ApiKey key, int version, Consumer<ByteBuffer> body) {
        ByteBuffer buf = ByteBuffer.allocate(4096);
        buf.putShort(key.code()).putShort((short) version).putInt(CORR);
        body.accept(buf);
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

    @Test
    void everyRequestRoundTrips() {
        for (Request r : REQUESTS) {
            RequestCodec.Decoded d = RequestCodec.decode(encode(r));
            assertEquals(r, d.request());
            assertEquals(r.apiKey(), d.header().apiKey());
            assertEquals(CORR, d.header().correlationId());
        }
    }

    @Test
    void everyResponseRoundTrips() {
        for (Response r : RESPONSES) {
            ResponseCodec.Decoded d = ResponseCodec.decode(keyOf(r), ResponseCodec.encode(CORR, ErrorCode.NONE, r));
            assertEquals(r, d.body());
            assertEquals(ErrorCode.NONE, d.errorCode());
            assertEquals(CORR, d.correlationId());
        }
    }

    @Test
    void errorResponsesHaveNoBodyForEveryNewCode() {
        for (ErrorCode e : List.of(ErrorCode.NOT_COORDINATOR, ErrorCode.UNKNOWN_MEMBER_ID, ErrorCode.ILLEGAL_GENERATION,
                ErrorCode.REBALANCE_IN_PROGRESS, ErrorCode.COORDINATOR_NOT_AVAILABLE, ErrorCode.INVALID_SESSION_TIMEOUT,
                ErrorCode.PARTITION_NOT_ASSIGNED)) {
            assertEquals(e, ErrorCode.fromCode(e.code()));
            ResponseCodec.Decoded d = ResponseCodec.decode(ApiKey.OFFSET_COMMIT, ResponseCodec.encodeError(CORR, e));
            assertEquals(e, d.errorCode());
            assertNull(d.body());
        }
        assertEquals(14, ErrorCode.NOT_COORDINATOR.code());
        assertEquals(20, ErrorCode.PARTITION_NOT_ASSIGNED.code());
        assertThrows(ProtocolException.class, () -> ErrorCode.fromCode((short) 21));
    }

    @Test
    void apiKeysHaveTheDocumentedNumbersAndOnlyVersionZero() {
        assertEquals(11, ApiKey.FIND_COORDINATOR.code());
        assertEquals(17, ApiKey.DESCRIBE_GROUP.code());
        for (ApiKey k : List.of(ApiKey.FIND_COORDINATOR, ApiKey.JOIN_GROUP, ApiKey.HEARTBEAT, ApiKey.LEAVE_GROUP,
                ApiKey.OFFSET_COMMIT, ApiKey.OFFSET_FETCH, ApiKey.DESCRIBE_GROUP)) {
            assertTrue(k.supportsVersion((short) 0));
            assertFalse(k.supportsVersion((short) 1));
        }
        ProtocolException e = fails(raw(ApiKey.HEARTBEAT, 1, b -> {
        }));
        assertEquals(ErrorCode.UNSUPPORTED_VERSION, e.errorCode());
        assertEquals(CORR, e.correlationId());
    }

    @Test
    void anyTruncationOfAnyRequestIsRejectedWithTheCorrelationId() {
        for (Request r : REQUESTS) {
            ByteBuffer full = encode(r);
            for (int len = 8; len < full.remaining(); len++) {
                ByteBuffer cut = full.duplicate();
                cut.limit(len);
                ProtocolException e = fails(cut);
                assertEquals(CORR, e.correlationId(), r + " cut at " + len);
            }
        }
    }

    @Test
    void trailingBytesAreAnError() {
        for (Request r : REQUESTS) {
            ByteBuffer full = encode(r);
            ByteBuffer longer = ByteBuffer.allocate(full.remaining() + 1);
            longer.put(full).put((byte) 0).flip();
            assertTrue(fails(longer).getMessage().contains("trailing"), r.toString());
        }
    }

    @Test
    void trailingBytesInAResponseAreAnError() {
        ByteBuffer ok = ResponseCodec.encode(CORR, ErrorCode.NONE, new HeartbeatResponse());
        ByteBuffer longer = ByteBuffer.allocate(ok.remaining() + 1);
        longer.put(ok).put((byte) 1).flip();
        assertThrows(ProtocolException.class, () -> ResponseCodec.decode(ApiKey.HEARTBEAT, longer));
    }

    @Test
    void negativeAndHugeCountsAreRejectedBeforeAllocating() {
        for (int bad : new int[]{-1, Integer.MIN_VALUE, 10_001, Integer.MAX_VALUE}) {
            fails(raw(ApiKey.JOIN_GROUP, 0, b -> {
                str(b, "g");
                str(b, "");
                str(b, "c");
                b.putInt(1000).putInt(bad);
            }));
            fails(raw(ApiKey.OFFSET_COMMIT, 0, b -> {
                str(b, "g");
                str(b, "m");
                b.putInt(1).putInt(bad);
            }));
            fails(raw(ApiKey.OFFSET_FETCH, 0, b -> {
                str(b, "g");
                b.putInt(bad);
            }));
        }
        // a count that is within the limit but larger than the bytes that follow
        fails(raw(ApiKey.OFFSET_FETCH, 0, b -> {
            str(b, "g");
            b.putInt(9_999);
        }));
    }

    @Test
    void listsAboveTheLimitCannotBeEncodedEither() {
        List<PartitionRef> many = java.util.stream.IntStream.range(0, 10_001).mapToObj(i -> new PartitionRef("t", i)).toList();
        assertThrows(ProtocolException.class, () -> encode(new OffsetFetchRequest("g", many)));
        List<PartitionRef> limit = many.subList(0, 10_000);
        assertEquals(10_000, ((OffsetFetchRequest) RequestCodec.decode(encode(new OffsetFetchRequest("g", limit))).request()).partitions().size());
    }

    @Test
    void idsMustBeNonEmptyAndAtMost255Bytes() {
        String max = "x".repeat(255);
        assertEquals(new FindCoordinatorRequest(max), RequestCodec.decode(encode(new FindCoordinatorRequest(max))).request());
        assertThrows(ProtocolException.class, () -> encode(new FindCoordinatorRequest("")));
        assertThrows(ProtocolException.class, () -> encode(new FindCoordinatorRequest("x".repeat(256))));
        assertThrows(ProtocolException.class, () -> encode(new HeartbeatRequest("g", "", 1)));
        // 128 two-byte characters = 256 bytes: the limit is in BYTES, not characters
        assertThrows(ProtocolException.class, () -> encode(new FindCoordinatorRequest("é".repeat(128))));
        fails(raw(ApiKey.FIND_COORDINATOR, 0, b -> str(b, "")));
        fails(raw(ApiKey.FIND_COORDINATOR, 0, b -> str(b, "x".repeat(256))));
        fails(raw(ApiKey.HEARTBEAT, 0, b -> {
            str(b, "g");
            str(b, "");
            b.putInt(1);
        }));
    }

    @Test
    void anEmptyMemberIdIsLegalOnlyInJoinGroup() {
        assertEquals("", ((JoinGroupRequest) RequestCodec.decode(encode(REQUESTS.get(1))).request()).memberId());
        fails(raw(ApiKey.LEAVE_GROUP, 0, b -> {
            str(b, "g");
            str(b, "");
        }));
        fails(raw(ApiKey.OFFSET_COMMIT, 0, b -> {
            str(b, "g");
            str(b, "");
            b.putInt(1).putInt(0);
        }));
    }

    @Test
    void invalidUtf8AndInvalidTopicNamesAreRejected() {
        fails(raw(ApiKey.FIND_COORDINATOR, 0, b -> b.putShort((short) 2).put((byte) 0xC3).put((byte) 0x28)));
        fails(raw(ApiKey.OFFSET_FETCH, 0, b -> {
            str(b, "g");
            b.putInt(1);
            str(b, "bad topic!");
            b.putInt(0);
        }));
        assertThrows(ProtocolException.class, () -> encode(new JoinGroupRequest("g", "", "c", 1000, List.of("bad topic!"))));
    }

    @Test
    void invalidGroupStateByteIsRejected() {
        ByteBuffer ok = ResponseCodec.encode(CORR, ErrorCode.NONE, new DescribeGroupResponse(1, 2, List.of(), List.of()));
        ByteBuffer bad = ByteBuffer.allocate(ok.remaining()).put(ok).flip();
        bad.put(6 + 4, (byte) 3);                      // correlation(4) + error(2) + generation(4) -> the state byte
        assertThrows(ProtocolException.class, () -> ResponseCodec.decode(ApiKey.DESCRIBE_GROUP, bad));
    }

    @Test
    void theFrameHelperAddsTheLengthPrefix() {
        Request r = new HeartbeatRequest("g", "m", 3);
        ByteBuffer frame = RequestCodec.encodeFrame(new RequestHeader(r.apiKey(), (short) 0, CORR), r);
        assertEquals(frame.remaining() - 4, frame.getInt(0));
    }
}
