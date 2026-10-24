package com.ember.protocol;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class QueueCodecTest {

    private static Request roundTrip(Request r) {
        ByteBuffer body = RequestCodec.encode(new RequestHeader(r.apiKey(), (short) 0, 7), r);
        RequestCodec.Decoded d = RequestCodec.decode(body);
        assertEquals(7, d.header().correlationId());
        return d.request();
    }

    private static Response roundTrip(ApiKey key, Response r) {
        ResponseCodec.Decoded d = ResponseCodec.decode(key, (short) 0, ResponseCodec.encode(9, ErrorCode.NONE, r));
        assertEquals(9, d.correlationId());
        return d.body();
    }

    @Test
    void everyRequestRoundTrips() {
        List<Request> all = List.of(
                new QueueFindRequest(),
                new QueueCreateRequest("matcher", "ride-requests", true, 30_000, 5, 1000, 60_000),
                new QueueNackRequest("matcher", "worker-1", List.of(new QueueHandle(0, 5, 2)), "timeout calling the bank"),
                new QueueNackRequest("matcher", "worker-1", List.of(), ""),
                new QueueDeadListRequest("matcher", 100),
                new QueueSettleRequest(ApiKey.QUEUE_DEAD_RETRY, "matcher", "admin", List.of(new QueueHandle(1, 7, 0)), 0),
                new QueueSettleRequest(ApiKey.QUEUE_DEAD_DISCARD, "matcher", "admin", List.of(new QueueHandle(1, 7, 0)), 0),
                new QueueClaimRequest("matcher", "worker-1", 50, 0, 1 << 20),
                new QueueSettleRequest(ApiKey.QUEUE_ACK, "matcher", "worker-1", List.of(new QueueHandle(0, 5, 1), new QueueHandle(2, 99, 3)), 0),
                new QueueSettleRequest(ApiKey.QUEUE_RELEASE, "matcher", "worker-1", List.of(new QueueHandle(1, 1, 1)), 0),
                new QueueSettleRequest(ApiKey.QUEUE_EXTEND, "matcher", "worker-1", List.of(new QueueHandle(1, 1, 1)), 60_000),
                new QueueSettleRequest(ApiKey.QUEUE_ACK, "matcher", "worker-1", List.of(), 0),
                new QueueDescribeRequest("matcher"));
        for (Request r : all) assertEquals(r, roundTrip(r), r.toString());
    }

    @Test
    void everyResponseRoundTrips() {
        QueueClaimResponse claim = (QueueClaimResponse) roundTrip(ApiKey.QUEUE_CLAIM, new QueueClaimResponse(List.of(
                new QueueMessage(0, 10, 1, 1234L, "k".getBytes(), "v".getBytes()), new QueueMessage(1, 11, 2, 5678L, null, new byte[0]))));
        assertEquals(2, claim.messages().size());
        assertArrayEquals("k".getBytes(), claim.messages().get(0).key());
        assertNull(claim.messages().get(1).key());
        assertEquals(2, claim.messages().get(1).attempt());
        assertEquals(new QueueSettleResponse(3, 1), roundTrip(ApiKey.QUEUE_ACK, new QueueSettleResponse(3, 1)));
        assertEquals(new QueueCreateResponse(6), roundTrip(ApiKey.QUEUE_CREATE, new QueueCreateResponse(6)));
        QueueDescribeResponse d = new QueueDescribeResponse("t", 30_000, 5, 1000, 60_000, List.of(new QueueDescribeResponse.PartitionState(0, 5, 9, 12, 2, 1, 3, 4)));
        assertEquals(d, roundTrip(ApiKey.QUEUE_DESCRIBE, d));
        QueueDeadListResponse dead = (QueueDeadListResponse) roundTrip(ApiKey.QUEUE_DEAD_LIST, new QueueDeadListResponse(3, List.of(
                new QueueDeadListResponse.DeadMessage(1, 42, 3, 3, 1234L, "bank down", "k".getBytes(), "v".getBytes()),
                new QueueDeadListResponse.DeadMessage(0, 7, 5, 5, 99L, "", null, new byte[0]))));
        assertEquals(3, dead.total());
        assertEquals("bank down", dead.items().get(0).reason());
        assertNull(dead.items().get(1).key());
        assertEquals(new QueueSettleResponse(1, 0), roundTrip(ApiKey.QUEUE_NACK, new QueueSettleResponse(1, 0)));
        FindCoordinatorResponse f = new FindCoordinatorResponse(2, "h", 9092);
        assertEquals(f, roundTrip(ApiKey.QUEUE_FIND, f));
    }

    @Test
    void hostileInputIsRejectedWithoutAllocating() {
        // a settle request claiming 2 billion handles but carrying none
        ByteBuffer b = ByteBuffer.allocate(8 + 2 + 1 + 2 + 1 + 4 + 4);
        b.putShort(ApiKey.QUEUE_ACK.code()).putShort((short) 0).putInt(1);
        b.putShort((short) 1).put((byte) 'a').putShort((short) 1).put((byte) 'w').putInt(0).putInt(Integer.MAX_VALUE).flip();
        assertThrows(ProtocolException.class, () -> RequestCodec.decode(b));

        ByteBuffer claim = ByteBuffer.allocate(8 + 3 + 3 + 12);
        claim.putShort(ApiKey.QUEUE_CLAIM.code()).putShort((short) 0).putInt(1);
        claim.putShort((short) 1).put((byte) 'a').putShort((short) 1).put((byte) 'w').putInt(0).putInt(0).putInt(10).flip();      // maxMessages 0
        assertThrows(ProtocolException.class, () -> RequestCodec.decode(claim));

        ByteBuffer truncated = ByteBuffer.allocate(8 + 3);
        truncated.putShort(ApiKey.QUEUE_DESCRIBE.code()).putShort((short) 0).putInt(1).putShort((short) 5).put((byte) 'a').flip();
        assertThrows(ProtocolException.class, () -> RequestCodec.decode(truncated));
    }

    @Test
    void nackAndDeadListAreBoundedAndRejectBadValues() {
        String longReason = "x".repeat(QueueCodec.MAX_REASON_BYTES + 1);
        assertThrows(ProtocolException.class, () -> RequestCodec.encode(new RequestHeader(ApiKey.QUEUE_NACK, (short) 0, 1),
                new QueueNackRequest("s", "w", List.of(), longReason)));
        ByteBuffer b = ByteBuffer.allocate(8 + 3 + 6);
        b.putShort(ApiKey.QUEUE_DEAD_LIST.code()).putShort((short) 0).putInt(1).putShort((short) 1).put((byte) 's').putInt(0).flip();   // maxItems 0
        assertThrows(ProtocolException.class, () -> RequestCodec.decode(b));
        ByteBuffer c = ByteBuffer.allocate(8 + 3 + 6);
        c.putShort(ApiKey.QUEUE_DEAD_LIST.code()).putShort((short) 0).putInt(1).putShort((short) 1).put((byte) 's').putInt(5000).flip();   // above the cap
        assertThrows(ProtocolException.class, () -> RequestCodec.decode(c));
    }

    @Test
    void settleRequestsRejectNonSettleKeys() {
        assertThrows(IllegalArgumentException.class, () -> new QueueSettleRequest(ApiKey.QUEUE_CLAIM, "s", "w", List.of(), 0));
    }
}
