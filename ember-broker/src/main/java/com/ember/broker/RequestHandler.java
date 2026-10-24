package com.ember.broker;

import com.ember.protocol.*;
import com.ember.storage.LogSlice;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.timeout.IdleStateEvent;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * One instance per connection. Runs on the shared handler executor group (NOT on the I/O event
 * loop), because produce/fetch touch the disk. Netty pins a connection to a single executor, so
 * the requests of one connection are handled and answered strictly in order.
 */
final class RequestHandler extends SimpleChannelInboundHandler<ByteBuf> {

    private static final System.Logger LOG = System.getLogger(RequestHandler.class.getName());

    private final java.util.function.Supplier<BrokerApis> apisSupplier;
    private final java.util.function.Consumer<RaftMessageRequest> raftSink;
    private final BrokerMetrics metrics;

    /** apisSupplier returns null until the broker has joined the cluster (between bind() and start()). */
    RequestHandler(java.util.function.Supplier<BrokerApis> apisSupplier, java.util.function.Consumer<RaftMessageRequest> raftSink,
                   BrokerMetrics metrics) {
        this.apisSupplier = apisSupplier;
        this.raftSink = raftSink;
        this.metrics = metrics;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf frame) {
        long startNanos = System.nanoTime();
        RequestCodec.Decoded decoded;
        try {
            decoded = RequestCodec.decode(frame.nioBuffer());
        } catch (ProtocolException e) {
            if (e.correlationId() == null) {
                LOG.log(System.Logger.Level.WARNING, "unreadable request header, closing {0}: {1}",
                        ctx.channel().remoteAddress(), e.getMessage());
                ctx.close();
            } else {
                LOG.log(System.Logger.Level.INFO, "bad request from {0}: {1}", ctx.channel().remoteAddress(), e.getMessage());
                writeError(ctx, e.correlationId(), e.errorCode());
            }
            return;
        }

        int correlationId = decoded.header().correlationId();
        if (decoded.request() instanceof RaftMessageRequest raft) {
            raftSink.accept(raft);                         // one-way: never answered, never blocks
            return;
        }
        BrokerApis apis = apisSupplier.get();
        if (apis == null) {
            writeError(ctx, correlationId, ErrorCode.LEADER_NOT_AVAILABLE);   // bound, but not yet part of the cluster
            return;
        }
        try {
            switch (decoded.request()) {
                case ProduceRequest p -> {
                    BrokerApis.Outcome<ProduceResponse> out = apis.handleProduce(p);
                    ChannelFuture written = writeOutcome(ctx, correlationId, out.error(), out.body());
                    long bytes = 0;
                    for (ProducerRecord r : p.records()) bytes += (r.key() == null ? 0 : r.key().length) + r.value().length;
                    final long producedBytes = bytes;
                    written.addListener(f -> metrics.produce(out.error(), p.records().size(), producedBytes, startNanos));   // latency ends when the answer is on the wire
                }
                case MetadataRequest m -> {
                    metrics.request(ApiKey.METADATA, ErrorCode.NONE);
                    short version = decoded.header().apiVersion();     // answer in the version the client asked in
                    if (version >= 2) {
                        BrokerApis.Outcome<ClusterMetadataResponse> out = apis.handleClusterMetadata(m);
                        writeOutcome(ctx, correlationId, out.error(), out.body(), version);
                    } else {
                        BrokerApis.Outcome<MetadataResponse> out = apis.handleMetadata(m, version);
                        writeOutcome(ctx, correlationId, out.error(), out.body(), version);
                    }
                }
                case CreateTopicRequest c -> {
                    BrokerApis.Outcome<CreateTopicResponse> out = apis.handleCreateTopic(c);
                    metrics.request(ApiKey.CREATE_TOPIC, out.error());
                    writeOutcome(ctx, correlationId, out.error(), out.body());
                }
                case FetchRequest f -> handleFetch(ctx, correlationId, f, apis);
                case RaftMessageRequest r -> throw new IllegalStateException("handled above");
                case FindCoordinatorRequest f -> {
                    BrokerApis.Outcome<FindCoordinatorResponse> out = apis.handleFindCoordinator(f);
                    metrics.request(ApiKey.FIND_COORDINATOR, out.error());
                    writeOutcome(ctx, correlationId, out.error(), out.body());
                }
                case JoinGroupRequest j -> {
                    BrokerApis.Outcome<JoinGroupResponse> out = apis.handleJoinGroup(j);
                    metrics.request(ApiKey.JOIN_GROUP, out.error());
                    writeOutcome(ctx, correlationId, out.error(), out.body());
                }
                case HeartbeatRequest h -> {
                    BrokerApis.Outcome<HeartbeatResponse> out = apis.handleHeartbeat(h);
                    metrics.request(ApiKey.HEARTBEAT, out.error());
                    writeOutcome(ctx, correlationId, out.error(), out.body());
                }
                case LeaveGroupRequest l -> {
                    BrokerApis.Outcome<LeaveGroupResponse> out = apis.handleLeaveGroup(l);
                    metrics.request(ApiKey.LEAVE_GROUP, out.error());
                    writeOutcome(ctx, correlationId, out.error(), out.body());
                }
                case OffsetCommitRequest c -> {
                    BrokerApis.Outcome<OffsetCommitResponse> out = apis.handleOffsetCommit(c);
                    metrics.request(ApiKey.OFFSET_COMMIT, out.error());
                    writeOutcome(ctx, correlationId, out.error(), out.body());
                }
                case OffsetFetchRequest f -> {
                    BrokerApis.Outcome<OffsetFetchResponse> out = apis.handleOffsetFetch(f);
                    metrics.request(ApiKey.OFFSET_FETCH, out.error());
                    writeOutcome(ctx, correlationId, out.error(), out.body());
                }
                case DescribeGroupRequest d -> {
                    BrokerApis.Outcome<DescribeGroupResponse> out = apis.handleDescribeGroup(d);
                    metrics.request(ApiKey.DESCRIBE_GROUP, out.error());
                    writeOutcome(ctx, correlationId, out.error(), out.body());
                }
                case QueueFindRequest q -> {
                    BrokerApis.Outcome<FindCoordinatorResponse> out = apis.handleQueueFind(q);
                    metrics.request(ApiKey.QUEUE_FIND, out.error());
                    writeOutcome(ctx, correlationId, out.error(), out.body());
                }
                case QueueCreateRequest q -> {
                    BrokerApis.Outcome<QueueCreateResponse> out = apis.handleQueueCreate(q);
                    metrics.request(ApiKey.QUEUE_CREATE, out.error());
                    writeOutcome(ctx, correlationId, out.error(), out.body());
                }
                case QueueClaimRequest q -> {
                    BrokerApis.Outcome<QueueClaimResponse> out = apis.handleQueueClaim(q);
                    metrics.request(ApiKey.QUEUE_CLAIM, out.error());
                    writeOutcome(ctx, correlationId, out.error(), out.body());
                }
                case QueueSettleRequest q -> {
                    BrokerApis.Outcome<QueueSettleResponse> out = apis.handleQueueSettle(q);
                    metrics.request(q.apiKey(), out.error());
                    writeOutcome(ctx, correlationId, out.error(), out.body());
                }
                case QueueDescribeRequest q -> {
                    BrokerApis.Outcome<QueueDescribeResponse> out = apis.handleQueueDescribe(q);
                    metrics.request(ApiKey.QUEUE_DESCRIBE, out.error());
                    writeOutcome(ctx, correlationId, out.error(), out.body());
                }
            }
        } catch (Throwable t) {
            LOG.log(System.Logger.Level.ERROR, "internal error handling " + decoded.header().apiKey(), t);
            writeError(ctx, correlationId, ErrorCode.INTERNAL_ERROR);
        }
    }

    /**
     * Zero-copy: a small header buffer, then a FileRegion that Netty sends with
     * FileChannel.transferTo (sendfile). The record bytes never enter the JVM heap.
     */
    private void handleFetch(ChannelHandlerContext ctx, int correlationId, FetchRequest request, BrokerApis apis) throws IOException {
        BrokerApis.FetchOutcome out = apis.handleFetch(request);
        if (out.error() != ErrorCode.NONE) {
            metrics.fetch(out.error(), 0);
            writeError(ctx, correlationId, out.error());
            return;
        }
        LogSlice slice = out.slice();
        metrics.fetch(ErrorCode.NONE, slice.length);
        boolean handedToNetty = false;
        try {
            ByteBuffer header = FetchResponse.encodeHeader(correlationId, out.topic(), out.partition(),
                    out.logStartOffset(), out.highWatermark(), slice.length);
            if (slice.length == 0) {
                ctx.writeAndFlush(Unpooled.wrappedBuffer(header));
                return;
            }
            DefaultFileRegion region = new DefaultFileRegion(slice.channel, slice.position, slice.length);
            handedToNetty = true;                       // Netty closes the channel when the region is released
            ctx.write(Unpooled.wrappedBuffer(header));
            ctx.writeAndFlush(region);
        } finally {
            if (!handedToNetty) {
                slice.close();
            }
        }
    }

    private static ChannelFuture writeOutcome(ChannelHandlerContext ctx, int correlationId, ErrorCode error, Response body) {
        return writeOutcome(ctx, correlationId, error, body, (short) 0);
    }

    private static ChannelFuture writeOutcome(ChannelHandlerContext ctx, int correlationId, ErrorCode error, Response body,
                                              short apiVersion) {
        ByteBuffer frame = error == ErrorCode.NONE
                ? ResponseCodec.encodeFrame(correlationId, ErrorCode.NONE, body, apiVersion)
                : errorFrame(correlationId, error);
        return ctx.writeAndFlush(Unpooled.wrappedBuffer(frame));
    }

    private static ChannelFuture writeError(ChannelHandlerContext ctx, int correlationId, ErrorCode error) {
        return ctx.writeAndFlush(Unpooled.wrappedBuffer(errorFrame(correlationId, error)));
    }

    private static ByteBuffer errorFrame(int correlationId, ErrorCode error) {
        return ResponseCodec.encodeFrame(correlationId, error, null);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            LOG.log(System.Logger.Level.INFO, "closing idle connection {0}", ctx.channel().remoteAddress());
            ctx.close();
        } else {
            super.userEventTriggered(ctx, evt);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (cause instanceof TooLongFrameException || cause instanceof CorruptedFrameException) {
            LOG.log(System.Logger.Level.WARNING, "bad framing from {0}, closing: {1}",
                    ctx.channel().remoteAddress(), cause.getMessage());
        } else if (cause instanceof IOException) {
            LOG.log(System.Logger.Level.DEBUG, "connection problem: {0}", cause.toString());
        } else {
            LOG.log(System.Logger.Level.ERROR, "unexpected error on connection", cause);
        }
        ctx.close();
    }
}
