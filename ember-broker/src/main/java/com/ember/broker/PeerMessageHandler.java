package com.ember.broker;

import com.ember.protocol.ApiKey;
import com.ember.protocol.ProtocolException;
import com.ember.protocol.RaftMessageRequest;
import com.ember.protocol.RequestCodec;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;

import java.util.function.Consumer;

/**
 * Raft messages from peers are handled HERE, on the I/O event loop, and never on the pool that runs client requests.
 *
 * WHY: client PRODUCE requests block their handler thread until the entry is committed, and Netty pins a connection to
 * one handler thread. If a peer connection shared a thread with a blocked produce, the very acknowledgement that the
 * produce is waiting for would be stuck behind it (and heartbeats would be delayed, making leaders resign). The
 * dispatch itself is non-blocking: it only puts the message into the mailbox of the right Raft group.
 */
final class PeerMessageHandler extends ChannelInboundHandlerAdapter {

    private static final System.Logger LOG = System.getLogger(PeerMessageHandler.class.getName());

    private final Consumer<RaftMessageRequest> sink;

    PeerMessageHandler(Consumer<RaftMessageRequest> sink) {
        this.sink = sink;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof ByteBuf frame && frame.readableBytes() >= 2 && frame.getShort(frame.readerIndex()) == ApiKey.RAFT_MESSAGE.code()) {
            try {
                if (RequestCodec.decode(frame.nioBuffer()).request() instanceof RaftMessageRequest raft) {
                    sink.accept(raft);
                }
            } catch (ProtocolException e) {
                LOG.log(System.Logger.Level.WARNING, "ignoring a malformed raft message from {0}: {1}", ctx.channel().remoteAddress(), e.getMessage());
            } finally {
                ReferenceCountUtil.release(frame);
            }
            return;                                         // never forwarded: no response is ever sent for a Raft message
        }
        ctx.fireChannelRead(msg);
    }
}
