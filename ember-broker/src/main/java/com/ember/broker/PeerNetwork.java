package com.ember.broker;

import com.ember.protocol.ApiKey;
import com.ember.protocol.RaftMessageRequest;
import com.ember.protocol.RequestCodec;
import com.ember.protocol.RequestHeader;
import com.ember.raft.RaftMessage;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Broker-to-broker traffic: ONE outbound connection per peer carrying the Raft messages of ALL groups.
 * One-way frames, a bounded queue per peer (when full the NEWEST message is dropped and counted: Raft tolerates
 * loss), reconnect with exponential back-off, and it never blocks the calling group thread.
 */
final class PeerNetwork implements ReplicaManager.RaftSender, AutoCloseable {

    private static final System.Logger LOG = System.getLogger(PeerNetwork.class.getName());

    private final int selfId;
    private final NioEventLoopGroup eventLoop = new NioEventLoopGroup(1);
    private final Bootstrap bootstrap;
    private final Map<Integer, Link> links = new TreeMap<>();
    private final Set<Integer> blocked = ConcurrentHashMap.newKeySet();
    private volatile boolean closed;

    PeerNetwork(int selfId, Map<Integer, InetSocketAddress> peers, int queueSize) {
        this.selfId = selfId;
        this.bootstrap = new Bootstrap().group(eventLoop).channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 1000)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                            @Override
                            public void channelWritabilityChanged(ChannelHandlerContext ctx) {
                                Link link = ctx.channel().attr(LINK).get();
                                if (link != null) link.drain();
                                ctx.fireChannelWritabilityChanged();
                            }
                        });
                    }
                });
        for (Map.Entry<Integer, InetSocketAddress> e : peers.entrySet()) {
            if (e.getKey() != selfId) {
                links.put(e.getKey(), new Link(e.getKey(), e.getValue(), queueSize));
            }
        }
    }

    private static final io.netty.util.AttributeKey<Link> LINK = io.netty.util.AttributeKey.valueOf("ember.link");

    void start() {
        for (Link l : links.values()) l.connect();
    }

    @Override
    public void send(int toNode, String topic, int partition, RaftMessage message) {
        Link link = links.get(toNode);
        if (link == null || closed) {
            return;
        }
        ByteBuffer frame = RequestCodec.encodeFrame(new RequestHeader(ApiKey.RAFT_MESSAGE, (short) 0, 0),
                new RaftMessageRequest(topic, partition, selfId, message));
        byte[] bytes = new byte[frame.remaining()];
        frame.get(bytes);
        link.send(bytes);
    }

    // ---- test hooks: simulate network partitions at the real-socket level

    void block(int peer) {
        blocked.add(peer);
    }

    void unblock(int peer) {
        blocked.remove(peer);
    }

    void blockAll() {
        blocked.addAll(links.keySet());
    }

    void unblockAll() {
        blocked.clear();
    }

    boolean isBlocked(int peer) {
        return blocked.contains(peer);
    }

    long droppedMessages() {
        long total = 0;
        for (Link l : links.values()) total += l.dropped.get();
        return total;
    }

    @Override
    public void close() {
        closed = true;
        for (Link l : links.values()) {
            Channel ch = l.channel;
            if (ch != null) ch.close();
        }
        eventLoop.shutdownGracefully(0, 1, TimeUnit.SECONDS).awaitUninterruptibly(3, TimeUnit.SECONDS);
    }

    private final class Link {
        final int peerId;
        final InetSocketAddress address;
        final LinkedBlockingQueue<byte[]> queue;
        final AtomicLong dropped = new AtomicLong();
        final AtomicBoolean drainScheduled = new AtomicBoolean();
        volatile Channel channel;
        long backoffMs = 100;

        Link(int peerId, InetSocketAddress address, int queueSize) {
            this.peerId = peerId;
            this.address = address;
            this.queue = new LinkedBlockingQueue<>(queueSize);
        }

        void send(byte[] frame) {
            if (blocked.contains(peerId) || !queue.offer(frame)) {
                dropped.incrementAndGet();                          // full (or "cut"): drop the NEWEST message, Raft copes
                return;
            }
            Channel ch = channel;
            if (ch != null && ch.isActive() && drainScheduled.compareAndSet(false, true)) {
                ch.eventLoop().execute(() -> {
                    drainScheduled.set(false);
                    drain();
                });
            }
        }

        void drain() {
            Channel ch = channel;
            if (ch == null || !ch.isActive()) {
                return;
            }
            boolean wrote = false;
            byte[] frame;
            while (ch.isWritable() && (frame = queue.poll()) != null) {
                ch.write(Unpooled.wrappedBuffer(frame));
                wrote = true;
            }
            if (wrote) {
                ch.flush();
            }
        }

        void connect() {
            if (closed) {
                return;
            }
            bootstrap.connect(address).addListener((ChannelFutureListener) f -> {
                if (f.isSuccess()) {
                    Channel ch = f.channel();
                    ch.attr(LINK).set(this);
                    backoffMs = 100;
                    channel = ch;
                    ch.closeFuture().addListener(c -> {
                        channel = null;
                        dropped.addAndGet(queue.size());
                        queue.clear();                              // stale messages are useless; heartbeats resynchronize
                        scheduleReconnect();
                    });
                    drain();
                } else {
                    scheduleReconnect();
                }
            });
        }

        void scheduleReconnect() {
            if (closed) {
                return;
            }
            long delay = backoffMs;
            backoffMs = Math.min(backoffMs * 2, 2000);
            try {
                eventLoop.schedule(this::connect, delay, TimeUnit.MILLISECONDS);
            } catch (RuntimeException shuttingDown) {
                LOG.log(System.Logger.Level.DEBUG, "peer {0}: not reconnecting, shutting down", peerId);
            }
        }
    }
}
