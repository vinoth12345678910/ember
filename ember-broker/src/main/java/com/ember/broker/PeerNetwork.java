package com.ember.broker;

import com.ember.metrics.Counter;
import com.ember.metrics.Labels;
import com.ember.metrics.MetricsRegistry;
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

/**
 * Broker-to-broker traffic: ONE outbound connection per peer carrying the Raft messages of ALL groups.
 * One-way frames, a bounded queue per peer (when full the NEWEST message is dropped and counted: Raft tolerates
 * loss), reconnect with exponential back-off, and it never blocks the calling group thread.
 */
final class PeerNetwork implements ReplicaManager.RaftSender, AutoCloseable {

    private static final System.Logger LOG = System.getLogger(PeerNetwork.class.getName());

    private final com.ember.replication.FaultInjector faults;
    private final int selfId;
    private final NioEventLoopGroup eventLoop = new NioEventLoopGroup(1);
    private final Bootstrap bootstrap;
    private final Map<Integer, Link> links = new TreeMap<>();
    private final Set<Integer> blocked = ConcurrentHashMap.newKeySet();
    private volatile boolean closed;

    PeerNetwork(int selfId, Map<Integer, InetSocketAddress> peers, int queueSize) {
        this(selfId, peers, queueSize, new MetricsRegistry());
    }

    PeerNetwork(int selfId, Map<Integer, InetSocketAddress> peers, int queueSize, MetricsRegistry registry) {
        this(selfId, peers, queueSize, registry, com.ember.replication.FaultInjector.NONE);
    }

    PeerNetwork(int selfId, Map<Integer, InetSocketAddress> peers, int queueSize, MetricsRegistry registry,
                com.ember.replication.FaultInjector faults) {
        this.faults = faults;
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
                links.put(e.getKey(), new Link(e.getKey(), e.getValue(), queueSize, registry));
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
        if (faults.active()) {                                      // TEST HOOKS (off by default): a lossy / slow network
            if (faults.dropPeerMessage(toNode)) {
                link.dropped.inc();
                return;
            }
            long delay = faults.peerDelayMillis(toNode);
            if (delay > 0) {
                try {
                    eventLoop.schedule(() -> link.send(bytes), delay, TimeUnit.MILLISECONDS);   // one fixed delay keeps the order
                } catch (RuntimeException shuttingDown) {
                    // closing
                }
                return;
            }
        }
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
        for (Link l : links.values()) total += l.dropped.value();
        return total;
    }

    record PeerStatus(int peerId, boolean connected, int queueDepth, long sent, long dropped, long reconnects, boolean blocked) {
    }

    /** Read-only view for /status. */
    java.util.List<PeerStatus> status() {
        java.util.List<PeerStatus> out = new java.util.ArrayList<>();
        for (Link l : links.values()) {
            Channel ch = l.channel;
            out.add(new PeerStatus(l.peerId, ch != null && ch.isActive(), l.queue.size(), l.sent.value(), l.dropped.value(),
                    l.reconnects.value(), blocked.contains(l.peerId)));
        }
        return out;
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
        final Counter dropped;
        final Counter sent;
        final Counter reconnects;
        final AtomicBoolean drainScheduled = new AtomicBoolean();
        volatile Channel channel;
        long backoffMs = 100;

        Link(int peerId, InetSocketAddress address, int queueSize, MetricsRegistry registry) {
            this.peerId = peerId;
            this.address = address;
            this.queue = new LinkedBlockingQueue<>(queueSize);
            Labels labels = Labels.of("peer", Integer.toString(peerId));
            this.dropped = registry.counter("ember_peer_messages_dropped_total", "Raft messages dropped because the queue was full, the link was cut, or the connection broke.", labels);
            this.sent = registry.counter("ember_peer_messages_sent_total", "Raft messages written to the peer connection.", labels);
            this.reconnects = registry.counter("ember_peer_reconnects_total", "Reconnect attempts after a failed or broken connection.", labels);
            registry.gauge("ember_peer_queue_depth", "Raft messages waiting to be sent to the peer.", labels, queue::size);
            registry.gauge("ember_peer_connected", "1 if the connection to the peer is up.", labels, () -> {
                Channel ch = channel;
                return ch != null && ch.isActive() && !blocked.contains(peerId) ? 1 : 0;      // a link cut by a test hook counts as down
            });
        }

        void send(byte[] frame) {
            if (blocked.contains(peerId) || !queue.offer(frame)) {
                dropped.inc();                                      // full (or "cut"): drop the NEWEST message, Raft copes
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
                sent.inc();
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
                        dropped.add(queue.size());
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
            reconnects.inc();
            try {
                eventLoop.schedule(this::connect, delay, TimeUnit.MILLISECONDS);
            } catch (RuntimeException shuttingDown) {
                LOG.log(System.Logger.Level.DEBUG, "peer {0}: not reconnecting, shutting down", peerId);
            }
        }
    }
}
