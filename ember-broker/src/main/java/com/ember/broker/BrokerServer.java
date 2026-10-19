package com.ember.broker;

import com.ember.protocol.ApiKey;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.GlobalEventExecutor;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Clock;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * One broker. Two-phase start so that tests can use ephemeral ports:
 *   bind()            opens the listening port (and takes the data directory lock); returns the real port;
 *   start(addresses)  joins the cluster: builds the replicas, the peer network and the request logic.
 * start() does both for fixed ports (and for single-broker mode).
 */
public class BrokerServer implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(BrokerServer.class.getName());
    /** Raft frames carry produce batches, so peers may exceed the client frame limit by this much. */
    private static final int PEER_FRAME_SLACK = 1024 * 1024;

    private final BrokerConfig config;
    private final Clock clock;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicInteger boundPort = new AtomicInteger();
    private final ChannelGroup connections = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

    private DataDirLock dataDirLock;
    private EventLoopGroup boss;
    private EventLoopGroup workers;
    private EventExecutorGroup handlers;
    private Channel serverChannel;

    private volatile ReplicaManager replicas;
    private volatile BrokerApis apis;
    private volatile PeerNetwork peers;

    public BrokerServer(BrokerConfig config, Clock clock) {
        this.config = config;
        this.clock = clock;
        this.boundPort.set(config.port());
    }

    // ------------------------------------------------------------------ phase 1: bind

    /** Opens the port (taking the data directory lock first). Returns the port actually bound. */
    public int bind() throws IOException, InterruptedException {
        // Lock first: a broker that cannot own the directory must not touch anything in it.
        dataDirLock = DataDirLock.acquire(config.dataDir());
        try {
            boss = new NioEventLoopGroup(1);
            workers = new NioEventLoopGroup();
            handlers = new DefaultEventExecutorGroup(config.handlerThreads());
            ServerBootstrap bootstrap = new ServerBootstrap()
                    .group(boss, workers)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .option(ChannelOption.SO_REUSEADDR, true)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            connections.add(ch);
                            ch.pipeline()
                                    .addLast(new IdleStateHandler(config.idleTimeoutMs(), 0, 0, TimeUnit.MILLISECONDS))
                                    .addLast(new FrameDecoder(config.maxFrameBytes()))
                                    .addLast(new PeerMessageHandler(BrokerServer.this::onRaftMessage))   // on the I/O loop: see its comment
                                    .addLast(handlers, new RequestHandler(() -> apis, BrokerServer.this::onRaftMessage));
                        }
                    });
            serverChannel = bootstrap.bind(config.port()).sync().channel();
            boundPort.set(((InetSocketAddress) serverChannel.localAddress()).getPort());
            return boundPort.get();
        } catch (Throwable t) {
            shutdown(true);
            throw t;
        }
    }

    // ------------------------------------------------------------------ phase 2: join the cluster

    /** addresses: every broker of the cluster, this one included (nodeId -> address). */
    public void start(Map<Integer, InetSocketAddress> addresses) throws IOException {
        if (serverChannel == null) {
            throw new IllegalStateException("bind() first");
        }
        try {
            Map<Integer, InetSocketAddress> cluster = new TreeMap<>(addresses);
            if (!cluster.containsKey(config.nodeId())) {
                throw new IllegalArgumentException("the cluster addresses do not contain this broker (node " + config.nodeId() + ")");
            }
            PeerNetwork network = new PeerNetwork(config.nodeId(), cluster, config.peerQueueSize());
            ReplicaManager manager = new ReplicaManager(config, cluster, network);
            this.peers = network;
            this.replicas = manager;
            manager.startMaintenance(clock);
            network.start();
            this.apis = new BrokerApis(config, manager, clock, boundPort::get);
            if (cluster.size() == 1) {
                manager.awaitMetaLeaderReady(10_000);               // a lone broker elects itself: be ready when start() returns
            }
        } catch (Throwable t) {
            shutdown(true);
            throw t;
        }
    }

    /** bind() + start(): fixed ports and single-broker mode. */
    public int start() throws IOException, InterruptedException {
        int port = bind();
        Map<Integer, InetSocketAddress> cluster = new TreeMap<>();
        if (config.cluster().isEmpty()) {
            cluster.put(config.nodeId(), new InetSocketAddress(config.host(), port));
        } else {
            for (BrokerConfig.ClusterMember m : config.cluster()) {
                cluster.put(m.nodeId(), new InetSocketAddress(m.host(), m.port()));
            }
        }
        start(cluster);
        return port;
    }

    public int port() {
        return boundPort.get();
    }

    /** The replicas (and, through them, the data logs). Named logManager() since Phase 2 tests use it. */
    public ReplicaManager logManager() {
        return replicas;
    }

    public ReplicaManager replicaManager() {
        return replicas;
    }

    // ------------------------------------------------------------------ peer traffic and test hooks

    private void onRaftMessage(com.ember.protocol.RaftMessageRequest r) {
        ReplicaManager manager = replicas;
        PeerNetwork network = peers;
        if (manager == null || network == null || network.isBlocked(r.fromNodeId())) {
            return;                                                  // not started yet, or the link is "cut" by a test
        }
        manager.deliver(r.groupTopic(), r.groupPartition(), r.fromNodeId(), r.message());
    }

    /** Test hook: stop sending to and accepting Raft traffic from this peer (a network partition at socket level). */
    public void blockPeer(int peerId) {
        if (peers != null) peers.block(peerId);
    }

    public void unblockPeer(int peerId) {
        if (peers != null) peers.unblock(peerId);
    }

    /** Test hook: cut this broker off from every peer (clients can still reach it). */
    public void isolate() {
        if (peers != null) peers.blockAll();
    }

    public void heal() {
        if (peers != null) peers.unblockAll();
    }

    public long droppedPeerMessages() {
        return peers == null ? 0 : peers.droppedMessages();
    }

    // ------------------------------------------------------------------ shutdown

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            shutdown(true);
        }
    }

    /** Test hook: die abruptly. Raft groups are aborted, nothing is flushed or announced; the lock is released. */
    public void crash() {
        if (closed.compareAndSet(false, true)) {
            shutdown(false);
        }
    }

    private void shutdown(boolean graceful) {
        if (serverChannel != null) {
            serverChannel.close().awaitUninterruptibly(2, TimeUnit.SECONDS);       // 1. stop accepting
        }
        connections.close().awaitUninterruptibly(3, TimeUnit.SECONDS);             // 2. hang up on clients
        if (workers != null) {
            workers.shutdownGracefully(graceful ? 50 : 0, 2000, TimeUnit.MILLISECONDS).awaitUninterruptibly(5, TimeUnit.SECONDS);
        }
        if (handlers != null) {                                                    // 3. let in-flight requests end
            handlers.shutdownGracefully(0, 3, TimeUnit.SECONDS).awaitUninterruptibly(5, TimeUnit.SECONDS);
        }
        if (boss != null) {
            boss.shutdownGracefully(0, 2, TimeUnit.SECONDS).awaitUninterruptibly(5, TimeUnit.SECONDS);
        }
        apis = null;
        if (peers != null) {                                                       // 4. stop talking to the other brokers
            peers.close();
        }
        if (replicas != null) {                                                    // 5. groups, then logs
            if (graceful) replicas.close(); else replicas.crash();
        }
        if (dataDirLock != null) {                                                 // 6. last: nobody may use the directory before
            dataDirLock.close();
        }
        LOG.log(System.Logger.Level.INFO, "broker {0} stopped{1}", config.nodeId(), graceful ? "" : " (crash)");
    }

    /**
     * Frames: LENGTH(4) apiKey(2) ... Client requests are limited to maxFrameBytes; RAFT_MESSAGE frames from peers may be
     * 1 MiB bigger. A too-long client frame is detected from its first 6 bytes, before anything is buffered.
     */
    private static final class FrameDecoder extends LengthFieldBasedFrameDecoder {
        private final int maxClientFrame;

        FrameDecoder(int maxClientFrame) {
            super(maxClientFrame + PEER_FRAME_SLACK + 4, 0, 4, 0, 4);
            this.maxClientFrame = maxClientFrame;
        }

        @Override
        protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
            if (in.readableBytes() >= 6) {
                int length = in.getInt(in.readerIndex());
                short apiKey = in.getShort(in.readerIndex() + 4);
                if (length > maxClientFrame && apiKey != ApiKey.RAFT_MESSAGE.code()) {
                    in.skipBytes(in.readableBytes());
                    throw new TooLongFrameException("client frame of " + length + " bytes exceeds maxFrameBytes=" + maxClientFrame);
                }
            }
            return super.decode(ctx, in);
        }
    }
}
