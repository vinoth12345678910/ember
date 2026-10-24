package com.ember.broker;

import com.ember.metrics.PrometheusFormat;
import com.ember.replication.GroupRuntime;
import com.ember.replication.GroupStateMachine;
import com.ember.storage.Log;
import com.ember.storage.LogRecord;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.CRC32;

/**
 * The admin HTTP endpoint (JDK built-in server, its own small thread pool so a slow scraper cannot slow the data path).
 *
 *   GET /metrics        Prometheus text
 *   GET /health/live    200 as soon as this server runs
 *   GET /health/ready   200 only when joined, leaders known, and every local group has applied what it knows is committed
 *   GET /status         JSON overview
 *   GET /debug/partition-checksum?topic=&partition=   CRC32 of a data log (ONLY with --enable-debug-endpoints)
 *
 * UNAUTHENTICATED plain HTTP. It never returns record keys or values, only metadata and counters.
 */
final class AdminServer implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(AdminServer.class.getName());

    private final BrokerServer broker;
    private final BrokerConfig config;
    private final HttpServer http;
    private final ExecutorService pool;

    AdminServer(BrokerServer broker, BrokerConfig config, int port) throws IOException {
        this.broker = broker;
        this.config = config;
        this.http = HttpServer.create(new InetSocketAddress(config.adminHost(), port), 16);
        this.pool = Executors.newFixedThreadPool(2, r -> {
            Thread t = new Thread(r, "ember-admin");
            t.setDaemon(true);
            return t;
        });
        http.setExecutor(pool);
        http.createContext("/metrics", ex -> handle(ex, this::metrics));
        http.createContext("/health/live", ex -> handle(ex, e -> reply(e, 200, "text/plain", "live\n")));
        http.createContext("/health/ready", ex -> handle(ex, this::ready));
        http.createContext("/status", ex -> handle(ex, this::status));
        if (config.enableDebugEndpoints()) {
            http.createContext("/debug/partition-checksum", ex -> handle(ex, this::checksum));
        }
        http.start();
        LOG.log(System.Logger.Level.WARNING,
                "broker {0}: admin HTTP on {1}:{2} is UNAUTHENTICATED (metrics, health, status{3}); keep it on a trusted network",
                config.nodeId(), config.adminHost(), port(), config.enableDebugEndpoints() ? ", DEBUG endpoints ENABLED" : "");
    }

    int port() {
        return http.getAddress().getPort();
    }

    private interface Handler {
        void handle(HttpExchange exchange) throws IOException;
    }

    private void handle(HttpExchange exchange, Handler handler) throws IOException {
        try {
            if (!exchange.getRequestMethod().equals("GET")) {
                reply(exchange, 405, "text/plain", "GET only\n");
                return;
            }
            handler.handle(exchange);
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "admin request " + exchange.getRequestURI() + " failed", e);
            reply(exchange, 500, "text/plain", "error: " + e + "\n");
        } finally {
            exchange.close();
        }
    }

    private static void reply(HttpExchange exchange, int status, String contentType, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", contentType + "; charset=utf-8");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    // ------------------------------------------------------------------ endpoints

    private void metrics(HttpExchange exchange) throws IOException {
        ReplicaManager replicas = broker.replicaManager();
        if (replicas != null) {
            broker.metrics().refreshGroupMetrics(replicas);      // groups appear at runtime, so their gauges are registered on demand
        }
        reply(exchange, 200, "text/plain; version=0.0.4", PrometheusFormat.render(broker.registry()));
    }

    private void ready(HttpExchange exchange) throws IOException {
        String reason = broker.notReadyReason();
        JsonWriter w = new JsonWriter().beginObject().name("ready").value(reason == null);
        if (reason != null) {
            w.name("reason").value(reason);
        }
        reply(exchange, reason == null ? 200 : 503, "application/json", w.endObject().toString() + "\n");
    }

    private void status(HttpExchange exchange) throws IOException {
        ReplicaManager replicas = broker.replicaManager();
        JsonWriter w = new JsonWriter().beginObject();
        w.name("nodeId").value(config.nodeId());
        w.name("uptimeSeconds").value(broker.uptimeMillis() / 1000.0);
        w.name("ready").value(broker.notReadyReason() == null);
        w.name("port").value(broker.port());
        w.name("adminDebugEndpoints").value(config.enableDebugEndpoints());
        if (replicas == null) {
            w.name("joined").value(false);
            reply(exchange, 200, "application/json", w.endObject().toString() + "\n");
            return;
        }
        w.name("joined").value(true);
        w.name("cluster").beginArray();
        replicas.addresses().forEach((id, addr) -> {
            java.net.InetSocketAddress adv = replicas.advertisedAddresses().getOrDefault(id, addr);
            w.beginObject().name("nodeId").value(id).name("host").value(addr.getHostString()).name("port").value(addr.getPort())
                    .name("advertisedHost").value(adv.getHostString()).name("advertisedPort").value(adv.getPort()).endObject();
        });
        w.endArray();
        w.name("controllerId").value(replicas.controllerId());
        w.name("coordinatorId").value(replicas.coordinatorId());

        w.name("raftGroups").beginArray();
        raftGroup(w, replicas.metaGroup());
        raftGroup(w, replicas.groupsGroup());
        raftGroup(w, replicas.acksGroup());
        for (ReplicaManager.TopicState t : replicas.topics()) {
            for (ReplicaManager.PartitionReplica r : t.replicas()) raftGroup(w, r.group());
        }
        w.endArray();

        w.name("topics").beginArray();
        for (ReplicaManager.TopicState t : replicas.topics()) {
            w.beginObject().name("name").value(t.name()).name("partitions").beginArray();
            for (ReplicaManager.PartitionReplica r : t.replicas()) {
                int leader = r.group().isLeader() ? config.nodeId() : r.group().leaderId();
                w.beginObject().name("partition").value(r.partition()).name("leader").value(leader)
                        .name("logStartOffset").value(r.dataLog().startOffset()).name("logEndOffset").value(r.dataLog().nextOffset()).endObject();
            }
            w.endArray().endObject();
        }
        w.endArray();

        w.name("consumerGroups").beginArray();
        GroupStateMachine groups = replicas.groupState();
        for (String id : groups.groupIds()) {
            GroupStateMachine.GroupData g = groups.group(id);
            if (g == null) continue;
            w.beginObject().name("group").value(id).name("generation").value(g.generation()).name("members").beginArray();
            g.members().forEach((memberId, m) -> w.beginObject().name("memberId").value(memberId).name("clientId").value(m.clientId())
                    .name("assignedPartitions").value(m.assigned().size()).endObject());
            w.endArray().name("committedPartitions").value(g.offsets().size()).endObject();
        }
        w.endArray();

        w.name("queueLeaderId").value(replicas.ackLeaderId());
        w.name("subscriptions").beginArray();
        for (String name : replicas.ackState().names()) {
            com.ember.replication.AckStateMachine.SubscriptionView v = replicas.ackState().view(name);
            if (v == null) continue;
            w.beginObject().name("subscription").value(name).name("topic").value(v.topic()).name("leaseMs").value(v.leaseMs()).name("partitions").beginArray();
            for (com.ember.replication.AckStateMachine.PartitionView p : v.partitions()) {
                long logEnd = replicas.topic(v.topic()) == null ? 0 : replicas.topic(v.topic()).replicas().get(p.partition()).dataLog().nextOffset();
                w.beginObject().name("partition").value(p.partition()).name("floor").value(p.floor()).name("next").value(p.next()).name("logEnd").value(logEnd)
                        .name("inFlight").value(p.inFlight()).name("redeliver").value(p.redeliver()).name("doneAbove").value(p.doneAbove()).endObject();
            }
            w.endArray().endObject();
        }
        w.endArray();

        w.name("peers").beginArray();
        for (PeerNetwork.PeerStatus p : broker.peerStatus()) {
            w.beginObject().name("peer").value(p.peerId()).name("connected").value(p.connected()).name("queueDepth").value(p.queueDepth())
                    .name("sent").value(p.sent()).name("dropped").value(p.dropped()).name("reconnects").value(p.reconnects())
                    .name("blockedByTestHook").value(p.blocked()).endObject();
        }
        w.endArray();

        w.name("config").beginObject()
                .name("raftElectionMinMs").value(config.raftElectionMinMs()).name("raftElectionMaxMs").value(config.raftElectionMaxMs())
                .name("raftHeartbeatMs").value(config.raftHeartbeatMs()).name("produceTimeoutMs").value(config.produceTimeoutMs())
                .name("unsafeNoRaftFsync").value(config.unsafeNoRaftFsync()).name("groupMinSessionMs").value(config.groupMinSessionMs())
                .name("groupMaxSessionMs").value(config.groupMaxSessionMs()).name("defaultPartitions").value(config.defaultPartitions())
                .endObject();
        reply(exchange, 200, "application/json", w.endObject().toString() + "\n");
    }

    private static void raftGroup(JsonWriter w, GroupRuntime g) {
        w.beginObject().name("group").value(g.name().contains("@") ? g.name().substring(0, g.name().indexOf('@')) : g.name()).name("role").value(g.role().name()).name("term").value(g.term())
                .name("leader").value(g.leaderId()).name("commitIndex").value(g.commitIndex()).name("lastApplied").value(g.lastApplied())
                .name("lastIndex").value(g.lastLogIndex()).name("pendingProposals").value(g.pendingCount()).endObject();
    }

    /** Debug aid: CRC32 over (offset, timestamp, key, value) of every record from the log start to the end seen at the start. */
    private void checksum(HttpExchange exchange) throws IOException {
        Map<String, String> query = new HashMap<>();
        String raw = exchange.getRequestURI().getRawQuery();
        if (raw != null) {
            for (String pair : raw.split("&")) {
                int eq = pair.indexOf('=');
                if (eq > 0) query.put(pair.substring(0, eq), java.net.URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8));
            }
        }
        ReplicaManager replicas = broker.replicaManager();
        String topic = query.get("topic");
        if (replicas == null || topic == null || query.get("partition") == null) {
            reply(exchange, 400, "text/plain", "need ?topic=&partition=\n");
            return;
        }
        Log log = replicas.logFor(topic, Integer.parseInt(query.get("partition")));
        if (log == null) {
            reply(exchange, 404, "text/plain", "unknown partition\n");
            return;
        }
        long start = log.startOffset();
        long end = log.nextOffset();
        CRC32 crc = new CRC32();
        ByteBuffer scratch = ByteBuffer.allocate(16);
        for (long offset = start; offset < end; offset++) {
            LogRecord r = log.read(offset);
            scratch.clear();
            scratch.putLong(r.offset()).putLong(r.timestamp());
            crc.update(scratch.array(), 0, 16);
            crc.update(r.key() == null ? new byte[]{(byte) 0xFF} : r.key());
            crc.update(r.value());
        }
        String json = new JsonWriter().beginObject().name("topic").value(topic).name("partition").value(Long.parseLong(query.get("partition")))
                .name("startOffset").value(start).name("endOffset").value(end).name("crc32").value(crc.getValue()).endObject().toString();
        reply(exchange, 200, "application/json", json + "\n");
    }

    @Override
    public void close() {
        http.stop(0);
        pool.shutdownNow();
    }
}
