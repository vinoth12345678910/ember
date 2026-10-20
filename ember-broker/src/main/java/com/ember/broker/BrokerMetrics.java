package com.ember.broker;

import com.ember.metrics.Counter;
import com.ember.metrics.Histogram;
import com.ember.metrics.Labels;
import com.ember.metrics.MetricsRegistry;
import com.ember.protocol.ApiKey;
import com.ember.protocol.ErrorCode;
import com.ember.replication.GroupCoordinator;
import com.ember.replication.GroupStateMachine;
import com.ember.protocol.PartitionRef;

import java.lang.management.BufferPoolMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntSupplier;

/**
 * Every metric the broker itself owns (requests, produce, fetch, JVM). Raft, peer, storage and group metrics are
 * registered by the classes that own those things, into the same registry.
 */
final class BrokerMetrics {

    final MetricsRegistry registry;
    private final Map<String, Counter> counters = new ConcurrentHashMap<>();   // hot path: avoid the registry lock
    private final Histogram produceLatency;
    private final Counter produceRecords;
    private final Counter produceBytes;
    private final Counter fetchBytes;
    private final Counter overloaded;

    BrokerMetrics(MetricsRegistry registry, IntSupplier connections) {
        this.registry = registry;
        this.produceLatency = registry.latencyHistogram("ember_produce_latency_seconds",
                "Time from a PRODUCE request arriving to its response being written (includes the Raft commit).", Labels.NONE);
        this.produceRecords = registry.counter("ember_produce_records_total", "Records in PRODUCE requests that were acknowledged.", Labels.NONE);
        this.produceBytes = registry.counter("ember_produce_bytes_total", "Key plus value bytes of acknowledged PRODUCE requests.", Labels.NONE);
        this.fetchBytes = registry.counter("ember_fetch_bytes_total", "Record bytes sent in FETCH responses.", Labels.NONE);
        this.overloaded = registry.counter("ember_overloaded_total", "Requests refused with BROKER_OVERLOADED.", Labels.NONE);
        registry.gauge("ember_connections", "Open client and peer connections to this broker.", Labels.NONE, connections::getAsInt);
        registerJvm();
        // Series that only appear after their first event break rate() queries and dashboards: create the common ones up front.
        for (ApiKey api : ApiKey.values()) {
            if (api != ApiKey.RAFT_MESSAGE) {
                counter("ember_requests_total", "Requests by API and error code.", "api", lower(api), "error", "none");
            }
        }
        counter("ember_produce_requests_total", "PRODUCE requests by result.", "result", "ok");
        counter("ember_fetch_requests_total", "FETCH requests by result.", "result", "ok");
        counter("ember_group_offset_commits_total", "OFFSET_COMMIT requests by result.", "result", "ok");
    }

    private Counter counter(String name, String help, String... labels) {
        StringBuilder key = new StringBuilder(name);
        for (String l : labels) key.append('|').append(l);
        return counters.computeIfAbsent(key.toString(), k -> registry.counter(name, help, Labels.of(labels)));
    }

    private static String lower(Enum<?> e) {
        return e.name().toLowerCase(Locale.ROOT);
    }

    void request(ApiKey api, ErrorCode error) {
        counter("ember_requests_total", "Requests by API and error code.", "api", lower(api), "error", lower(error)).inc();
        if (error == ErrorCode.BROKER_OVERLOADED) {
            overloaded.inc();
        }
    }

    void produce(ErrorCode error, int records, long bytes, long startNanos) {
        request(ApiKey.PRODUCE, error);
        counter("ember_produce_requests_total", "PRODUCE requests by result.", "result", error == ErrorCode.NONE ? "ok" : lower(error)).inc();
        produceLatency.record((System.nanoTime() - startNanos) / 1_000);
        if (error == ErrorCode.NONE) {
            produceRecords.add(records);
            produceBytes.add(bytes);
        }
    }

    void fetch(ErrorCode error, long bytes) {
        request(ApiKey.FETCH, error);
        counter("ember_fetch_requests_total", "FETCH requests by result.", "result", error == ErrorCode.NONE ? "ok" : lower(error)).inc();
        if (bytes > 0) {
            fetchBytes.add(bytes);
        }
    }

    void offsetCommit(ErrorCode error) {
        counter("ember_group_offset_commits_total", "OFFSET_COMMIT requests by result.", "result", error == ErrorCode.NONE ? "ok" : lower(error)).inc();
    }

    // ------------------------------------------------------------------ consumer groups (registered lazily: groups appear at runtime)

    /**
     * Called before every scrape. Registers (idempotently) the gauges of every group the replicated state knows about.
     * CARDINALITY WARNING: group_lag has one child per (group, topic, partition) that has a committed offset.
     */
    void refreshGroupMetrics(ReplicaManager replicas) {
        GroupStateMachine state = replicas.groupState();
        GroupCoordinator coordinator = replicas.coordinator();
        for (String group : state.groupIds()) {
            registry.gauge("ember_group_members", "Members in the group (as seen by this broker's replicated state).", Labels.of("group", group),
                    () -> {
                        GroupStateMachine.GroupData g = state.group(group);
                        return g == null ? Double.NaN : g.members().size();
                    });
            registry.gauge("ember_group_generation", "Current generation (seating-chart version) of the group.", Labels.of("group", group),
                    () -> {
                        GroupStateMachine.GroupData g = state.group(group);
                        return g == null ? Double.NaN : g.generation();
                    });
            registry.counterFromSupplier("ember_group_rebalances_total",
                    "Rebalances caused by this broker while it was the coordinator (restarts from 0 after a coordinator failover).",
                    Labels.of("group", group), () -> coordinator.rebalancesOf(group));
            GroupStateMachine.GroupData data = state.group(group);
            if (data == null) {
                continue;
            }
            for (PartitionRef p : data.offsets().keySet()) {
                registry.gauge("ember_group_lag", "Log end offset minus committed offset; only the coordinator leader reports it.",
                        Labels.of("group", group, "topic", p.topic(), "partition", Integer.toString(p.partition())), () -> {
                            if (!replicas.groupsGroup().isLeader()) {
                                return Double.NaN;
                            }
                            com.ember.storage.Log log = replicas.logFor(p.topic(), p.partition());
                            long committed = state.committedOffset(group, p);
                            return log == null || committed < 0 ? Double.NaN : Math.max(0, log.nextOffset() - committed);
                        });
            }
        }
    }

    // ------------------------------------------------------------------ JVM and process

    private void registerJvm() {
        var memory = ManagementFactory.getMemoryMXBean();
        var runtime = ManagementFactory.getRuntimeMXBean();
        var threads = ManagementFactory.getThreadMXBean();
        registry.gauge("ember_jvm_heap_used_bytes", "Heap in use.", Labels.NONE, () -> memory.getHeapMemoryUsage().getUsed());
        registry.gauge("ember_jvm_heap_max_bytes", "Maximum heap.", Labels.NONE, () -> memory.getHeapMemoryUsage().getMax());
        registry.gauge("ember_jvm_heap_after_gc_bytes", "Heap pools' usage right after their last garbage collection: the live set. Heap 'used' is a sawtooth; only this one shows a leak.", Labels.NONE, () -> {
            long total = 0;
            boolean any = false;
            for (java.lang.management.MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
                if (pool.getType() == java.lang.management.MemoryType.HEAP && pool.getCollectionUsage() != null) {
                    total += pool.getCollectionUsage().getUsed();
                    any = true;
                }
            }
            return any ? total : Double.NaN;
        });
        registry.gauge("ember_jvm_threads", "Live threads.", Labels.NONE, threads::getThreadCount);
        registry.gauge("ember_process_uptime_seconds", "Seconds since the JVM started.", Labels.NONE, () -> runtime.getUptime() / 1000.0);
        registry.counterFromSupplier("ember_jvm_gc_collections_total", "Garbage collections, all collectors.", Labels.NONE, () -> {
            long n = 0;
            for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) n += Math.max(0, gc.getCollectionCount());
            return n;
        });
        registry.gauge("ember_jvm_gc_collection_seconds_total", "Time spent in garbage collection, all collectors.", Labels.NONE, () -> {
            long ms = 0;
            for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) ms += Math.max(0, gc.getCollectionTime());
            return ms / 1000.0;
        });
        registry.gauge("ember_jvm_direct_buffer_used_bytes", "Direct (off-heap) buffer memory in use.", Labels.NONE, () -> {
            List<BufferPoolMXBean> pools = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
            for (BufferPoolMXBean pool : pools) {
                if (pool.getName().equals("direct")) return pool.getMemoryUsed();
            }
            return Double.NaN;
        });
        registry.gauge("ember_process_open_fds", "Open file descriptors (Unix only).", Labels.NONE, () -> {
            if (ManagementFactory.getOperatingSystemMXBean() instanceof com.sun.management.UnixOperatingSystemMXBean unix) {
                return unix.getOpenFileDescriptorCount();
            }
            return Double.NaN;
        });
    }
}
