package com.ember.broker;

import com.ember.protocol.TopicNames;
import com.ember.storage.Log;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * topic -> partitions -> one Log each, stored in {@code <dataDir>/<topic>-<partition>/}.
 * The partition count of a topic is persisted in {@code _meta/<topic>.properties} and never changes.
 */
public class LogManager implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(LogManager.class.getName());

    /** A parsed partition directory name. */
    record PartitionDir(String topic, int partition) {
    }

    private final BrokerConfig config;
    private final TopicMetaStore metaStore;
    private final Map<String, Topic> topics = new ConcurrentHashMap<>();
    private final Object creationLock = new Object();          // creation is rare; reads never take this lock
    private ScheduledExecutorService scheduler;

    public LogManager(BrokerConfig config) throws IOException {
        this.config = config;
        Files.createDirectories(config.dataDir());
        this.metaStore = new TopicMetaStore(config.dataDir());
        try {
            loadExistingTopics();
        } catch (IOException | RuntimeException e) {
            close();                                           // don't leak the logs we already opened
            throw e;
        }
    }

    // ---- startup

    private void loadExistingTopics() throws IOException {
        Map<String, Integer> declared = metaStore.loadAll();
        Map<String, TreeSet<Integer>> onDisk = scanPartitionDirectories();

        TreeSet<String> names = new TreeSet<>(declared.keySet());
        names.addAll(onDisk.keySet());
        for (String name : names) {
            int count;
            if (declared.containsKey(name)) {
                count = declared.get(name);
                for (int p : onDisk.getOrDefault(name, new TreeSet<>())) {
                    if (p >= count) {
                        LOG.log(System.Logger.Level.WARNING, "ignoring directory {0}-{1}: topic has only {2} partitions",
                                name, p, count);
                    }
                }
            } else {
                count = deriveLegacyCount(name, onDisk.get(name));
                metaStore.write(name, count);                  // from now on this topic follows the normal rule
                LOG.log(System.Logger.Level.INFO, "adopted legacy topic {0} with {1} partition(s)", name, count);
            }
            // opens existing partitions and creates any that a crash left missing
            topics.put(name, openTopic(name, count));
        }
    }

    /** Phase 2 layout: no metadata file. The count is max index + 1, but only if nothing is missing. */
    private static int deriveLegacyCount(String topic, TreeSet<Integer> indexes) {
        int count = indexes.last() + 1;
        if (indexes.size() != count) {
            throw new IllegalStateException("topic '" + topic + "' has partition directories " + indexes
                    + " but no metadata file, and the indexes are not contiguous from 0; refusing to start."
                    + " Restore the missing directories or create _meta/" + topic + ".properties by hand.");
        }
        return count;
    }

    private Map<String, TreeSet<Integer>> scanPartitionDirectories() throws IOException {
        Map<String, TreeSet<Integer>> found = new TreeMap<>();
        try (Stream<Path> entries = Files.list(config.dataDir())) {
            for (Path entry : entries.sorted().toList()) {
                String name = entry.getFileName().toString();
                if (!Files.isDirectory(entry) || name.startsWith("_") || name.startsWith(".")) {
                    continue;                                  // _meta, .ember.lock and friends are ours
                }
                PartitionDir parsed = parseDirectoryName(name);
                if (parsed == null) {
                    LOG.log(System.Logger.Level.WARNING, "ignoring directory that is not a partition: {0}", entry);
                    continue;
                }
                found.computeIfAbsent(parsed.topic(), t -> new TreeSet<>()).add(parsed.partition());
            }
        }
        return found;
    }

    /** "my-topic-v2-1" -> (my-topic-v2, 1). Split on the LAST dash; the rest must be digits. Null if not a partition dir. */
    static PartitionDir parseDirectoryName(String directoryName) {
        int dash = directoryName.lastIndexOf('-');
        if (dash <= 0 || dash == directoryName.length() - 1) {
            return null;
        }
        String suffix = directoryName.substring(dash + 1);
        if (suffix.length() > 9 || !suffix.chars().allMatch(c -> c >= '0' && c <= '9')) {
            return null;
        }
        String topic = directoryName.substring(0, dash);
        return TopicNames.isValid(topic) ? new PartitionDir(topic, Integer.parseInt(suffix)) : null;
    }

    static String directoryName(String topic, int partition) {
        return topic + "-" + partition;
    }

    private Topic openTopic(String name, int count) throws IOException {
        List<Log> logs = new ArrayList<>(count);
        try {
            for (int p = 0; p < count; p++) {
                logs.add(new Log(config.dataDir().resolve(directoryName(name, p)), config.segmentBytes(),
                        config.indexIntervalBytes(), config.flushPolicy()));
            }
        } catch (IOException | RuntimeException e) {
            for (Log l : logs) {
                closeQuietly(name, l);
            }
            throw e;
        }
        return new Topic(name, logs);
    }

    // ---- creation

    /**
     * Creates a topic with a fixed number of partitions. The metadata file is written FIRST: if we
     * crash before all directories exist, startup completes them and the count cannot change.
     */
    public Topic createTopic(String name, int partitions) throws IOException {
        if (!TopicNames.isValid(name)) {
            throw new IllegalArgumentException("invalid topic name: " + name);
        }
        if (partitions < 1 || partitions > config.maxPartitionsPerTopic()) {
            throw new IllegalArgumentException("partitions must be in 1.." + config.maxPartitionsPerTopic());
        }
        synchronized (creationLock) {
            if (topics.containsKey(name)) {
                throw new TopicAlreadyExistsException(name);
            }
            return create(name, partitions);
        }
    }

    /** Caller holds creationLock and has checked that the topic does not exist. */
    private Topic create(String name, int partitions) throws IOException {
        metaStore.write(name, partitions);
        Topic topic;
        try {
            topic = openTopic(name, partitions);
        } catch (IOException | RuntimeException e) {
            metaStore.delete(name);                            // nobody saw this topic, so leave no trace
            throw e;
        }
        topics.put(name, topic);                               // published only when complete
        return topic;
    }

    /**
     * The log to append to, creating the topic if it is missing and auto-creation applies.
     * Returns null when the topic/partition does not exist and may not be created.
     */
    public Log getOrAutoCreate(String name, int partition) throws IOException {
        Topic topic = topics.get(name);
        if (topic == null) {
            if (!config.autoCreateTopics() || partition < 0 || partition >= config.defaultPartitions()) {
                return null;
            }
            synchronized (creationLock) {
                topic = topics.get(name);                      // an explicit create may have won the race
                if (topic == null) {
                    topic = create(name, config.defaultPartitions());
                }
            }
        }
        return partitionOf(topic, partition);
    }

    // ---- lookup

    public Topic topic(String name) {
        return topics.get(name);
    }

    /** The log of this partition, or null if the topic or partition does not exist. */
    public Log logFor(String name, int partition) {
        Topic topic = topics.get(name);
        return topic == null ? null : partitionOf(topic, partition);
    }

    private static Log partitionOf(Topic topic, int partition) {
        return partition < 0 || partition >= topic.partitionCount() ? null : topic.partitions().get(partition);
    }

    /** A snapshot sorted by topic name. */
    public List<Topic> topics() {
        List<Topic> all = new ArrayList<>(topics.values());
        all.sort((a, b) -> a.name().compareTo(b.name()));
        return all;
    }

    // ---- maintenance and shutdown

    /** fsync per the flush policy, then retention, on EVERY partition of every topic. One bad log doesn't stop the rest. */
    public void runMaintenance(long nowMs) {
        for (Topic topic : topics.values()) {
            for (int p = 0; p < topic.partitionCount(); p++) {
                Log log = topic.partitions().get(p);
                try {
                    log.maybeFlush(nowMs);
                    log.deleteOldSegments(nowMs, config.retentionMs(), config.retentionBytes());
                } catch (IOException | RuntimeException ex) {
                    LOG.log(System.Logger.Level.ERROR, "maintenance failed for " + topic.name() + "-" + p, ex);
                }
            }
        }
    }

    /** Starts the background timer that calls runMaintenance(clock.millis()). Daemon: never keeps the JVM alive. */
    public synchronized void startMaintenance(Clock clock) {
        if (scheduler != null) {
            return;
        }
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "ember-maintenance");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleWithFixedDelay(() -> runMaintenance(clock.millis()),
                config.maintenanceIntervalMs(), config.maintenanceIntervalMs(), TimeUnit.MILLISECONDS);
    }

    /** Stops the timer, then closes every log (which fsyncs unflushed data). Safe to call twice. */
    @Override
    public void close() {
        ScheduledExecutorService s;
        synchronized (this) {
            s = scheduler;
            scheduler = null;
        }
        if (s != null) {
            s.shutdown();
            try {
                s.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        for (Topic topic : topics.values()) {
            for (Log log : topic.partitions()) {
                closeQuietly(topic.name(), log);
            }
        }
    }

    private static void closeQuietly(String topic, Log log) {
        try {
            log.close();
        } catch (IOException ex) {
            LOG.log(System.Logger.Level.ERROR, "closing a partition of " + topic + " failed", ex);
        }
    }
}
