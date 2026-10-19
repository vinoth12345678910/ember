package com.ember.replication;

import com.ember.protocol.TopicNames;
import com.ember.raft.EntryType;
import com.ember.raft.LogEntry;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The state machine of the META group: the set of topics. It is applied on EVERY broker, so the decision
 * "created or already exists" is made deterministically at apply time, identically everywhere.
 */
public final class MetadataStateMachine implements StateMachine {

    public enum CreateResult {
        CREATED, EXISTS, INVALID
    }

    /** Called (on the META group's thread) for a newly created topic, also during replay at startup. */
    public interface TopicListener {
        void topicCreated(String name, int partitionCount);
    }

    private final Map<String, Integer> topics = new ConcurrentHashMap<>();
    private final TopicListener listener;
    private final int maxPartitions;

    public MetadataStateMachine(TopicListener listener, int maxPartitions) {
        this.listener = listener;
        this.maxPartitions = maxPartitions;
    }

    @Override
    public Object apply(long index, LogEntry entry) {
        if (entry.type() == EntryType.NOOP) {
            return null;
        }
        byte[] command = entry.command();
        if (Commands.type(command) != Commands.CREATE_TOPIC) {
            throw new IllegalStateException("unknown META command type " + Commands.type(command));
        }
        Commands.CreateTopic create = Commands.decodeCreateTopic(command);
        if (!TopicNames.isValid(create.name()) || create.name().startsWith("__")
                || create.partitionCount() < 1 || create.partitionCount() > maxPartitions) {
            return CreateResult.INVALID;
        }
        if (topics.putIfAbsent(create.name(), create.partitionCount()) != null) {
            return CreateResult.EXISTS;
        }
        listener.topicCreated(create.name(), create.partitionCount());
        return CreateResult.CREATED;
    }

    /** name -> partition count, sorted by name. */
    public Map<String, Integer> topics() {
        return new TreeMap<>(topics);
    }

    public Integer partitionCount(String name) {
        return topics.get(name);
    }
}
