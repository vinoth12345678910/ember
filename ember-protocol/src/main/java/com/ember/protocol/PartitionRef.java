package com.ember.protocol;

/** A (topic, partition) pair used by the group APIs. Ordered by topic, then partition (stable, deterministic maps). */
public record PartitionRef(String topic, int partition) implements Comparable<PartitionRef> {

    @Override
    public int compareTo(PartitionRef other) {
        int byTopic = topic.compareTo(other.topic);
        return byTopic != 0 ? byTopic : Integer.compare(partition, other.partition);
    }
}
