package com.ember.protocol;

import java.util.List;

/**
 * METADATA version 2: the whole cluster as seen by the answering broker. Offsets are that broker's
 * applied (committed) view; a follower may be slightly behind the leader.
 */
public record ClusterMetadataResponse(List<Broker> brokers, int controllerId, List<Topic> topics) implements Response {

    public record Broker(int nodeId, String host, int port) {
    }

    public record Topic(String name, List<Partition> partitions) {
    }

    /** leaderId is -1 when unknown. */
    public record Partition(int partition, int leaderId, List<Integer> replicas, long logStartOffset, long logEndOffset) {
    }
}
