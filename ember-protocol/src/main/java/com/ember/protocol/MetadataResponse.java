package com.ember.protocol;

import java.util.List;

public record MetadataResponse(BrokerInfo broker, List<TopicMetadata> topics) implements Response {

    public record BrokerInfo(int nodeId, String host, int port) {
    }

    public record TopicMetadata(String topic, List<PartitionMetadata> partitions) {
    }

    /** leaderNodeId is only on the wire in METADATA v1; v0 responses decode with NO_LEADER. */
    public record PartitionMetadata(int partition, int leaderNodeId, long logStartOffset, long logEndOffset) {

        public static final int NO_LEADER = -1;

        public PartitionMetadata(int partition, long logStartOffset, long logEndOffset) {
            this(partition, NO_LEADER, logStartOffset, logEndOffset);
        }
    }
}
