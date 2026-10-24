package com.ember.protocol;

import java.util.List;

public record QueueDescribeResponse(String topic, int leaseMs, List<PartitionState> partitions) implements Response {

    /**
     * floor: everything below is finished. next: first never-claimed offset. logEnd: end of the partition on the answering broker.
     * inFlight: claimed and not yet settled. redeliver: waiting to be claimed again. doneAbove: finished offsets at or above the floor.
     */
    public record PartitionState(int partition, long floor, long next, long logEnd, int inFlight, int redeliver, long doneAbove) {
    }

    public QueueDescribeResponse {
        partitions = List.copyOf(partitions);
    }
}
