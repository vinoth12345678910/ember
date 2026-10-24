package com.ember.protocol;

import java.util.List;

public record QueueDescribeResponse(String topic, int leaseMs, int maxAttempts, int backoffBaseMs, int backoffMaxMs,
                                    List<PartitionState> partitions) implements Response {

    /**
     * floor: everything below is finished (a dead-lettered message holds the floor until it is retried or discarded). next: first
     * never-claimed offset. logEnd: end of the partition on the answering broker. inFlight: claimed and not yet settled. redeliver:
     * waiting to be claimed again (including those still backing off). doneAbove: finished offsets at or above the floor.
     * dead: dead-lettered messages.
     */
    public record PartitionState(int partition, long floor, long next, long logEnd, int inFlight, int redeliver, long doneAbove, int dead) {
    }

    public QueueDescribeResponse {
        partitions = List.copyOf(partitions);
    }
}
