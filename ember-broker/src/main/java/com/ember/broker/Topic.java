package com.ember.broker;

import com.ember.storage.Log;

import java.util.List;

/** A topic and its partitions. The list is immutable: the partition count never changes in Phase 3. */
public record Topic(String name, List<Log> partitions) {

    public Topic {
        partitions = List.copyOf(partitions);
    }

    public int partitionCount() {
        return partitions.size();
    }
}
