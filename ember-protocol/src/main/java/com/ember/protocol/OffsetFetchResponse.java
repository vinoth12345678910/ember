package com.ember.protocol;

public record OffsetFetchResponse(java.util.List<PartitionOffset> offsets) implements Response {
}
