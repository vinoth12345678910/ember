package com.ember.protocol;

public record OffsetFetchRequest(String groupId, java.util.List<PartitionRef> partitions) implements Request {
    @Override
    public ApiKey apiKey() {
        return ApiKey.OFFSET_FETCH;
    }
}
