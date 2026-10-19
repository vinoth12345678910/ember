package com.ember.protocol;

public record OffsetCommitRequest(String groupId, String memberId, int generation, java.util.List<PartitionOffset> offsets) implements Request {
    @Override
    public ApiKey apiKey() {
        return ApiKey.OFFSET_COMMIT;
    }
}
