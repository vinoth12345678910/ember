package com.ember.protocol;

public record JoinGroupRequest(String groupId, String memberId, String clientId, int sessionTimeoutMs, java.util.List<String> topics) implements Request {
    @Override
    public ApiKey apiKey() {
        return ApiKey.JOIN_GROUP;
    }
}
