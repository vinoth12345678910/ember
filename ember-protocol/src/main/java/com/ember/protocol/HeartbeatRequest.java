package com.ember.protocol;

public record HeartbeatRequest(String groupId, String memberId, int generation) implements Request {
    @Override
    public ApiKey apiKey() {
        return ApiKey.HEARTBEAT;
    }
}
