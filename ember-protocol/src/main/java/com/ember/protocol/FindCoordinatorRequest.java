package com.ember.protocol;

public record FindCoordinatorRequest(String groupId) implements Request {
    @Override
    public ApiKey apiKey() {
        return ApiKey.FIND_COORDINATOR;
    }
}
