package com.ember.protocol;

public record LeaveGroupRequest(String groupId, String memberId) implements Request {
    @Override
    public ApiKey apiKey() {
        return ApiKey.LEAVE_GROUP;
    }
}
