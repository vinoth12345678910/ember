package com.ember.protocol;

public record DescribeGroupRequest(String groupId) implements Request {
    @Override
    public ApiKey apiKey() {
        return ApiKey.DESCRIBE_GROUP;
    }
}
