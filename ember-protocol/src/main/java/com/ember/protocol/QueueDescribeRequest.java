package com.ember.protocol;

public record QueueDescribeRequest(String subscription) implements Request {
    @Override
    public ApiKey apiKey() {
        return ApiKey.QUEUE_DESCRIBE;
    }
}
