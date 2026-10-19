package com.ember.protocol;

public record CreateTopicRequest(String topic, int partitionCount) implements Request {
    @Override
    public ApiKey apiKey() {
        return ApiKey.CREATE_TOPIC;
    }
}
