package com.ember.protocol;

public record CreateTopicResponse(String topic, int partitionCount) implements Response {
}
