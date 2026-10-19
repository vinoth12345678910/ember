package com.ember.protocol;

public record ProduceResponse(String topic, int partition, long baseOffset, int recordCount) implements Response {
}
