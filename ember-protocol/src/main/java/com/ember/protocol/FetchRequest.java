package com.ember.protocol;

public record FetchRequest(String topic, int partition, long fetchOffset, int maxBytes) implements Request {
    @Override
    public ApiKey apiKey() {
        return ApiKey.FETCH;
    }
}
