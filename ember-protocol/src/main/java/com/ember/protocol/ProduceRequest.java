package com.ember.protocol;

import java.util.List;

public record ProduceRequest(String topic, int partition, List<ProducerRecord> records) implements Request {
    @Override
    public ApiKey apiKey() {
        return ApiKey.PRODUCE;
    }
}
