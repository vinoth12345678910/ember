package com.ember.protocol;

import java.util.List;

/**
 * QUEUE_ACK (done), QUEUE_RELEASE (give back for redelivery) or QUEUE_EXTEND (keep holding, new lease of leaseMs from now).
 * leaseMs is only used by QUEUE_EXTEND (0 = the subscription's default).
 */
public record QueueSettleRequest(ApiKey apiKey, String subscription, String worker, List<QueueHandle> handles, int leaseMs) implements Request {
    public QueueSettleRequest {
        if (apiKey != ApiKey.QUEUE_ACK && apiKey != ApiKey.QUEUE_RELEASE && apiKey != ApiKey.QUEUE_EXTEND) {
            throw new IllegalArgumentException("not a settle api: " + apiKey);
        }
        handles = List.copyOf(handles);
    }
}
