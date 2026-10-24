package com.ember.protocol;

import java.util.List;

/**
 * QUEUE_ACK (done), QUEUE_RELEASE (give back for redelivery, not a failure) or QUEUE_EXTEND (keep holding, new lease of leaseMs from now).
 * leaseMs is only used by QUEUE_EXTEND (0 = the subscription's default). QUEUE_DEAD_RETRY / QUEUE_DEAD_DISCARD act on dead-lettered
 * messages (admin action; the attempt in the handle is ignored and the worker name is only for the audit trail).
 */
public record QueueSettleRequest(ApiKey apiKey, String subscription, String worker, List<QueueHandle> handles, int leaseMs) implements Request {
    public QueueSettleRequest {
        if (apiKey != ApiKey.QUEUE_ACK && apiKey != ApiKey.QUEUE_RELEASE && apiKey != ApiKey.QUEUE_EXTEND
                && apiKey != ApiKey.QUEUE_DEAD_RETRY && apiKey != ApiKey.QUEUE_DEAD_DISCARD) {
            throw new IllegalArgumentException("not a settle api: " + apiKey);
        }
        handles = List.copyOf(handles);
    }
}
