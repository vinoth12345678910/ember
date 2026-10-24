package com.ember.protocol;

import java.util.List;

/** The worker could not process these messages. They are retried after a backoff, or dead-lettered when their attempts are used up. */
public record QueueNackRequest(String subscription, String worker, List<QueueHandle> handles, String reason) implements Request {
    public QueueNackRequest {
        handles = List.copyOf(handles);
        reason = reason == null ? "" : reason;
    }

    @Override
    public ApiKey apiKey() {
        return ApiKey.QUEUE_NACK;
    }
}
