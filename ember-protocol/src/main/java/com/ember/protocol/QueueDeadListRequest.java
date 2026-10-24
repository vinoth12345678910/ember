package com.ember.protocol;

/** The dead-lettered messages of a subscription (oldest first), at most maxItems. */
public record QueueDeadListRequest(String subscription, int maxItems) implements Request {
    @Override
    public ApiKey apiKey() {
        return ApiKey.QUEUE_DEAD_LIST;
    }
}
