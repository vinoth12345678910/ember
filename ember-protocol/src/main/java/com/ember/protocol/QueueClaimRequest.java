package com.ember.protocol;

/** Claim up to maxMessages (and about maxBytes) from any partition of the subscription's topic. leaseMs 0 = the subscription's default. */
public record QueueClaimRequest(String subscription, String worker, int maxMessages, int leaseMs, int maxBytes) implements Request {
    @Override
    public ApiKey apiKey() {
        return ApiKey.QUEUE_CLAIM;
    }
}
