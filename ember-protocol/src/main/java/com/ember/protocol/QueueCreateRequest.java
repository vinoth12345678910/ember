package com.ember.protocol;

/**
 * Creates a subscription: a named reader of one topic with its own per-message progress. fromStart=false starts at the
 * end of every partition (only messages produced from now on); leaseMs is the default time a claimed message may be held.
 *
 * Retry policy (0 = the broker default): a message that fails (nack, or its lease runs out) maxAttempts times is dead-lettered;
 * between failed attempts it waits backoffBaseMs, doubling each time, up to backoffMaxMs.
 */
public record QueueCreateRequest(String subscription, String topic, boolean fromStart, int leaseMs, int maxAttempts, int backoffBaseMs,
                                 int backoffMaxMs) implements Request {
    @Override
    public ApiKey apiKey() {
        return ApiKey.QUEUE_CREATE;
    }
}
