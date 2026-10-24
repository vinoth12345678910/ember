package com.ember.protocol;

/**
 * Creates a subscription: a named reader of one topic with its own per-message progress. fromStart=false starts at the
 * end of every partition (only messages produced from now on); leaseMs is the default time a claimed message may be held.
 */
public record QueueCreateRequest(String subscription, String topic, boolean fromStart, int leaseMs) implements Request {
    @Override
    public ApiKey apiKey() {
        return ApiKey.QUEUE_CREATE;
    }
}
