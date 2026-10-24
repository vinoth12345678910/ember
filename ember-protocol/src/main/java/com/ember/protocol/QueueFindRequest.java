package com.ember.protocol;

/** Who leads the "__acks" Raft group? Answered with a FindCoordinatorResponse (node id, host, port). No body. */
public record QueueFindRequest() implements Request {
    @Override
    public ApiKey apiKey() {
        return ApiKey.QUEUE_FIND;
    }
}
