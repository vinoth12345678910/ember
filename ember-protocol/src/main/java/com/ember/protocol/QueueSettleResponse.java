package com.ember.protocol;

/** settled: handles accepted (an ack of an already finished message counts as settled). rejected: stale or not held by this worker. */
public record QueueSettleResponse(int settled, int rejected) implements Response {
}
