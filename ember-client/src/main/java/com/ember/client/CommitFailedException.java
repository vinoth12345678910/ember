package com.ember.client;

/**
 * commitSync() could not commit because this consumer is no longer the owner of its partitions (it was replaced
 * after a rebalance or a missed session, or the generation changed). Treat the batch as POSSIBLY REPROCESSED by
 * the new owner: that is the at-least-once contract. The consumer rejoins on its next poll().
 */
public class CommitFailedException extends RuntimeException {

    private final com.ember.protocol.ErrorCode errorCode;

    public CommitFailedException(com.ember.protocol.ErrorCode errorCode) {
        super("commit failed: " + errorCode + " (this consumer was fenced; its uncommitted work may be redone by another member)");
        this.errorCode = errorCode;
    }

    public com.ember.protocol.ErrorCode errorCode() {
        return errorCode;
    }
}
