package com.ember.protocol;

import com.ember.raft.RaftMessage;

/** One Raft message from one broker to another, addressed to a Raft group ("__meta"/0 or topic/partition). */
public record RaftMessageRequest(String groupTopic, int groupPartition, int fromNodeId, RaftMessage message) implements Request {
    @Override
    public ApiKey apiKey() {
        return ApiKey.RAFT_MESSAGE;
    }
}
