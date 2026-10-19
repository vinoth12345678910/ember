package com.ember.raft;

/** How a node talks to its peers. Fire-and-forget: messages may be lost, duplicated or reordered. */
public interface RaftTransport {
    void send(int toNodeId, RaftMessage message);
}
