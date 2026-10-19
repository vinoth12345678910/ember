package com.ember.raft;

import java.util.List;

/** The two RPCs of Figure 2, each as a request record and a response record. */
public sealed interface RaftMessage {

    long term();

    /** Figure 2: RequestVote arguments. */
    record RequestVote(long term, int candidateId, long lastLogIndex, long lastLogTerm) implements RaftMessage {
    }

    record RequestVoteResponse(long term, boolean voteGranted) implements RaftMessage {
    }

    /** Figure 2: AppendEntries arguments. entries is empty for a heartbeat. */
    record AppendEntries(long term, int leaderId, long prevLogIndex, long prevLogTerm, List<LogEntry> entries,
                         long leaderCommit) implements RaftMessage {
        public AppendEntries {
            entries = List.copyOf(entries);
        }
    }

    /**
     * matchIndex (on success) = prevLogIndex + entries.size(): the highest index the follower verified,
     * so the leader never has to correlate a response with a request.
     * conflictTerm/conflictIndex (on failure) are the accelerated-backtracking hint (paper 5.3, end).
     */
    record AppendEntriesResponse(long term, boolean success, long matchIndex, long conflictIndex,
                                 long conflictTerm) implements RaftMessage {
    }
}
