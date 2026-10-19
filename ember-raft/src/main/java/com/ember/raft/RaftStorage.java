package com.ember.raft;

import java.util.List;

/**
 * The persistent state of Figure 2: currentTerm, votedFor and the log. Log indexes start at 1;
 * index 0 means "none" and has term 0.
 *
 * The contract that matters is sync(): everything written before it survives a crash; anything
 * written after the last sync may be lost. RaftNode must call sync() BEFORE it tells anybody about
 * a new term, a vote or an appended entry.
 */
public interface RaftStorage {

    long currentTerm();

    /** -1 = has not voted in currentTerm. */
    int votedFor();

    /** Term and vote change together, atomically (a crash must not keep one and lose the other). */
    void setTermAndVote(long term, int votedFor);

    long lastIndex();

    long lastTerm();

    /** 0 for index 0. */
    long termAt(long index);

    LogEntry entryAt(long index);

    /** Up to maxCount entries starting at fromIndex (fewer, or none, near the end). */
    List<LogEntry> entries(long fromIndex, int maxCount);

    void append(List<LogEntry> entries);

    /** Deletes index and everything after it. */
    void truncateFrom(long index);

    /** Durability barrier. */
    void sync();
}
