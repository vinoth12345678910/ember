package com.ember.raft;

public enum EntryType {
    /** Appended by a new leader so it can commit something from its own term (paper 5.4.2, 8). */
    NOOP,
    COMMAND
}
