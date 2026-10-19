package com.ember.raft;

import java.util.Arrays;

/**
 * One slot of the replicated log. The command array is copied on the way in and on the way out:
 * an entry that changes after it was replicated would silently break consensus.
 * equals/hashCode compare CONTENT (arrays compare by identity in Java) and never use
 * Enum.hashCode (an identity hash that differs between JVM runs).
 */
public record LogEntry(long term, EntryType type, byte[] command) {

    public LogEntry {
        command = command == null ? new byte[0] : command.clone();
    }

    public static LogEntry noop(long term) {
        return new LogEntry(term, EntryType.NOOP, new byte[0]);
    }

    public static LogEntry command(long term, byte[] command) {
        return new LogEntry(term, EntryType.COMMAND, command);
    }

    @Override
    public byte[] command() {
        return command.clone();
    }

    /** Avoids the copy of command() for read-only comparisons inside this package. */
    byte[] rawCommand() {
        return command;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof LogEntry e && e.term == term && e.type == type && Arrays.equals(e.command, command);
    }

    @Override
    public int hashCode() {
        return 31 * (31 * Long.hashCode(term) + type.ordinal()) + Arrays.hashCode(command);
    }

    @Override
    public String toString() {
        return "LogEntry(term=" + term + "," + type + "," + command.length + "B)";
    }
}
