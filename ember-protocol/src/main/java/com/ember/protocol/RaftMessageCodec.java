package com.ember.protocol;

import com.ember.raft.EntryType;
import com.ember.raft.LogEntry;
import com.ember.raft.RaftMessage;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static com.ember.protocol.WireFormat.*;

/** The body of RAFT_MESSAGE after the group and sender: messageType + payload. Same strict rules as every codec. */
final class RaftMessageCodec {

    static final int REQUEST_VOTE = 1, REQUEST_VOTE_RESPONSE = 2, APPEND_ENTRIES = 3, APPEND_ENTRIES_RESPONSE = 4;
    /** term(8) + type(1) + length(4) */
    private static final int ENTRY_OVERHEAD = 13;

    private RaftMessageCodec() {
    }

    static int size(RaftMessage m) {
        return 1 + switch (m) {
            case RaftMessage.RequestVote v -> 8 + 4 + 8 + 8;
            case RaftMessage.RequestVoteResponse v -> 8 + 1;
            case RaftMessage.AppendEntries a -> {
                int n = 8 + 4 + 8 + 8 + 8 + 4;
                for (LogEntry e : a.entries()) {
                    n += ENTRY_OVERHEAD + e.command().length;
                }
                yield n;
            }
            case RaftMessage.AppendEntriesResponse a -> 8 + 1 + 8 + 8 + 8;
        };
    }

    static void encode(ByteBuffer buf, RaftMessage m) {
        switch (m) {
            case RaftMessage.RequestVote v -> {
                buf.put((byte) REQUEST_VOTE).putLong(v.term()).putInt(v.candidateId()).putLong(v.lastLogIndex()).putLong(v.lastLogTerm());
            }
            case RaftMessage.RequestVoteResponse v -> {
                buf.put((byte) REQUEST_VOTE_RESPONSE).putLong(v.term()).put((byte) (v.voteGranted() ? 1 : 0));
            }
            case RaftMessage.AppendEntries a -> {
                buf.put((byte) APPEND_ENTRIES).putLong(a.term()).putInt(a.leaderId()).putLong(a.prevLogIndex())
                        .putLong(a.prevLogTerm()).putLong(a.leaderCommit()).putInt(a.entries().size());
                for (LogEntry e : a.entries()) {
                    byte[] command = e.command();
                    buf.putLong(e.term()).put((byte) (e.type() == EntryType.NOOP ? 0 : 1)).putInt(command.length).put(command);
                }
            }
            case RaftMessage.AppendEntriesResponse a -> {
                buf.put((byte) APPEND_ENTRIES_RESPONSE).putLong(a.term()).put((byte) (a.success() ? 1 : 0))
                        .putLong(a.matchIndex()).putLong(a.conflictIndex()).putLong(a.conflictTerm());
            }
        }
    }

    static RaftMessage decode(ByteBuffer buf) {
        byte type = getByte(buf, "messageType");
        return switch (type) {
            case REQUEST_VOTE -> new RaftMessage.RequestVote(nonNegative(getLong(buf, "term"), "term"), getInt(buf, "candidateId"),
                    nonNegative(getLong(buf, "lastLogIndex"), "lastLogIndex"), nonNegative(getLong(buf, "lastLogTerm"), "lastLogTerm"));
            case REQUEST_VOTE_RESPONSE -> new RaftMessage.RequestVoteResponse(nonNegative(getLong(buf, "term"), "term"),
                    getBoolean(buf, "voteGranted"));
            case APPEND_ENTRIES -> decodeAppendEntries(buf);
            case APPEND_ENTRIES_RESPONSE -> new RaftMessage.AppendEntriesResponse(nonNegative(getLong(buf, "term"), "term"),
                    getBoolean(buf, "success"), nonNegative(getLong(buf, "matchIndex"), "matchIndex"),
                    nonNegative(getLong(buf, "conflictIndex"), "conflictIndex"), nonNegative(getLong(buf, "conflictTerm"), "conflictTerm"));
            default -> throw new ProtocolException("unknown raft messageType " + type);
        };
    }

    private static RaftMessage decodeAppendEntries(ByteBuffer buf) {
        long term = nonNegative(getLong(buf, "term"), "term");
        int leaderId = getInt(buf, "leaderId");
        long prevLogIndex = nonNegative(getLong(buf, "prevLogIndex"), "prevLogIndex");
        long prevLogTerm = nonNegative(getLong(buf, "prevLogTerm"), "prevLogTerm");
        long leaderCommit = nonNegative(getLong(buf, "leaderCommit"), "leaderCommit");
        int count = getCount(buf, ENTRY_OVERHEAD, "entry");
        List<LogEntry> entries = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            long entryTerm = nonNegative(getLong(buf, "entry term"), "entry term");
            byte entryType = getByte(buf, "entry type");
            if (entryType != 0 && entryType != 1) {
                throw new ProtocolException("unknown entry type " + entryType);
            }
            int length = getInt(buf, "entry length");
            if (length < 0) {
                throw new ProtocolException("negative entry length " + length);
            }
            byte[] command = getBytes(buf, length, "entry bytes");
            entries.add(new LogEntry(entryTerm, entryType == 0 ? EntryType.NOOP : EntryType.COMMAND, command));
        }
        return new RaftMessage.AppendEntries(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit);
    }

    private static long nonNegative(long value, String what) {
        if (value < 0) {
            throw new ProtocolException(what + " must not be negative, got " + value);
        }
        return value;
    }
}
