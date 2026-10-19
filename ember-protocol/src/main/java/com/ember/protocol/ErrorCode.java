package com.ember.protocol;

public enum ErrorCode {
    NONE(0),
    OFFSET_OUT_OF_RANGE(1),
    UNKNOWN_TOPIC_OR_PARTITION(2),
    INVALID_REQUEST(3),
    UNSUPPORTED_API(4),
    UNSUPPORTED_VERSION(5),
    REQUEST_TOO_LARGE(6),
    INTERNAL_ERROR(7),
    TOPIC_ALREADY_EXISTS(8),
    /** The broker is not (or no longer) the Raft leader of that partition: refresh metadata and retry elsewhere. */
    NOT_LEADER_OR_FOLLOWER(9),
    /** Not committed in time. AMBIGUOUS: the record may still be committed, so a retry can duplicate it. */
    REQUEST_TIMED_OUT(10),
    /** The partition has no leader right now (an election is going on): back off and retry. */
    LEADER_NOT_AVAILABLE(11),
    /** CREATE_TOPIC sent to a broker that is not the leader of the META group. */
    NOT_CONTROLLER(12),
    /** Too many produce requests are waiting for a commit in this partition. */
    BROKER_OVERLOADED(13);

    private final short code;

    ErrorCode(int code) {
        this.code = (short) code;
    }

    public short code() {
        return code;
    }

    public static ErrorCode fromCode(short code) {
        for (ErrorCode e : values()) {
            if (e.code == code) {
                return e;
            }
        }
        throw new ProtocolException("unknown error code " + code);
    }
}
