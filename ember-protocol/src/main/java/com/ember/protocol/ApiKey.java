package com.ember.protocol;

public enum ApiKey {
    PRODUCE(0),
    FETCH(1),
    METADATA(2),
    CREATE_TOPIC(3),
    /** Broker-to-broker Raft traffic: one-way, no response frame. */
    RAFT_MESSAGE(10),
    FIND_COORDINATOR(11),
    JOIN_GROUP(12),
    HEARTBEAT(13),
    LEAVE_GROUP(14),
    OFFSET_COMMIT(15),
    OFFSET_FETCH(16),
    DESCRIBE_GROUP(17),
    /** Ack queues (per-message acknowledgement), served by the leader of the "__acks" Raft group. */
    QUEUE_FIND(20),
    QUEUE_CREATE(21),
    QUEUE_CLAIM(22),
    QUEUE_ACK(23),
    QUEUE_RELEASE(24),
    QUEUE_EXTEND(25),
    QUEUE_DESCRIBE(26);

    private final short code;

    ApiKey(int code) {
        this.code = (short) code;
    }

    public short code() {
        return code;
    }

    /**
     * Versions this build understands. METADATA has 0, 1 (adds the partition leader) and 2 (cluster-aware); every
     * other API only has 0. Adding a version is how the protocol evolves without breaking old clients.
     */
    public boolean supportsVersion(short version) {
        return version == 0 || (this == METADATA && (version == 1 || version == 2));
    }

    /** Returns null for an unknown code; the caller decides what that means (UNSUPPORTED_API). */
    public static ApiKey fromCode(short code) {
        for (ApiKey k : values()) {
            if (k.code == code) {
                return k;
            }
        }
        return null;
    }
}
