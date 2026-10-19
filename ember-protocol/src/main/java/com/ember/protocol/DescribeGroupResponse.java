package com.ember.protocol;

/** state: 0 unknown group, 1 empty, 2 stable. */
public record DescribeGroupResponse(int generation, int state, java.util.List<Member> members, java.util.List<PartitionOffset> offsets) implements Response {

    public static final int UNKNOWN = 0;
    public static final int EMPTY = 1;
    public static final int STABLE = 2;

    public record Member(String memberId, String clientId, int sessionTimeoutMs, java.util.List<String> topics,
                         java.util.List<PartitionRef> assignment) {
    }
}
