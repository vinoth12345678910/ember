package com.ember.protocol;

public record JoinGroupResponse(int generation, String memberId, java.util.List<PartitionRef> assignment) implements Response {
}
