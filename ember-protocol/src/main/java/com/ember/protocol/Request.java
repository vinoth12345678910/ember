package com.ember.protocol;

public sealed interface Request permits ProduceRequest, FetchRequest, MetadataRequest, CreateTopicRequest, RaftMessageRequest,
        FindCoordinatorRequest, JoinGroupRequest, HeartbeatRequest, LeaveGroupRequest, OffsetCommitRequest, OffsetFetchRequest, DescribeGroupRequest {
    ApiKey apiKey();
}
