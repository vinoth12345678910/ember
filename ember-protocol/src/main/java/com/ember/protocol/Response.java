package com.ember.protocol;

public sealed interface Response permits ProduceResponse, FetchResponse, MetadataResponse, CreateTopicResponse, ClusterMetadataResponse,
        FindCoordinatorResponse, JoinGroupResponse, HeartbeatResponse, LeaveGroupResponse, OffsetCommitResponse, OffsetFetchResponse, DescribeGroupResponse {
}
