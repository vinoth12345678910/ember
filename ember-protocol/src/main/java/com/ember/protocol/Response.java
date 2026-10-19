package com.ember.protocol;

public sealed interface Response permits ProduceResponse, FetchResponse, MetadataResponse, CreateTopicResponse, ClusterMetadataResponse {
}
