package com.ember.protocol;

public sealed interface Request permits ProduceRequest, FetchRequest, MetadataRequest, CreateTopicRequest, RaftMessageRequest {
    ApiKey apiKey();
}
