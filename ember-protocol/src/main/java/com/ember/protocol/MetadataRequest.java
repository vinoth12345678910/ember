package com.ember.protocol;

import java.util.List;

/** An empty topic list means "all topics". */
public record MetadataRequest(List<String> topics) implements Request {
    @Override
    public ApiKey apiKey() {
        return ApiKey.METADATA;
    }
}
