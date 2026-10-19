package com.ember.client;

import java.util.List;

/**
 * autoCreateTopics: when metadata says a topic is missing, the producer creates it with defaultPartitions.
 * bootstrap: "host:port" of brokers to ask for the cluster layout (any one is enough).
 * retries / retryBackoffMs / maxRetryBackoffMs: how hard the cluster client tries before giving up.
 */
public record ClientConfig(int connectTimeoutMs, int readTimeoutMs, int maxFrameBytes,
                           boolean autoCreateTopics, int defaultPartitions,
                           List<String> bootstrap, int retries, int retryBackoffMs, int maxRetryBackoffMs) {

    public ClientConfig {
        bootstrap = List.copyOf(bootstrap);
    }

    public ClientConfig(int connectTimeoutMs, int readTimeoutMs, int maxFrameBytes, boolean autoCreateTopics, int defaultPartitions) {
        this(connectTimeoutMs, readTimeoutMs, maxFrameBytes, autoCreateTopics, defaultPartitions, List.of(), 10, 100, 2000);
    }

    public ClientConfig(int connectTimeoutMs, int readTimeoutMs, int maxFrameBytes) {
        this(connectTimeoutMs, readTimeoutMs, maxFrameBytes, true, 3);
    }

    public static ClientConfig defaults() {
        return new ClientConfig(5_000, 30_000, 16 * 1024 * 1024);
    }

    /** The request timeout is the read timeout of the connections. */
    public int requestTimeoutMs() {
        return readTimeoutMs;
    }

    public ClientConfig withAutoCreate(boolean autoCreateTopics, int defaultPartitions) {
        return new ClientConfig(connectTimeoutMs, readTimeoutMs, maxFrameBytes, autoCreateTopics, defaultPartitions,
                bootstrap, retries, retryBackoffMs, maxRetryBackoffMs);
    }

    public ClientConfig withBootstrap(List<String> bootstrap) {
        return new ClientConfig(connectTimeoutMs, readTimeoutMs, maxFrameBytes, autoCreateTopics, defaultPartitions,
                bootstrap, retries, retryBackoffMs, maxRetryBackoffMs);
    }

    public ClientConfig withRetries(int retries, int backoffMs, int maxBackoffMs) {
        return new ClientConfig(connectTimeoutMs, readTimeoutMs, maxFrameBytes, autoCreateTopics, defaultPartitions,
                bootstrap, retries, backoffMs, maxBackoffMs);
    }

    public ClientConfig withTimeouts(int connectMs, int readMs) {
        return new ClientConfig(connectMs, readMs, maxFrameBytes, autoCreateTopics, defaultPartitions,
                bootstrap, retries, retryBackoffMs, maxRetryBackoffMs);
    }
}
