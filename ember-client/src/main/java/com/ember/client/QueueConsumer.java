package com.ember.client;

import com.ember.protocol.ApiKey;
import com.ember.protocol.QueueDescribeResponse;
import com.ember.protocol.QueueHandle;
import com.ember.protocol.QueueMessage;
import com.ember.protocol.QueueSettleResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A worker of an ack queue. Any number of workers (and processes) may share one subscription: each message is handed to one of them
 * at a time. The worker claims messages, processes them, and acks the ones it finished. A message that is not acked before its lease
 * runs out goes to another worker (at-least-once: process idempotently). A late ack from a worker that lost its lease is rejected.
 *
 * Not thread-safe by itself for claim() (the connection is shared); use one QueueConsumer per worker thread.
 */
public final class QueueConsumer implements AutoCloseable {

    private final ClusterClient client;
    private final String subscription;
    private final String workerId;

    public QueueConsumer(ClientConfig config, String subscription, String workerId) {
        this.client = new ClusterClient(config);
        this.subscription = subscription;
        this.workerId = workerId;
    }

    /** Creates a subscription to a topic (cluster-wide, once). Returns the partition count. Fails with SUBSCRIPTION_ALREADY_EXISTS if it exists. */
    public static int createSubscription(ClientConfig config, String subscription, String topic, boolean fromStart, int leaseMs) throws IOException {
        try (ClusterClient c = new ClusterClient(config)) {
            return c.queueCreate(subscription, topic, fromStart, leaseMs);
        }
    }

    /** Up to maxMessages messages (about maxBytes), possibly none. leaseMs 0 = the subscription's default. */
    public List<QueueMessage> claim(int maxMessages, int leaseMs, int maxBytes) throws IOException {
        return client.queueClaim(subscription, workerId, maxMessages, leaseMs, maxBytes);
    }

    public List<QueueMessage> claim(int maxMessages) throws IOException {
        return claim(maxMessages, 0, 4 * 1024 * 1024);
    }

    public QueueSettleResponse ack(List<QueueMessage> messages) throws IOException {
        return client.queueSettle(ApiKey.QUEUE_ACK, subscription, workerId, handles(messages), 0);
    }

    /** Gives messages back for redelivery to any worker (for example when this worker is shutting down or cannot process them now). */
    public QueueSettleResponse release(List<QueueMessage> messages) throws IOException {
        return client.queueSettle(ApiKey.QUEUE_RELEASE, subscription, workerId, handles(messages), 0);
    }

    /** Keeps the messages for another leaseMs (0 = the subscription's default) from now. */
    public QueueSettleResponse extend(List<QueueMessage> messages, int leaseMs) throws IOException {
        return client.queueSettle(ApiKey.QUEUE_EXTEND, subscription, workerId, handles(messages), leaseMs);
    }

    public QueueDescribeResponse describe() throws IOException {
        return client.queueDescribe(subscription);
    }

    public String workerId() {
        return workerId;
    }

    private static List<QueueHandle> handles(List<QueueMessage> messages) {
        List<QueueHandle> out = new ArrayList<>(messages.size());
        for (QueueMessage m : messages) {
            out.add(m.handle());
        }
        return out;
    }

    @Override
    public void close() throws IOException {
        client.close();
    }
}
