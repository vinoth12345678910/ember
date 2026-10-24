package com.ember.client;

import com.ember.protocol.ApiKey;
import com.ember.protocol.QueueDeadListResponse;
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
        return createSubscription(config, subscription, topic, fromStart, leaseMs, 0, 0, 0);
    }

    /**
     * With a retry policy: a message that fails (nack, or its lease runs out) maxAttempts times is dead-lettered; between failed attempts it
     * waits backoffBaseMs, doubling each time up to backoffMaxMs (plus a little jitter). 0 = the default (5 attempts, 1 s, 60 s).
     */
    public static int createSubscription(ClientConfig config, String subscription, String topic, boolean fromStart, int leaseMs, int maxAttempts,
                                         int backoffBaseMs, int backoffMaxMs) throws IOException {
        try (ClusterClient c = new ClusterClient(config)) {
            return c.queueCreate(subscription, topic, fromStart, leaseMs, maxAttempts, backoffBaseMs, backoffMaxMs);
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

    /**
     * The messages could not be processed (not the worker's fault: for example a bad payload or a failing downstream). They are retried after
     * a backoff, and dead-lettered once their attempts are used up. Use release() instead when the worker merely cannot take them now.
     */
    public QueueSettleResponse nack(List<QueueMessage> messages, String reason) throws IOException {
        return client.queueNack(subscription, workerId, handles(messages), reason);
    }

    /** The dead-lettered messages (at most max, 1..1000) with their content, and how many there are in total. */
    public QueueDeadListResponse deadLetters(int max) throws IOException {
        return client.queueDeadList(subscription, max);
    }

    /** Puts dead-lettered messages back in the queue with a fresh failure budget. */
    public QueueSettleResponse retryDead(List<QueueDeadListResponse.DeadMessage> messages) throws IOException {
        return client.queueSettle(ApiKey.QUEUE_DEAD_RETRY, subscription, workerId, deadHandles(messages), 0);
    }

    /** Finishes dead-lettered messages for good (they are never delivered again). */
    public QueueSettleResponse discardDead(List<QueueDeadListResponse.DeadMessage> messages) throws IOException {
        return client.queueSettle(ApiKey.QUEUE_DEAD_DISCARD, subscription, workerId, deadHandles(messages), 0);
    }

    public QueueDescribeResponse describe() throws IOException {
        return client.queueDescribe(subscription);
    }

    public String workerId() {
        return workerId;
    }

    private static List<QueueHandle> deadHandles(List<QueueDeadListResponse.DeadMessage> messages) {
        List<QueueHandle> out = new ArrayList<>(messages.size());
        for (QueueDeadListResponse.DeadMessage m : messages) {
            out.add(new QueueHandle(m.partition(), m.offset(), 0));
        }
        return out;
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
