package com.ember.client;

import com.ember.protocol.MetadataResponse;
import com.ember.protocol.ProducerRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Everything over ONE connection, no routing and no retries: the single-broker behaviour of Phase 3. */
final class SingleConnectionRouter implements Router {

    private final EmberClient client;

    SingleConnectionRouter(EmberClient client) {
        this.client = client;
    }

    @Override
    public long produce(String topic, int partition, List<ProducerRecord> records) throws IOException {
        return client.produce(topic, partition, records);
    }

    @Override
    public FetchResult fetch(String topic, int partition, long offset, int maxBytes) throws IOException {
        return client.fetch(topic, partition, offset, maxBytes);
    }

    @Override
    public int createTopic(String topic, int partitions) throws IOException {
        return client.createTopic(topic, partitions);
    }

    @Override
    public List<PartitionView> describe(String topic) throws IOException {
        for (MetadataResponse.TopicMetadata t : client.metadata(topic).topics()) {
            if (t.topic().equals(topic)) {
                List<PartitionView> out = new ArrayList<>();
                for (MetadataResponse.PartitionMetadata p : t.partitions()) {
                    out.add(new PartitionView(p.partition(), p.leaderNodeId(), p.logStartOffset(), p.logEndOffset()));
                }
                return out;
            }
        }
        return null;
    }

    @Override
    public void close() throws IOException {
        client.close();
    }
}
