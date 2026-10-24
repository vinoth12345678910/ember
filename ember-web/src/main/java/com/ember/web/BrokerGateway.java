package com.ember.web;

import com.ember.client.ClientConfig;
import com.ember.client.ClusterClient;
import com.ember.client.EmberProducer;
import com.ember.client.FetchResult;
import com.ember.client.PartitionView;
import com.ember.client.RecordMetadata;
import com.ember.protocol.ClusterMetadataResponse;
import com.ember.storage.LogRecord;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/** Everything the web console needs from the Ember cluster: data path through the client library, cluster health through the admin HTTP ports. */
final class BrokerGateway implements AutoCloseable {

    private static final int FETCH_BYTES = 512 * 1024;

    private final List<String> adminAddresses;
    private final ClusterClient client;
    private final EmberProducer producer;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();

    BrokerGateway(List<String> bootstrap, List<String> adminAddresses) {
        this.adminAddresses = adminAddresses;
        ClientConfig config = ClientConfig.defaults().withBootstrap(bootstrap).withAutoCreate(false, 3).withTimeouts(2_000, 8_000);
        this.client = new ClusterClient(config);
        this.producer = new EmberProducer(config);
    }

    // ------------------------------------------------------------------ data path

    List<Object> topics() throws IOException {
        ClusterMetadataResponse m = client.clusterMetadata();
        List<Object> out = new ArrayList<>();
        for (ClusterMetadataResponse.Topic t : m.topics()) {
            if (t.name().startsWith("__")) continue;                   // internal topics are not for the console
            List<Object> parts = new ArrayList<>();
            long messages = 0;
            List<PartitionView> views = client.describe(t.name());     // offsets from each partition's leader, not a possibly lagging follower
            for (ClusterMetadataResponse.Partition p : t.partitions()) {
                long start = p.logStartOffset();
                long end = p.logEndOffset();
                if (views != null) {
                    for (PartitionView v : views) {
                        if (v.partition() == p.partition()) {
                            start = v.logStartOffset();
                            end = v.logEndOffset();
                        }
                    }
                }
                messages += Math.max(0, end - start);
                parts.add(Json.obj("partition", p.partition(), "leader", p.leaderId(), "replicas", p.replicas(), "start", start, "end", end));
            }
            out.add(Json.obj("name", t.name(), "partitionCount", t.partitions().size(), "messages", messages, "partitions", parts));
        }
        out.sort((a, b) -> ((String) ((Map<?, ?>) a).get("name")).compareTo((String) ((Map<?, ?>) b).get("name")));
        return out;
    }

    List<Object> partitions(String topic) throws IOException {
        List<PartitionView> views = client.describe(topic);
        if (views == null) return null;
        List<Object> out = new ArrayList<>();
        for (PartitionView v : views) {
            out.add(Json.obj("partition", v.partition(), "leader", v.leaderId(), "start", v.logStartOffset(), "end", v.logEndOffset()));
        }
        return out;
    }

    Map<String, Object> produce(String topic, String key, String value) throws IOException {
        RecordMetadata md = producer.send(topic, key == null || key.isEmpty() ? null : key.getBytes(StandardCharsets.UTF_8),
                value.getBytes(StandardCharsets.UTF_8));
        return Json.obj("topic", md.topic(), "partition", md.partition(), "offset", md.offset());
    }

    Map<String, Object> fetch(String topic, int partition, long offset, int limit) throws IOException {
        FetchResult r = client.fetch(topic, partition, offset, FETCH_BYTES);
        List<Object> records = new ArrayList<>();
        for (LogRecord rec : r.records()) {
            if (records.size() >= limit) break;
            records.add(Json.obj("offset", rec.offset(), "timestamp", rec.timestamp(),
                    "key", rec.key() == null ? null : text(rec.key()), "value", rec.value() == null ? null : text(rec.value())));
        }
        long next = records.isEmpty() ? r.nextFetchOffset() : ((Number) ((Map<?, ?>) records.get(records.size() - 1)).get("offset")).longValue() + 1;
        return Json.obj("records", records, "nextOffset", next, "logStartOffset", r.logStartOffset(), "highWatermark", r.highWatermark());
    }

    int createTopic(String name, int partitions) throws IOException {
        return client.createTopic(name, partitions);
    }

    /** UTF-8 text when the bytes are valid UTF-8, otherwise "base64:..." so the browser never shows garbage. */
    private static String text(byte[] bytes) {
        try {
            return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException e) {
            return "base64:" + Base64.getEncoder().encodeToString(bytes);
        }
    }

    // ------------------------------------------------------------------ cluster health (admin ports)

    /** One entry per configured admin address: reachable or not, ready or not, and the broker's own /status document when reachable. */
    List<Object> nodes() {
        List<Object> out = new ArrayList<>();
        for (String address : adminAddresses) {
            Map<String, Object> node = Json.obj("admin", address);
            try {
                HttpResponse<String> status = get(address, "/status");
                if (status.statusCode() != 200) throw new IOException("status " + status.statusCode());
                node.put("up", true);
                node.put("status", Json.parseObject(status.body()));
            } catch (IOException | RuntimeException e) {
                node.put("up", false);
                node.put("error", String.valueOf(e.getMessage()));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                node.put("up", false);
                node.put("error", "interrupted");
            }
            out.add(node);
        }
        return out;
    }

    /** Consumer groups as reported by the first reachable broker (they are replicated, so any broker will do). */
    List<Object> consumerGroups() {
        for (Object o : nodes()) {
            Map<?, ?> node = (Map<?, ?>) o;
            if (Boolean.TRUE.equals(node.get("up")) && ((Map<?, ?>) node.get("status")).get("consumerGroups") instanceof List<?> groups) {
                return new ArrayList<>(groups);
            }
        }
        return List.of();
    }

    private HttpResponse<String> get(String address, String path) throws IOException, InterruptedException {
        HttpRequest req = HttpRequest.newBuilder(URI.create("http://" + address + path)).timeout(Duration.ofSeconds(3)).GET().build();
        return http.send(req, HttpResponse.BodyHandlers.ofString());
    }

    @Override
    public void close() throws IOException {
        producer.close();
        client.close();
    }
}
