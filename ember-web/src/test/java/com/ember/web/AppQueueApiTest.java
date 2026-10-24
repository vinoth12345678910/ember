package com.ember.web;

import com.ember.broker.BrokerConfig;
import com.ember.broker.BrokerServer;
import com.ember.client.ClientConfig;
import com.ember.client.EmberProducer;
import com.ember.client.QueueConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Clock;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** The HTTP API apps use for ack queues, against a real in-process broker. */
class AppQueueApiTest {

    @TempDir
    Path dir;
    BrokerServer broker;
    BrokerGateway gateway;
    WebServer web;
    AppStore apps;
    String base;
    final HttpClient http = HttpClient.newHttpClient();

    String workerKey;       // may consume "orders" and produce to it
    String otherKey;        // may only consume "invoices"
    String readOnlyKey;     // may consume "orders"

    @BeforeEach
    void start() throws Exception {
        broker = new BrokerServer(BrokerConfig.parse(new String[]{"--port=0", "--data-dir=" + dir.resolve("b"), "--flush-ms=never", "--default-partitions=2",
                "--raft-election-min-ms=50", "--raft-election-max-ms=100", "--raft-heartbeat-ms=20", "--unsafe-no-raft-fsync", "--produce-timeout-ms=5000",
                "--maintenance-interval-ms=3600000"}), Clock.systemUTC());
        int port = broker.start();
        String bootstrap = "localhost:" + port;
        ClientConfig cfg = ClientConfig.defaults().withBootstrap(List.of(bootstrap)).withRetries(40, 50, 400).withAutoCreate(true, 2);
        try (EmberProducer p = new EmberProducer(cfg)) {
            for (int i = 0; i < 6; i++) p.send("orders", ("k" + i).getBytes(), ("order-" + i).getBytes());
            p.send("invoices", null, "inv-0".getBytes());
        }
        QueueConsumer.createSubscription(cfg, "fulfil", "orders", true, 700, 2, 50, 50);
        QueueConsumer.createSubscription(cfg, "billing", "invoices", true, 5_000);

        apps = new AppStore(null);
        workerKey = apps.register("worker-app", "", List.of("orders"), List.of("orders"), 1).key();
        otherKey = apps.register("other-app", "", List.of(), List.of("invoices"), 1).key();
        readOnlyKey = apps.register("second-app", "", List.of(), List.of("orders"), 1).key();
        gateway = new BrokerGateway(List.of(bootstrap), List.of());
        web = new WebServer("127.0.0.1", 0, new UserStore(null), apps, new Tokens("secret-secret-secret".getBytes(), 60_000), gateway, Clock.systemUTC());
        base = "http://127.0.0.1:" + web.port();
    }

    @AfterEach
    void stop() throws Exception {
        web.close();
        gateway.close();
        broker.close();
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> call(String method, String path, String key, String body, int expectStatus) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(base + path)).header("Authorization", "Bearer " + key);
        HttpRequest r = (method.equals("GET") ? b.GET() : b.method(method, HttpRequest.BodyPublishers.ofString(body == null ? "{}" : body))).build();
        HttpResponse<String> resp = http.send(r, HttpResponse.BodyHandlers.ofString());
        assertEquals(expectStatus, resp.statusCode(), method + " " + path + " -> " + resp.body());
        return Json.parseObject(resp.body());
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> messages(Map<String, Object> r) {
        return (List<Map<String, Object>>) r.get("messages");
    }

    private static String handles(List<Map<String, Object>> msgs) {
        StringBuilder b = new StringBuilder("[");
        for (int i = 0; i < msgs.size(); i++) {
            Map<String, Object> m = msgs.get(i);
            if (i > 0) b.append(',');
            b.append("{\"partition\":").append(((Number) m.get("partition")).intValue()).append(",\"offset\":").append(((Number) m.get("offset")).longValue())
                    .append(",\"attempt\":").append(((Number) m.get("attempt")).intValue()).append('}');
        }
        return b.append(']').toString();
    }

    private List<Map<String, Object>> claimAll(String key, String worker, int n) throws Exception {
        List<Map<String, Object>> out = new java.util.ArrayList<>();
        long deadline = System.nanoTime() + 15_000_000_000L;
        while (out.size() < n && System.nanoTime() < deadline) {
            out.addAll(messages(call("POST", "/api/app/queues/fulfil/claim", key, "{\"max\":" + (n - out.size()) + ",\"worker\":\"" + worker + "\"}", 200)));
            Thread.sleep(20);
        }
        return out;
    }

    @Test
    void claimAckRoundTripDeliversEveryMessageOnce() throws Exception {
        List<Map<String, Object>> got = claimAll(workerKey, "w1", 6);
        assertEquals(6, got.size());
        assertEquals(6, got.stream().map(m -> m.get("value")).distinct().count());
        assertTrue(got.stream().allMatch(m -> ((Number) m.get("attempt")).intValue() == 1));
        Map<String, Object> ack = call("POST", "/api/app/queues/fulfil/ack", workerKey, "{\"worker\":\"w1\",\"handles\":" + handles(got) + "}", 200);
        assertEquals(6, ((Number) ack.get("settled")).intValue());
        assertEquals(0, ((Number) ack.get("rejected")).intValue());
        assertTrue(messages(call("POST", "/api/app/queues/fulfil/claim", workerKey, "{\"max\":10}", 200)).isEmpty());

        Map<String, Object> d = call("GET", "/api/app/queues/fulfil", workerKey, null, 200);
        assertEquals("orders", d.get("topic"));
        assertEquals(0, ((Number) d.get("waiting")).intValue());
        assertEquals(0, ((Number) d.get("inFlight")).intValue());
        assertEquals(2, ((Number) d.get("maxAttempts")).intValue());
        assertEquals(6, apps.list().stream().filter(a -> a.get("name").equals("worker-app")).mapToLong(a -> ((Number) a.get("acked")).longValue()).sum());
    }

    @Test
    void workersOfOneAppCannotAckEachOthersMessagesAndAnotherAppCannotTouchThemAtAll() throws Exception {
        List<Map<String, Object>> mine = claimAll(workerKey, "w1", 3);
        String h = handles(mine);
        assertEquals(3, ((Number) call("POST", "/api/app/queues/fulfil/ack", workerKey, "{\"worker\":\"w2\",\"handles\":" + h + "}", 200).get("rejected")).intValue(),
                "w2 does not hold what w1 claimed");
        // an app with read access to the same topic is a different worker identity, so it is rejected too
        assertEquals(3, ((Number) call("POST", "/api/app/queues/fulfil/ack", readOnlyKey, "{\"worker\":\"w1\",\"handles\":" + h + "}", 200).get("rejected")).intValue());
        // an app without access to the topic is refused outright
        call("POST", "/api/app/queues/fulfil/claim", otherKey, "{\"max\":1}", 403);
        call("GET", "/api/app/queues/fulfil", otherKey, null, 403);
        assertEquals(3, ((Number) call("POST", "/api/app/queues/fulfil/ack", workerKey, "{\"worker\":\"w1\",\"handles\":" + h + "}", 200).get("settled")).intValue());
    }

    @Test
    void aNackedMessageComesBackWithTheNextAttemptThenDiesAndLeavesTheQueueAlone() throws Exception {
        List<Map<String, Object>> got = claimAll(workerKey, "w1", 6);
        List<Map<String, Object>> poison = got.subList(0, 1), fine = got.subList(1, got.size());
        call("POST", "/api/app/queues/fulfil/ack", workerKey, "{\"worker\":\"w1\",\"handles\":" + handles(fine) + "}", 200);
        assertEquals(1, ((Number) call("POST", "/api/app/queues/fulfil/nack", workerKey,
                "{\"worker\":\"w1\",\"reason\":\"bad address\",\"handles\":" + handles(poison) + "}", 200).get("settled")).intValue());
        List<Map<String, Object>> again = claimAll(workerKey, "w2", 1);
        assertEquals(poison.get(0).get("value"), again.get(0).get("value"));
        assertEquals(2, ((Number) again.get(0).get("attempt")).intValue());
        call("POST", "/api/app/queues/fulfil/nack", workerKey, "{\"worker\":\"w2\",\"reason\":\"bad address\",\"handles\":" + handles(again) + "}", 200);
        Map<String, Object> d = call("GET", "/api/app/queues/fulfil", workerKey, null, 200);
        assertEquals(1, ((Number) d.get("dead")).intValue(), "maxAttempts is 2");
        assertEquals(0, ((Number) d.get("retrying")).intValue());
    }

    @Test
    void anExtendedLeaseSurvivesAndAReleasedMessageIsAvailableAtOnce() throws Exception {
        List<Map<String, Object>> got = claimAll(workerKey, "w1", 2);
        call("POST", "/api/app/queues/fulfil/extend", workerKey, "{\"worker\":\"w1\",\"leaseMs\":60000,\"handles\":" + handles(got.subList(0, 1)) + "}", 200);
        call("POST", "/api/app/queues/fulfil/release", workerKey, "{\"worker\":\"w1\",\"handles\":" + handles(got.subList(1, 2)) + "}", 200);
        Thread.sleep(1_200);                                                   // the 700 ms default lease has run out; the extended one has not
        List<Map<String, Object>> back = claimAll(workerKey, "w2", 5);
        assertTrue(back.stream().anyMatch(m -> m.get("value").equals(got.get(1).get("value"))), "the released message is redeliverable");
        assertTrue(back.stream().noneMatch(m -> m.get("value").equals(got.get(0).get("value"))), "the extended message is still w1's");
    }

    @Test
    void longPollingWaitsForNewMessagesWithoutBusyLooping() throws Exception {
        List<Map<String, Object>> drained = claimAll(workerKey, "drain", 6);                // take the 6 that exist...
        call("POST", "/api/app/queues/fulfil/ack", workerKey, "{\"worker\":\"drain\",\"handles\":" + handles(drained) + "}", 200);   // ...and finish them
        long t0 = System.nanoTime();
        Thread producer = new Thread(() -> {
            try {
                Thread.sleep(400);
                call("POST", "/api/app/topics/orders/messages", workerKey, "{\"key\":\"late\",\"value\":\"arrived-late\"}", 200);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        producer.start();
        List<Map<String, Object>> got = messages(call("POST", "/api/app/queues/fulfil/claim", workerKey, "{\"max\":5,\"waitMs\":8000,\"worker\":\"poller\"}", 200));
        long ms = (System.nanoTime() - t0) / 1_000_000;
        producer.join();
        assertEquals(1, got.size());
        assertEquals("arrived-late", got.get(0).get("value"));
        assertTrue(ms >= 300 && ms < 6_000, "returned when the message arrived, not at the timeout: " + ms + " ms");
        long t1 = System.nanoTime();
        assertTrue(messages(call("POST", "/api/app/queues/fulfil/claim", workerKey, "{\"max\":5,\"waitMs\":500,\"worker\":\"poller\"}", 200)).isEmpty());
        assertTrue((System.nanoTime() - t1) / 1_000_000 >= 450, "an empty long poll waits for the requested time");
    }

    @Test
    void badInputAndUnknownQueuesAreAnsweredCleanly() throws Exception {
        call("POST", "/api/app/queues/nope/claim", workerKey, "{}", 404);
        call("POST", "/api/app/queues/fulfil/claim", workerKey, "{\"max\":0}", 400);
        call("POST", "/api/app/queues/fulfil/claim", workerKey, "{\"max\":501}", 400);
        call("POST", "/api/app/queues/fulfil/claim", workerKey, "{\"worker\":\"bad name!\"}", 400);
        call("POST", "/api/app/queues/fulfil/ack", workerKey, "{\"handles\":\"x\"}", 400);
        call("POST", "/api/app/queues/fulfil/claim", "emb_nope", "{}", 401);
        call("PUT", "/api/app/queues/fulfil/claim", workerKey, "{}", 405);
        call("GET", "/api/app/queues/fulfil/claim", workerKey, null, 405);
    }
}
