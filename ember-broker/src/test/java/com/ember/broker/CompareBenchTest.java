package com.ember.broker;

import com.ember.client.EmberCli;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** compare-bench on a real cluster (scaled down): the claims in the README about position-only versus per-message consumption, as assertions. */
class CompareBenchTest {

    @SuppressWarnings("unchecked")
    @Test
    void aStuckGroupALossyGroupAndAnAckQueueThatKeepsEverything(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, false)) {
            c.awaitController();
            Path json = dir.resolve("compare.json");
            ByteArrayOutputStream out = new ByteArrayOutputStream(), err = new ByteArrayOutputStream();
            int code = EmberCli.run(new String[]{"compare-bench", "--bootstrap=" + c.bootstrap(), "--records=300", "--partitions=3", "--poison-every=25", "--work-ms=5",
                    "--seconds=10", "--queue-workers=9", "--json=" + json}, new PrintStream(out), new PrintStream(err));
            assertEquals(0, code, err.toString(StandardCharsets.UTF_8) + out.toString(StandardCharsets.UTF_8));

            Map<String, Object> result = TestJson.object(Files.readString(json));
            List<Object> rows = (List<Object>) result.get("rows");
            assertEquals(4, rows.size());
            Map<String, Object> retry = (Map<String, Object>) rows.get(0), skip = (Map<String, Object>) rows.get(1), same = (Map<String, Object>) rows.get(2), many = (Map<String, Object>) rows.get(3);
            int healthy = ((Number) retry.get("healthyTotal")).intValue();
            assertEquals(288, healthy, "300 messages, every 25th is poison");

            assertEquals(false, retry.get("finished"), "a poison message retried in place blocks its partition for ever");
            assertTrue(((Number) retry.get("healthyProcessed")).intValue() < healthy, "messages behind the poison one never ran");

            assertEquals(true, skip.get("finished"));
            assertEquals(healthy, ((Number) skip.get("healthyProcessed")).intValue());
            assertTrue(((String) skip.get("poisonOutcome")).startsWith("12 lost"), "skipping loses every poison message: " + skip.get("poisonOutcome"));

            for (Map<String, Object> q : List.of(same, many)) {
                assertEquals(true, q.get("finished"), q.toString());
                assertEquals(healthy, ((Number) q.get("healthyProcessed")).intValue());
                assertTrue(((String) q.get("poisonOutcome")).startsWith("12 dead-lettered"), "the ack queue keeps them: " + q.get("poisonOutcome"));
            }
            assertTrue(((Number) many.get("elapsedMs")).longValue() < ((Number) same.get("elapsedMs")).longValue(),
                    "more workers than partitions finish sooner: " + many.get("elapsedMs") + " ms vs " + same.get("elapsedMs") + " ms");
        }
    }
}
