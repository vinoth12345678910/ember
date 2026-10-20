package com.ember.broker;

import com.ember.client.EmberCli;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.time.Clock;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** `cluster status`, `cluster metrics`, `healthcheck` against a real in-process cluster. */
class ClusterCliTest {

    private record Result(int code, String out, String err) {
    }

    private static Result cli(String... args) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        int code = EmberCli.run(args, new PrintStream(out), new PrintStream(err));
        return new Result(code, out.toString(), err.toString());
    }

    private static String admins(TestCluster c) {
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= c.n; i++) sb.append(i > 1 ? "," : "").append("localhost:").append(c.adminPort(i));
        return sb.toString();
    }

    @Test
    void clusterStatusListsEveryNodeAndTheRoleOfEveryNodeInEveryGroup(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, false)) {
            ClusterIntegrationTest.createTopic(c, "t", 2);
            Await.until(30_000, "every broker to be ready\n" + c.describe(), () -> {
                Result r = cli("cluster", "status", "--admin=" + admins(c));
                return r.code() == 0 && !r.out().contains("NOT READY") && !r.out().contains("UNREACHABLE");
            });
            Result r = cli("cluster", "status", "--admin=" + admins(c));
            assertEquals(0, r.code(), r.err());
            for (int i = 1; i <= 3; i++) assertTrue(r.out().contains("localhost:" + c.adminPort(i)), r.out());
            assertEquals(3, r.out().lines().filter(l -> l.contains(" ready ")).count(), r.out());
            for (String group : new String[]{"__meta-0", "__groups-0", "t-0", "t-1"}) {
                String row = r.out().lines().filter(l -> l.startsWith(group + " ")).findFirst().orElseThrow(() -> new AssertionError("no row for " + group + "\n" + r.out()));
                assertEquals(1, row.split("leader/", -1).length - 1, "exactly one leader in " + row);
                assertTrue(row.contains("follower/"), row);
            }
            int controller = c.awaitController();
            assertTrue(r.out().lines().filter(l -> l.startsWith("__meta-0")).findFirst().orElseThrow().trim().endsWith(Integer.toString(controller)));
        }
    }

    @Test
    void anUnreachableBrokerIsReportedNotFatal(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, false)) {
            c.awaitController();
            String list = admins(c) + ",localhost:1";
            Result r = cli("cluster", "status", "--admin=" + list);
            assertEquals(0, r.code(), r.err());
            assertTrue(r.out().contains("UNREACHABLE"), r.out());
        }
    }

    @Test
    void clusterMetricsPrintsMatchingSamplesOnly(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 1, false)) {
            ClusterIntegrationTest.createTopic(c, "t", 1);
            Result r = cli("cluster", "metrics", "--admin=localhost:" + c.adminPort(1), "--filter=ember_raft_term");
            assertEquals(0, r.code(), r.err());
            assertTrue(r.out().lines().count() >= 3, r.out());
            assertTrue(r.out().lines().allMatch(l -> l.startsWith("ember_raft_term")), r.out());
            assertFalse(r.out().contains("#"), "comment lines are not printed");
            assertTrue(cli("cluster", "metrics", "--admin=localhost:" + c.adminPort(1)).out().contains("ember_jvm_heap_used_bytes"));
        }
    }

    @Test
    void healthcheckExitsZeroOnlyWhenReady(@TempDir Path dir) throws Exception {
        BrokerServer server = new BrokerServer(ServerFixture.config(dir, "--admin-port=ephemeral"), Clock.systemUTC());
        try {
            int port = server.bind();
            String admin = "localhost:" + server.adminPort();
            Result notReady = cli("healthcheck", "--admin=" + admin);
            assertEquals(1, notReady.code());
            assertTrue(notReady.err().contains("not ready") && notReady.err().contains("not joined"), notReady.err());

            server.start(Map.of(0, new InetSocketAddress("localhost", port)));
            Await.until(20_000, "healthcheck to turn green", () -> cli("healthcheck", "--admin=" + admin).code() == 0);
            assertTrue(cli("healthcheck", "--admin=" + admin).out().contains("ready"));
        } finally {
            server.close();
        }
        Result unreachable = cli("healthcheck", "--admin=localhost:1");
        assertEquals(1, unreachable.code());
        assertTrue(unreachable.err().contains("unreachable"), unreachable.err());
    }

    @Test
    void badFlagsAreUsageErrors() {
        assertEquals(2, cli("cluster").code());
        assertEquals(2, cli("cluster", "status").code(), "--admin is required");
        assertEquals(2, cli("cluster", "status", "--admin=nonsense").code());
        assertEquals(2, cli("cluster", "explode", "--admin=a:1").code());
    }
}
