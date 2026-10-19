package com.ember.broker;

import com.ember.client.EmberCli;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

/** The real EmberCli against a real 3-broker cluster. */
class GroupCliTest {

    private static String[] withBootstrap(TestCluster c, String... args) {
        String[] all = Arrays.copyOf(args, args.length + 1);
        all[args.length] = "--bootstrap=" + c.bootstrap();
        return all;
    }

    private static String cli(int expectedExit, TestCluster c, String... args) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        int exit = EmberCli.run(withBootstrap(c, args), new PrintStream(out), new PrintStream(err));
        assertEquals(expectedExit, exit, "stderr: " + err);
        return out.toString();
    }

    @Test
    void consumeWithAGroupResumesAndDescribeShowsLag(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, false)) {
            ClusterIntegrationTest.createTopic(c, "orders", 3);
            EmberGroupConsumerTest.produce(c, "orders", 0, 30);

            String first = cli(0, c, "consume", "--group=cli", "--topic=orders", "--max=20", "--session-ms=3000");
            assertTrue(first.contains("(20 records)"), first);
            assertEquals(20, first.lines().filter(l -> l.startsWith("topic=orders")).count());

            String describe = cli(0, c, "group", "describe", "--group=cli");
            assertTrue(describe.contains("group cli"), describe);
            assertTrue(describe.contains("state EMPTY"), "the CLI consumer left the group at exit:\n" + describe);
            assertTrue(describe.contains("total lag: 10"), "20 of 30 were committed:\n" + describe);
            assertTrue(describe.contains("(unowned)"), describe);

            String second = cli(0, c, "consume", "--group=cli", "--topic=orders", "--session-ms=3000");
            assertTrue(second.contains("(10 records)"), "only the uncommitted rest:\n" + second);

            String offsets = cli(0, c, "group", "offsets", "--group=cli");
            long committed = offsets.lines().filter(l -> l.startsWith("orders")).mapToLong(l -> Long.parseLong(l.split("  ")[2])).sum();
            assertEquals(30, committed, offsets);
            assertTrue(cli(0, c, "group", "describe", "--group=cli").contains("total lag: 0"));
        }
    }

    @Test
    void anUnknownGroupAndBadFlagsAreReportedPlainly(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, false)) {
            assertTrue(cli(0, c, "group", "describe", "--group=nobody").contains("does not exist"));
            assertTrue(cli(0, c, "group", "offsets", "--group=nobody").contains("does not exist"));
            cli(2, c, "group");
            cli(2, c, "group", "explode", "--group=g");
            cli(2, c, "group", "describe");
            cli(2, c, "consume", "--group=g", "--topic=t", "--from=sideways");
            cli(2, c, "consume", "--group=g");
        }
    }

    @Test
    void produceCountConsumeBenchCommitBenchWatchAndTheCoordinatorLine(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, false)) {
            ClusterIntegrationTest.createTopic(c, "orders", 3);
            assertTrue(cli(0, c, "produce", "--topic=orders", "--count=300", "--keys=7", "--value=order").contains("produced 300 records"));

            String manual = cli(0, c, "consume-bench", "--topic=orders", "--expect=300", "--mode=manual");
            assertTrue(manual.contains("mode=manual records=300"), manual);
            String grouped = cli(0, c, "consume-bench", "--topic=orders", "--expect=300", "--mode=group", "--auto-commit=false");
            assertTrue(grouped.contains("mode=group (no commit) records=300"), grouped);
            cli(2, c, "consume-bench", "--topic=orders", "--mode=group");                          // --expect is required
            cli(2, c, "consume-bench", "--topic=orders", "--expect=5", "--mode=sideways");

            String bench = cli(0, c, "group", "commit-bench", "--group=cb", "--topic=orders", "--count=100");
            assertTrue(bench.contains("100 synchronous OFFSET_COMMITs") && bench.contains("p99"), bench);

            String printed = cli(0, c, "consume", "--group=ts", "--topic=orders", "--max=3", "--print-time", "--session-ms=3000");
            assertTrue(printed.lines().filter(l -> l.startsWith("ts=")).count() == 3, printed);

            String describe = cli(0, c, "group", "describe", "--group=cb");
            assertTrue(describe.matches("(?s)coordinator: node [123] localhost:\\d+\\n.*"), describe);

            String watch = cli(0, c, "group", "watch", "--group=cb", "--for-ms=300", "--interval-ms=50");
            assertTrue(watch.startsWith("ts=") && watch.contains("committed="), watch);
        }
    }
}
