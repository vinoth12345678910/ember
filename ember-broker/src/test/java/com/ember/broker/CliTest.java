package com.ember.broker;

import com.ember.client.EmberCli;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Path;
import java.time.Clock;
import java.util.function.BooleanSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

/** The CLI commands, run in-process against a real broker. */
class CliTest {

    private record Result(int code, String out, String err) {
    }

    private static Result run(BooleanSupplier stop, String... args) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        int code = EmberCli.run(args, new PrintStream(out), new PrintStream(err), stop);
        return new Result(code, out.toString(), err.toString());
    }

    private static Result run(String... args) {
        return run(() -> true, args);
    }

    private static BrokerServer start(Path dir) throws Exception {
        return ServerFixture.start(ServerFixture.config(dir), Clock.systemUTC());
    }

    private static final Pattern PRODUCED = Pattern.compile("partition=(\\d+) offset=(\\d+)");

    @Test
    void createTopicAndTheDuplicateError(@TempDir Path dir) throws Exception {
        try (BrokerServer server = start(dir)) {
            String port = "--port=" + server.port();
            Result created = run("create-topic", port, "--topic=orders", "--partitions=4");
            assertEquals(0, created.code, created.err);
            assertTrue(created.out.contains("created topic orders with 4 partition(s)"), created.out);

            Result again = run("create-topic", port, "--topic=orders", "--partitions=4");
            assertEquals(1, again.code);
            assertTrue(again.err.contains("TOPIC_ALREADY_EXISTS"), again.err);

            assertEquals(2, run("create-topic", port, "--topic=x").code);                  // --partitions missing
            assertEquals(2, run("create-topic", port, "--topic=x", "--partitions=abc").code);
            assertEquals(1, run("create-topic", port, "--topic=x", "--partitions=0").code); // broker says INVALID_REQUEST
        }
    }

    @Test
    void keyedProducePrintsPartitionAndOffsetAndTheSameKeyStaysPut(@TempDir Path dir) throws Exception {
        try (BrokerServer server = start(dir)) {
            String port = "--port=" + server.port();
            run("create-topic", port, "--topic=t", "--partitions=4");
            Matcher first = PRODUCED.matcher(run("produce", port, "--topic=t", "--key=user-7", "--value=a").out);
            Matcher second = PRODUCED.matcher(run("produce", port, "--topic=t", "--key=user-7", "--value=b").out);
            assertTrue(first.find() && second.find());
            assertEquals(first.group(1), second.group(1));                                  // same key, same partition
            assertEquals(Long.parseLong(first.group(2)) + 1, Long.parseLong(second.group(2)));
        }
    }

    @Test
    void produceAutoCreatesWithThePartitionsFlag(@TempDir Path dir) throws Exception {
        try (BrokerServer server = start(dir)) {
            String port = "--port=" + server.port();
            assertEquals(0, run("produce", port, "--topic=auto", "--value=v", "--partitions=5").code);
            assertEquals(5, server.logManager().topic("auto").partitionCount());
        }
    }

    @Test
    void fetchReadsOneChosenPartition(@TempDir Path dir) throws Exception {
        try (BrokerServer server = start(dir)) {
            String port = "--port=" + server.port();
            run("create-topic", port, "--topic=t", "--partitions=3");
            Matcher m = PRODUCED.matcher(run("produce", port, "--topic=t", "--key=k1", "--value=hello").out);
            assertTrue(m.find());
            String partition = m.group(1);

            Result hit = run("fetch", port, "--topic=t", "--partition=" + partition, "--offset=0");
            assertEquals(0, hit.code, hit.err);
            assertTrue(hit.out.contains("offset=0") && hit.out.contains("key=k1 value=hello"), hit.out);

            String other = partition.equals("0") ? "1" : "0";
            assertTrue(run("fetch", port, "--topic=t", "--partition=" + other).out.contains("(0 records"));
            Result missing = run("fetch", port, "--topic=t", "--partition=9");
            assertEquals(1, missing.code);
            assertTrue(missing.err.contains("UNKNOWN_TOPIC_OR_PARTITION"), missing.err);
        }
    }

    @Test
    void consumeFromBeginningPrintsEverythingAndStopsWhenCaughtUp(@TempDir Path dir) throws Exception {
        try (BrokerServer server = start(dir)) {
            String port = "--port=" + server.port();
            run("create-topic", port, "--topic=t", "--partitions=3");
            for (int i = 0; i < 12; i++) {
                run("produce", port, "--topic=t", "--key=k" + i, "--value=v" + i);
            }
            Result all = run("consume", port, "--topic=t", "--from-beginning");
            assertEquals(0, all.code, all.err);
            for (int i = 0; i < 12; i++) {
                assertTrue(all.out.contains("key=k" + i + " value=v" + i), "missing record " + i);
            }
            assertTrue(all.out.contains("(12 records)"), all.out);

            Result latest = run("consume", port, "--topic=t");                              // default: only new records
            assertTrue(latest.out.contains("(0 records)"), latest.out);
            assertEquals(1, run("consume", port, "--topic=ghost", "--from-beginning").code);
        }
    }

    @Test
    void consumeFollowKeepsPollingUntilToldToStop(@TempDir Path dir) throws Exception {
        try (BrokerServer server = start(dir)) {
            String port = "--port=" + server.port();
            run("create-topic", port, "--topic=t", "--partitions=2");
            run("produce", port, "--topic=t", "--key=a", "--value=before");
            int[] calls = {0};
            BooleanSupplier stop = () -> {
                if (calls[0]++ == 0) {                                                      // first time caught up: not allowed to stop yet
                    run("produce", port, "--topic=t", "--key=b", "--value=while-following");
                    return false;
                }
                return true;
            };
            Result r = run(stop, "consume", port, "--topic=t", "--from-beginning", "--follow");
            assertTrue(r.out.contains("value=before"), r.out);
            assertTrue(r.out.contains("value=while-following"), r.out);                     // arrived after the first catch-up
            assertTrue(r.out.contains("(2 records)"), r.out);
        }
    }

    @Test
    void metadataPrintsATable(@TempDir Path dir) throws Exception {
        try (BrokerServer server = start(dir)) {
            String port = "--port=" + server.port();
            assertTrue(run("metadata", port).out.contains("(no topics)"));
            run("create-topic", port, "--topic=alpha", "--partitions=2");
            run("create-topic", port, "--topic=beta", "--partitions=1");
            Await.until(10_000, "every partition to have a leader", () -> !run("metadata", port).out.contains(" -1 "));
            Result all = run("metadata", port);
            assertTrue(all.out.contains("TOPIC") && all.out.contains("LEADER") && all.out.contains("START OFFSET"), all.out);
            assertTrue(all.out.matches("(?s).*alpha\\s+0\\s+0\\s+0\\s+0.*alpha\\s+1\\s+0\\s+0\\s+0.*beta\\s+0\\s+0\\s+0\\s+0.*"), all.out);
            Result one = run("metadata", port, "--topic=beta");
            assertTrue(one.out.contains("beta") && !one.out.contains("alpha"), one.out);
            assertTrue(run("metadata", port, "--topic=nope").out.contains("(no topics)"));
        }
    }

    @Test
    void benchWithSeveralProducersVerifiesItsOwnResult(@TempDir Path dir) throws Exception {
        try (BrokerServer server = start(dir)) {
            String port = "--port=" + server.port();
            Result b = run("bench", port, "--topic=b1", "--partitions=4", "--producers=3", "--records=3000",
                    "--value-bytes=50", "--batch=100");
            assertEquals(0, b.code, b.err);
            assertTrue(b.out.contains("produce:") && b.out.contains("read:") && b.out.contains("records/s"), b.out);
            assertTrue(b.out.contains("verify: OK (3000 records"), b.out);
            assertEquals(4, server.logManager().topic("b1").partitionCount());
            long total = 0;
            for (var log : server.logManager().topic("b1").partitions()) total += log.nextOffset();
            assertEquals(3000, total);

            // running it again on the same topic reads back only the NEW records
            Result again = run("bench", port, "--topic=b1", "--partitions=4", "--producers=2", "--records=1000", "--batch=100");
            assertTrue(again.out.contains("verify: OK (1000 records"), again.out);
        }
    }

    @Test
    void errorsGiveNonZeroExitCodes(@TempDir Path dir) throws Exception {
        try (BrokerServer server = start(dir)) {
            String port = "--port=" + server.port();
            assertEquals(1, run("fetch", port, "--topic=missing").code);
            assertEquals(2, run("produce", port, "--topic=t").code);                        // missing --value
            assertEquals(2, run("explode", port).code);
            assertEquals(2, run().code);
            assertEquals(2, run("fetch", port, "--topic=t", "--offset=abc").code);
            assertEquals(2, run("bench", port, "--records=0").code);
            assertEquals(2, run("produce", port, "--topic", "--value=v").code);                // --topic without a value
        }
        assertEquals(1, run("metadata", "--port=1").code);                                  // nobody listens there
    }
}
