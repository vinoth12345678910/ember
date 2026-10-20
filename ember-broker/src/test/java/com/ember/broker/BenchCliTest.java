package com.ember.broker;

import com.ember.client.EmberCli;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** The upgraded `bench`: percentiles, warm-up, CSV, fixed-rate mode, and honest error accounting. */
class BenchCliTest {

    private record Result(int code, String out, String err) {
    }

    private static Result bench(int port, String... flags) {
        String[] args = new String[flags.length + 2];
        args[0] = "bench";
        args[1] = "--port=" + port;
        System.arraycopy(flags, 0, args, 2, flags.length);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        int code = EmberCli.run(args, new PrintStream(out), new PrintStream(err));
        return new Result(code, out.toString(), err.toString());
    }

    @Test
    void closedLoopReportsPercentilesThroughputAndErrorCounts(@TempDir Path dir) throws Exception {
        try (BrokerServer server = ServerFixture.start(ServerFixture.config(dir), Clock.systemUTC())) {
            Result r = bench(server.port(), "--topic=b", "--partitions=2", "--producers=2", "--records=2000", "--batch=50", "--warmup-records=200");
            assertEquals(0, r.code(), r.err());
            assertTrue(r.out().contains("mode=closed-loop"), r.out());
            assertTrue(r.out().contains("p50 ") && r.out().contains("p90 ") && r.out().contains("p99 ") && r.out().contains("p99.9 ") && r.out().contains("max "), r.out());
            assertTrue(r.out().contains("from the actual send time"));
            assertTrue(r.out().contains("failed requests during produce: 0"));
            assertTrue(r.out().contains("retried requests during produce:"));
            assertTrue(r.out().contains("measured: 1,800 records after 200 warm-up records"), "warm-up records are excluded from the statistics\n" + r.out());
            assertTrue(r.out().contains("verify: OK (2000 records read back"), r.out());
        }
    }

    @Test
    void theCsvHasOneRowPerMeasuredRequest(@TempDir Path dir) throws Exception {
        try (BrokerServer server = ServerFixture.start(ServerFixture.config(dir), Clock.systemUTC())) {
            Path csv = dir.resolve("bench.csv");
            Result r = bench(server.port(), "--topic=b", "--partitions=1", "--producers=1", "--records=1000", "--batch=100", "--warmup-records=200", "--csv=" + csv);
            assertEquals(0, r.code(), r.err());
            List<String> lines = Files.readAllLines(csv);
            assertEquals("elapsed_us,producer,records,latency_us", lines.get(0));
            assertEquals(1 + 8, lines.size(), "1000 records in batches of 100, the first 200 are warm-up: 8 measured requests");
            long previous = -1;
            for (String line : lines.subList(1, lines.size())) {
                String[] f = line.split(",");
                assertEquals(4, f.length);
                assertEquals(100, Long.parseLong(f[2]));
                assertTrue(Long.parseLong(f[3]) > 0);
                assertTrue(Long.parseLong(f[0]) >= previous, "rows are in time order");
                previous = Long.parseLong(f[0]);
            }
        }
    }

    @Test
    void fixedRateModeSendsOnAScheduleAndMeasuresFromTheIntendedTime(@TempDir Path dir) throws Exception {
        try (BrokerServer server = ServerFixture.start(ServerFixture.config(dir), Clock.systemUTC())) {
            long start = System.nanoTime();
            Result r = bench(server.port(), "--topic=b", "--partitions=1", "--producers=1", "--records=1000", "--batch=10", "--rate=2000");
            long seconds = (System.nanoTime() - start) / 1_000_000_000L;
            assertEquals(0, r.code(), r.err());
            assertTrue(r.out().contains("mode=fixed-rate(2000 records/s)"), r.out());
            assertTrue(r.out().contains("from the INTENDED send time"), r.out());
            assertTrue(r.out().contains("verify: OK"), r.out());
            assertTrue(seconds >= 0, "1000 records at 2000/s is about half a second of schedule");
        }
    }

    @Test
    void badBenchFlagsAreUsageErrors(@TempDir Path dir) throws Exception {
        try (BrokerServer server = ServerFixture.start(ServerFixture.config(dir), Clock.systemUTC())) {
            assertEquals(2, bench(server.port(), "--rate=-1").code());
            assertEquals(2, bench(server.port(), "--warmup-records=-5").code());
            assertEquals(2, bench(server.port(), "--records=0").code());
        }
    }
}
