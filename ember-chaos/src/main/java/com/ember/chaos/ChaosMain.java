package com.ember.chaos;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The chaos tool.
 *   --mode=inprocess  (default) start real brokers in this JVM and attack them through the test hooks
 *   --mode=external   talk to running brokers; faults via --driver=process (PID files) or --driver=docker
 *   --check-only=FILE re-check a recorded history, run nothing
 * Exit code 0 only if the checker found no violation.
 */
public final class ChaosMain {

    private static final String USAGE = """
            usage: java -jar ember-chaos-all.jar [--mode=inprocess|external] [flags]
              common   : --seed=N --duration=SECONDS --producers=2 --readers=1 --consumers=3 --partitions=3 --rate=RECORDS_PER_SECOND (default: as fast as possible) --queue-workers=N (ack-queue workers, 0 = none; use a --rate so the queue keeps up) --rebalance=eager|cooperative (cooperative: in-process brokers get --group-rebalance=cooperative, consumers rejoin incrementally; for an external cluster start the brokers with that flag yourself)
                         --sample-admin=h:p,h:p,h:p --samples-csv=FILE (scrape /metrics every 10 s; adds a leak-trend verdict to the result)
                         --nemesis=all|crash,pause,isolate,link,slow_fsync,drop,delay,client_pause
                         --history=FILE (write the recorded history) --check-only=FILE (check a history, run nothing)
                         --analyze-samples=FILE [--warmup=120] (re-run the resource-trend analysis on a saved samples CSV)
                         --min-gap-ms=500 --max-gap-ms=1500
              inprocess: --brokers=3 --fsync=true|false --data-dir=DIR
              external : --bootstrap=h:p,h:p,h:p --admin=h:p,h:p,h:p (brokers started with --enable-debug-endpoints)
                         --bench=failover [--kills=20] | --bench=catchup [--records=100000] [--victim=3]   (measure instead of attack)
                         --driver=process --root=DIR --run-script=scripts/run-cluster.sh --base-port=19001 [--broker-flags=a,b]
                         --driver=docker --container-prefix=ember --network=NAME""";

    private ChaosMain() {
    }

    public static void main(String[] args) throws Exception {
        System.exit(run(args));
    }

    static int run(String[] args) throws Exception {
        Map<String, String> flags = new HashMap<>();
        for (String a : args) {
            if (!a.startsWith("--")) {
                System.err.println("expected --name=value, got: " + a + "\n" + USAGE);
                return 2;
            }
            int eq = a.indexOf('=');
            flags.put(eq < 0 ? a.substring(2) : a.substring(2, eq), eq < 0 ? "true" : a.substring(eq + 1));
        }
        if (flags.containsKey("help")) {
            System.out.println(USAGE);
            return 0;
        }
        try {
            if (flags.containsKey("analyze-samples")) {          // re-run the leak analysis on a saved samples.csv
                List<SoakAnalysis.Trend> trends = SoakAnalysis.analyze(SoakAnalysis.readCsv(Path.of(flags.get("analyze-samples"))), Double.parseDouble(flags.getOrDefault("warmup", "120")));
                System.out.print(SoakAnalysis.table(trends));
                System.out.println(SoakAnalysis.allFlat(trends) ? "no leak trend" : "LEAK TREND in at least one metric");
                return SoakAnalysis.allFlat(trends) ? 0 : 1;
            }
            if (flags.containsKey("check-only")) {
                History history = History.read(Path.of(flags.get("check-only")));
                Checker.Result result = Checker.check(history);
                System.out.println("checked " + history.events().size() + " events: " + result.violations().size() + " violation(s); stats " + result.stats());
                result.violations().forEach(v -> System.out.println("  " + v));
                return result.ok() ? 0 : 1;
            }
            if (flags.containsKey("bench")) {
                return runBench(flags);
            }
            long seed = flags.containsKey("seed") ? Long.parseLong(flags.get("seed")) : System.nanoTime() & 0xFFFFFFFFL;
            Path historyFile = flags.containsKey("history") ? Path.of(flags.get("history")) : null;
            ChaosRunner.Config cfg = new ChaosRunner.Config(seed, Long.parseLong(flags.getOrDefault("duration", "20")) * 1000, Integer.parseInt(flags.getOrDefault("producers", "2")),
                    Integer.parseInt(flags.getOrDefault("readers", "1")), Integer.parseInt(flags.getOrDefault("consumers", "3")), Integer.parseInt(flags.getOrDefault("partitions", "3")),
                    Fault.parse(flags.getOrDefault("nemesis", "all")), Long.parseLong(flags.getOrDefault("min-gap-ms", "500")), Long.parseLong(flags.getOrDefault("max-gap-ms", "1500")),
                    historyFile, Double.parseDouble(flags.getOrDefault("rate", "0")),
                    flags.containsKey("sample-admin") ? List.of(flags.get("sample-admin").split(",")) : List.of(),
                    flags.containsKey("samples-csv") ? Path.of(flags.get("samples-csv")) : null, Integer.parseInt(flags.getOrDefault("queue-workers", "0")), "cooperative".equals(flags.get("rebalance")));
            System.out.println("seed " + seed + " (use --seed=" + seed + " to repeat this fault schedule)");
            ChaosRunner.Summary summary;
            if (flags.getOrDefault("mode", "inprocess").equals("external")) {
                summary = runExternal(cfg, flags);
            } else {
                Path dir = flags.containsKey("data-dir") ? Path.of(flags.get("data-dir")) : Files.createTempDirectory("ember-chaos");
                summary = ChaosRunner.runInProcess(cfg, dir, Integer.parseInt(flags.getOrDefault("brokers", "3")), Boolean.parseBoolean(flags.getOrDefault("fsync", "false")));
            }
            System.out.print(summary.table());
            System.out.println("schedule: " + summary.schedule());
            if (historyFile != null) System.out.println("history written to " + historyFile + " (re-check with --check-only=" + historyFile + ")");
            return summary.ok() ? 0 : 1;
        } catch (IllegalArgumentException e) {
            System.err.println("error: " + e.getMessage() + "\n" + USAGE);
            return 2;
        }
    }

    /** --bench=failover [--kills=20] | --bench=catchup [--records=100000] [--victim=3]; always against running brokers (external). */
    private static int runBench(Map<String, String> flags) throws Exception {
        List<String> bootstrap = List.of(required(flags, "bootstrap").split(","));
        List<String> admin = List.of(required(flags, "admin").split(","));
        FaultDriver driver = driverFor(flags, admin.size());
        switch (flags.get("bench")) {
            case "failover" -> {
                List<Long> times = FailoverBench.failover(driver, bootstrap, admin, Integer.parseInt(flags.getOrDefault("kills", "20")), "failover-bench");
                System.out.println("failover times (ms, kill -9 of the leader -> first successful produce): " + times);
                System.out.println("failover: " + FailoverBench.Stats.of(times));
            }
            case "catchup" -> System.out.println("catch-up: " + FailoverBench.catchUp(driver, bootstrap, admin, Integer.parseInt(flags.getOrDefault("victim", "3")),
                    Long.parseLong(flags.getOrDefault("records", "100000")), "catchup-bench"));
            default -> throw new IllegalArgumentException("--bench is failover or catchup");
        }
        return 0;
    }

    private static FaultDriver driverFor(Map<String, String> flags, int brokers) {
        return switch (flags.getOrDefault("driver", "process")) {
            case "docker" -> new DockerFaultDriver(brokers, flags.getOrDefault("container-prefix", "ember"), required(flags, "network"));
            case "process" -> new ProcessFaultDriver(brokers, Path.of(required(flags, "root")), Path.of(flags.getOrDefault("run-script", "scripts/run-cluster.sh")),
                    Integer.parseInt(required(flags, "base-port")), flags.containsKey("broker-flags") ? new ArrayList<>(List.of(flags.get("broker-flags").split(","))) : List.of());
            default -> throw new IllegalArgumentException("--driver is process or docker");
        };
    }

    private static ChaosRunner.Summary runExternal(ChaosRunner.Config cfg, Map<String, String> flags) throws Exception {
        List<String> bootstrap = List.of(required(flags, "bootstrap").split(","));
        List<String> admin = List.of(required(flags, "admin").split(","));
        int brokers = admin.size();
        FaultDriver driver = driverFor(flags, brokers);
        return ChaosRunner.run(cfg, new ChaosRunner.Environment() {
            @Override
            public List<String> bootstrap() {
                return bootstrap;
            }

            @Override
            public FaultDriver driver() {
                return driver;
            }

            @Override
            public boolean awaitReady(long timeoutMs) {
                return InProcessCluster.await(timeoutMs, () -> admin.stream().allMatch(AdminClient::ready));
            }

            @Override
            public Collector collector(String topic, int partitions) {
                return new ExternalCollector(admin, topic, partitions);
            }
        });
    }

    private static String required(Map<String, String> flags, String name) {
        String v = flags.get(name);
        if (v == null) throw new IllegalArgumentException("missing --" + name);
        return v;
    }
}
