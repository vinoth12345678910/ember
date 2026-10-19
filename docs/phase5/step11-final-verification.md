# Step 11 - Final verification

## What we are building
The last gate: `mvn clean verify` from the root, the `cluster-long` profile once, and a check that no process is left running.

## Why it is designed this way
`clean` proves nothing is stale in `target/`; `verify` also builds the shaded jars the scripts use; the long profile adds the 60-second chaos run and the Raft long-run suites that the default build skips to stay fast.

## Alternatives I rejected and why
Running only `mvn test`: skips jar packaging, which is how a broken shade configuration would sneak through.

## What goes wrong if you get it wrong
A green default run with a red slow suite means the release is not trustworthy; both must pass.

## Tradeoffs and honest limits
The slow chaos run is random in timing: a pass is evidence, not proof.

## How it is tested
Results (Apple M4 laptop):

| Module | Tests |
|---|---|
| ember-storage | 55 |
| ember-raft | 182 |
| ember-protocol | 83 |
| ember-replication | 42 |
| ember-client | 22 |
| ember-broker | 154 |
| **Total (default)** | **538**, `mvn clean verify` in 2 min 46 s |

`mvn verify -Pcluster-long`: 539 tests in total (the broker module runs 155, the extra one being the 60-second chaos run), 3 min 49 s, green. The Raft module's own `raft-long` profile is separate and was not part of this run.

Cluster chaos (real brokers, real sockets): default 10-second variant passed **5 times in a row** (seeds 1335965369, 2320694439, 369982005, 1079639325, 2010938687; 25,000-29,500 acknowledged records each, 7-10 chaos events each), plus the first run (seed 1884955436); the 60-second variant passed **3 times** (seeds 3117440383, 1292288647, 3385202665; 152,000-169,000 acknowledged records, 49-55 chaos events each). In every run every acknowledged record was present, in order, and all replicas were byte-identical. The extra stored records (5-31 per run) are unacknowledged or retried sends, as the design allows.
