# Step 11 - Final verification

## What we are building
The last gate: `mvn clean verify` from the repository root, then `mvn verify -Pcluster-long` (the slow chaos tests), then a check that nothing is left running.

## Why it is designed this way
Every earlier step ran `mvn -q test`; only a clean build also proves the jars are built and that nothing depends on stale `target/` files.

## Alternatives I rejected and why
Trusting the last incremental run: the last edits touched ChaosRunner, Nemesis and SoakAnalysis after the previous full run.

## What goes wrong if you get it wrong
Stale classes can make tests pass that a clean checkout fails.

## Tradeoffs and honest limits
The `cluster-long` run was done for the `ember-chaos` module only (`-pl ember-chaos -am -Dtest=ChaosRunTest`), because it is the only module with slow-tagged tests. The other modules were verified in the default run just before.

## How it is tested (results)
* `mvn clean verify`: BUILD SUCCESS, 747 tests, 0 failures: metrics 15, storage 55, raft 182, protocol 97, replication 92, client 39, broker 215, chaos 52. About 5.5 minutes on the Apple M4 machine.
* `mvn verify -Pcluster-long -pl ember-chaos -am -Dtest=ChaosRunTest`: 8 tests, 0 failures, 453 s (the 3 default seeds x 20 s, 3 slow seeds x 120 s, majority loss, CLI tool).
