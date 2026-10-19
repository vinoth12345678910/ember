# Step 11 - Final verification

## What we are building
The last gate: `mvn clean verify` from the root, the `cluster-long` profile once, and a check that no process is left running.

## Why it is designed this way
`clean` proves nothing is stale in `target/`; `verify` also builds the shaded jars the scripts use; the long profile adds the two 60-second chaos runs (produce chaos and group chaos) that the default build skips to stay fast.

## Alternatives I rejected and why
Running only `mvn test`: skips jar packaging, which is how a broken shade configuration would slip through.

## What goes wrong if you get it wrong
A green default run with a red slow suite means the release is not trustworthy; both must pass.

## Tradeoffs and honest limits
The chaos runs are random in timing: a pass is evidence, not proof. In one of four full runs the fsync-ON cluster test (`withFsyncOnAcknowledgedDataSurvivesACrashOfEveryBroker`) took 108 s instead of about 2 s; it did not reproduce in two reruns or alone, and its cause is unknown.

## How it is tested
Results (Apple M4 laptop):

| Module | Tests (Phase 5 -> Phase 6) |
|---|---|
| ember-storage | 55 |
| ember-raft | 182 |
| ember-protocol | 83 -> 97 (+14: `Phase6CodecTest`) |
| ember-replication | 42 -> 90 (+48: `AssignorTest` 9, `GroupCommandsTest` 5, `GroupStateMachineTest` 12, `GroupCoordinatorTest` 22) |
| ember-client | 22 |
| ember-broker | 154 -> 175 (+21: `GroupApisOverTheWireTest` 6, `EmberGroupConsumerTest` 4, `GroupCliTest` 3, `GroupScenariosTest` 7, `GroupChaosTest` default 1) |
| **Total (default)** | **621**; `mvn clean verify` in **3 min 24 s** |

`mvn verify -Pcluster-long`: 623 tests (broker module 177: the extra two are the 60-second produce-chaos and group-chaos runs), 5 min 33 s, green.

Group chaos (3 real brokers, 6 partitions, 2 producers, 3 group consumers, random crashes / cut links / killed consumers / frozen heartbeats): the 10-second variant passed **5 times in a row** (seeds 354470591, 2371327370, 343746769, 1958353464, 2749179368; 5,700-6,500 acknowledged records each; duplicate rate 4.9%-15.3%), the 60-second variant passed **3 times** (seeds 3591989813, 2040061411, 3544844221; 28,000-31,000 acknowledged records, 53-56 faults each; duplicate rate 7.5%-9.7%). In every run every acknowledged record was delivered at least once, committed offsets never passed the log end, every partition had one owner and the `__groups` state was identical on all brokers.
