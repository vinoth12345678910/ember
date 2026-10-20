# Resume bullets for Ember

**Simple English. Every number below was measured in this repository. The line under each bullet says where to find the proof.**

## 5 bullets

- Built **Ember**, a Kafka-style message broker, from zero in Java 21. It has partitions, a Raft-replicated log on three brokers, consumer groups, and its own metrics library. The only dependency is Netty.
  <!-- proof: README.md (project structure); pom.xml modules; ember-metrics, ember-raft, ember-chaos are written in this repo -->

- Made the cluster survive a leader crash with **no acknowledged message lost**. I killed the partition leader with `kill -9` 20 times. A new leader accepted writes again after a **median of 770 ms** (559 ms with faster election timers).
  <!-- proof: docs/benchmarks/E-failover-default.txt and docs/benchmarks/E-failover-election150-300.txt; docs/phase7/step9-benchmarks.md (table E) -->

- Wrote a **chaos testing tool with an independent checker**. A 5-minute run on a Docker cluster did 18 faults (container kills, freezes, network cuts) with **102,840 acknowledged writes and 0 violations** of nine safety rules.
  <!-- proof: docs/phase7/step7-running-it-for-real.md ("External chaos run"); scripts/chaos-docker.sh -->

- **Tested the checker itself** by putting one bug at a time into a good history (lost write, split-brain read, duplicate offset, zombie commit, a replica that differs by one byte, and more). All 13 injected-bug tests are caught with the right error code. This showed that a green result means something.
  <!-- proof: ember-chaos/src/test/java/com/ember/chaos/CheckerMutationTest.java; docs/phase7/design.md part 3 -->

- Used the tools to find and fix **real bugs in my own code**: a readiness endpoint that said "ready" too early, a consumer that could skip messages, a consumer group that could be left without partitions, and a benchmark that timed its own shutdown. Each fix has a regression test.
  <!-- proof: docs/phase7/design.md part 7; ember-client/src/test/java/com/ember/client/GroupConsumerFetchFailureTest.java; docs/ember-part8-notebooklm.md ("Bugs and surprises") -->

## One more bullet (observability and operations)

- Added Prometheus metrics, health checks, a `/status` page, a Grafana dashboard, a 3-broker Docker Compose setup, and a runbook. A fixed-rate benchmark mode (it avoids coordinated omission) showed the latency knee: p99 went from **49 ms at 50% load to 143 ms at 75% load**.
  <!-- proof: docs/benchmarks/D-rate-50.txt and D-rate-75.txt; docker-compose.yml; docs/OPERATIONS.md -->

## LinkedIn project blurb

**Ember: a Kafka-style message broker I built from scratch (Java 21).** I wanted to learn how replicated logs really work, so I wrote everything myself: the storage engine, the network protocol, Raft consensus, consumer groups and the metrics. Every partition is copied to three brokers. A message is "saved" only when most brokers have it on disk. When I kill the leader, a new one takes over in under a second and no saved message is lost. To prove it, I built a chaos tool that breaks the cluster on purpose, plus a checker that reads what the clients saw and checks nine safety rules. I tested the checker too, by injecting bugs. The project has a Docker cluster, Prometheus and Grafana, a runbook, and honest benchmarks with their limits written down. Stack: Java 21, Netty, Maven, Docker.

## 3 numbers you can say out loud in an interview

1. **"About 0.8 seconds."** After I kill the leader with `kill -9`, writes work again in about 0.8 seconds. That is the median of 20 kills (770 ms).
   <!-- proof: docs/benchmarks/E-failover-default.txt -->
2. **"A hundred thousand messages, eighteen faults, zero violations."** In a 5-minute chaos run on Docker, there were 102,840 acknowledged writes, 18 injected faults, and the checker found no problem.
   <!-- proof: docs/phase7/step7-running-it-for-real.md -->
3. **"Two and a half seconds."** A broker that missed 100,000 messages was ready and had all of them about 2.5 seconds after I restarted it (2.2 to 2.8 seconds in three runs).
   <!-- proof: docs/benchmarks/F-catchup.txt -->

**Say this too, so no one thinks you hide it:** "The speed numbers are from one laptop, with all three brokers on the same disk. They show how the system behaves, not how fast it could be on real servers."
