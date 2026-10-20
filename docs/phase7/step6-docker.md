# Step 6 - Docker: a 3-broker cluster with one command

## What we are building
* `Dockerfile` (repo root): two stages. Stage 1 (`maven:3-eclipse-temurin-21`) builds the shaded broker and CLI jars with the tests skipped (they run in `mvn clean verify`). Stage 2 (`eclipse-temurin:21-jre`) holds only the two jars and `docker/entrypoint.sh`, runs as the non-root user `ember`, keeps its data in `VOLUME /data`.
* `docker/entrypoint.sh`: turns environment variables (`EMBER_NODE_ID`, `EMBER_CLUSTER`, `EMBER_ADVERTISE`, `EMBER_DATA_DIR=/data`, `EMBER_ADMIN_PORT`, `EMBER_OPTS`, `EMBER_JAVA_OPTS`) into broker flags and **execs** `java -XX:MaxRAMPercentage=60 -XX:MaxDirectMemorySize=256m -XX:+ExitOnOutOfMemoryError ...`.
* `HEALTHCHECK`: `java -jar /app/ember-cli.jar healthcheck --admin=localhost:8080`. The runtime image has no `curl`.
* `docker-compose.yml`: services `ember1..3` on one network (`ember-net`), container ports 9092 (data) and 8080 (admin), host ports 19092/19093/19094 and 18081/18082/18083, named volumes, `mem_limit: 768m`, `restart: "no"`.
  `EMBER_CLUSTER=1@ember1:9092/localhost:19092,2@ember2:9092/localhost:19093,3@ember3:9092/localhost:19094`.
* Profile `observability`: Prometheus (scrapes the three admin ports every 5 s) and Grafana with a provisioned datasource and one dashboard (`docker/grafana/dashboards/ember-dashboard.json`, 12 panels: produce rate and p99, Raft role per node, leader changes, commit index, peer queue depth and drops, log end offsets, group lag, JVM heap, file descriptors and threads, fsync p99, apply p99).
* `scripts/demo.sh`, `scripts/chaos-docker.sh`, `scripts/soak.sh`.

## Why it is designed this way
**exec, because of SIGTERM.** If the entrypoint ran `java` as a child of a shell, `docker stop` would send SIGTERM to the shell, which would not forward it; Docker would wait 10 seconds and then `kill -9` the broker, so every "graceful" shutdown would be a crash. With `exec` the JVM *is* PID 1's process, receives SIGTERM, and our shutdown hook closes the groups and fsyncs the logs.

**The JVM flags are about containers.** `MaxRAMPercentage=60` makes the heap follow the container's memory limit (768 MB gives a heap of about 460 MB; the rest is for Netty's direct buffers, thread stacks, metaspace and the page cache that the log files need). `MaxDirectMemorySize=256m` caps off-heap buffers explicitly. `ExitOnOutOfMemoryError` makes an out-of-memory broker die and be restarted by whoever supervises it instead of limping along with a dead thread. The Java code only *logs* what it was given (heap max, processors, free disk space at start); limits live in the entrypoint, not in Java.

**Advertised addresses, the compose way.** Inside the network the brokers reach each other as `ember2:9092`. Your laptop cannot resolve `ember2`; it can reach `localhost:19093`. So each broker advertises its **host-mapped** address to clients (the third part of its `--cluster` entry) and keeps using the container names between brokers. Consequence: clients must run on the host. A client inside another container would be told `localhost:19093`, which is itself.

**`restart: "no"`.** The chaos tool decides when a broker dies and when it returns; a supervisor that restarts it behind our back would hide the very thing we are measuring.

**Non-root user, unauthenticated admin port.** The broker never needs root. The admin endpoint is plain HTTP without authentication; inside the container it must listen on `0.0.0.0` (Docker maps the port), so the compose file publishes it on host ports you should not expose to an untrusted network. A broker warns about this at start.

## Alternatives I rejected and why
* One Docker image per role / a Helm chart / Kubernetes manifests: explicitly out of scope.
* `restart: unless-stopped`: see above.
* Running the broker as PID 1 through `tini` or a shell wrapper: `exec java` already makes the JVM the main process; only zombie reaping would be missing, and the broker spawns no children.
* Baking Prometheus rules into the image: alert rules are documented as text in `docs/OPERATIONS.md`.

## What goes wrong if you get it wrong
Forgetting `exec` turns `docker stop` into `kill -9` after 10 s. A heap set to the full container memory gets the broker OOM-killed by the kernel (the JVM heap is not the only memory). Advertising `ember2:9092` to host clients gives "bootstrap works, produce hangs". Using the advertised addresses between brokers breaks the cluster (the same test as in step 5).

## Tradeoffs and honest limits
No TLS or authentication anywhere. Data volumes are named Docker volumes (`docker compose down -v` deletes them). Cluster membership is fixed at three; there is no way to add a fourth broker without recreating the Raft groups (known limitation since Phase 5). Only the Docker driver faults crash / pause / isolate: no link, disk or message faults (those need root-level tools or the in-process hooks).

## How it is tested
`docker compose config` validates the YAML. Step 7 runs it for real: `docker compose up --wait`, CLI smoke tests from the host, `docker kill` of the leader with a producer running, and the external chaos run with the Docker driver for 5 minutes with the checker.
