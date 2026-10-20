# Step 7 - Running it for real (Docker)

## What we are doing
Bring the cluster up with Docker Compose, smoke-test it with the real CLI from the host, kill a leader container while a producer runs, run the external chaos tool for 5 minutes with the Docker fault driver and the checker, and look at the metrics path (Prometheus and Grafana).

Environment: Docker Desktop 29.3.1 (Compose v5.1.1) on an Apple M4 laptop; the Docker VM got 10 CPUs and 8 GB; each broker container is limited to 768 MB. Docker Desktop was **not running** at the start of the session; I started it (and quit it again at the end).

## Why it is done this way
Everything before this step ran inside one JVM or as plain processes. A container is a different environment: its own network namespace (names, ports), its own memory limit, its own signal handling, a volume for data. The things Docker can break are exactly the things this step checks: the entrypoint (does `docker stop` reach the JVM), the advertised addresses (can a host client use the cluster), the healthcheck, the heap sizing under a memory limit.

## What happened (real output, shortened)

**Start.** `docker compose up -d --build --wait` built the image (Maven stage about 56 s with a warm cache) and returned when all three containers were `healthy` (that is our own `ember healthcheck` command running inside each container).

**Smoke test with the real CLI from the host** (`scripts/demo.sh --kill`):
```
NODE  ADMIN            STATE   UPTIME  CONTROLLER  COORDINATOR
1     localhost:18081  ready   6s      2           3
2     localhost:18082  ready   6s      2           3
3     localhost:18083  ready   6s      2           3
created topic orders with 3 partition(s)      produced 300 records to orders
orders  0  leader 2  replicas 1,2,3  end 70   |  1: leader 2  end 120  |  2: leader 1  end 110
```
**Kill a leader container with a producer running** (`docker kill ember2`, the leader of partition 0 and the controller): the producer (600 more records) finished without error; `cluster status` showed node 2 UNREACHABLE and new leaders for everything it had led (`orders-0` -> node 1, `orders-1` -> node 3); `consume --from-beginning` returned **900 of 900** records; `docker start ember2` and a few seconds later node 2 was `ready` again as a follower in every group.

**External chaos run, 5 minutes, Docker driver** (`scripts/chaos-docker.sh 300 20261019`, seed 20261019):
```
chaos run  seed=20261019  duration=327s
  produce operations     : 102840 acknowledged, 0 unknown outcome, 0 failed
  reads / polls / commits: 2302 / 12863 / 1626
  faults injected        : 18  isolate=5  pause=4  crash_restart=6  client_pause=3
  group deliveries       : 110428 (102840 distinct, 7588 duplicates = 6.87%)
  replica comparisons    : 5
  VIOLATIONS             : 0
```
The Docker driver can `docker kill`/`start` (6 crashes), `docker pause`/`unpause` (4 freezes), `docker network disconnect`/`connect` (5 isolations), and the workload froze 3 consumers' heartbeats. **Link blocking, slow fsync, message drop and message delay cannot be done from outside a container** (they need root-level traffic tools or our in-process hooks), so those four fault types are exercised by the in-process chaos suite (3 seeds x 20 s in the default build, 3 x 120 s with `-Pcluster-long`), and this external run uses the other four. "ALL nemesis types" is therefore met across the two runners, not in a single external run; this is listed under Deviations.

**Metrics path** (`docker compose --profile observability up`):
```
targets:  node 1 http://ember1:8080/metrics up    node 2 ... up    node 3 ... up
ember_raft_role{group="__meta-0"}:  node 1 = 0, node 2 = 2, node 3 = 0        (node 2 leads the topic list)
grafana:  dashboard "Ember cluster" (uid ember-overview) is provisioned; datasource health: "Successfully queried the Prometheus API."
docker kill ember2 (the controller), 20 s later:
  ember_raft_leader_changes_total{group="__meta-0"}: node 1 = 2, node 3 = 2     (it moved)
  ember_raft_role{group="__meta-0"}:  node 1 = 2, node 3 = 0                    (node 1 is the new controller)
  target node 2: down
docker start ember2, 20 s later: node 2 role = 0 (follower) and its target is up again
```
I verified the dashboard through Grafana's and Prometheus' HTTP APIs (provisioning, datasource health, the queries behind the panels); I did not take screenshots.

## Alternatives I rejected and why
* Using the in-process runner only: it cannot test the entrypoint, the signals, the healthcheck or the address advertising.
* Faking link faults with `iptables` inside containers: needs `NET_ADMIN` and extra tools in the image; out of scope.

## What went wrong / surprises
* The first checker alarm of the project (a "zombie commit") was a mistake in my own *recording*; see step 4.
* A consumer subscribed right after its topic was created could get an empty assignment for ever (a coordinator whose copy of the topic list lagged). Found by the chaos CLI test, fixed in the coordinator; see step 4.
* `demo.sh` printed 70 lines of Docker build log; fixed with `--quiet-build`.

## Tradeoffs and honest limits
One laptop, one Docker VM; "isolate" in Docker disconnects the whole container from the network (clients reach it no more either), unlike the in-process hook, which cuts only broker-to-broker traffic. The chaos run produced 0 unknown outcomes because the client library retries through every failure; the INFO path of the checker is exercised by its unit tests, not by this run.
