#!/bin/bash
# One-command demo of the docker compose cluster.
#   scripts/demo.sh            start 3 brokers, create a topic, produce, show leaders and cluster status
#   scripts/demo.sh --kill     ... then also kill the leader of partition 0 while producing, and show the recovery
#   scripts/demo.sh --down     stop and remove the containers (add -v to also delete the data volumes)
set -eu
HERE="$(cd "$(dirname "$0")/.." && pwd)"
cd "$HERE"
if [ "${1:-}" = "--down" ]; then docker compose --profile observability down "${2:-}"; exit 0; fi
CLIJAR=$(ls "$HERE"/ember-client/target/ember-client-*-all.jar 2>/dev/null | head -1 || true)
[ -n "$CLIJAR" ] || { echo "building the CLI jar first (mvn -q package -DskipTests)"; mvn -q package -DskipTests; CLIJAR=$(ls "$HERE"/ember-client/target/ember-client-*-all.jar | head -1); }
BOOT=localhost:19092,localhost:19093,localhost:19094
ADMIN=localhost:18081,localhost:18082,localhost:18083
cli() { java -jar "$CLIJAR" "$@"; }

echo "### 1. start the 3-broker cluster (docker compose up --wait)"
docker compose up -d --build --wait --quiet-build 2>&1 | grep -v "^#" | grep -E "Healthy|Started|Error|error" || true
echo; echo "### 2. cluster status (read from the admin ports)"
cli cluster status --admin=$ADMIN
echo; echo "### 3. create a topic with 3 partitions and produce 300 keyed records"
cli create-topic --topic=orders --partitions=3 --bootstrap=$BOOT
cli produce --topic=orders --count=300 --keys=30 --value=order --bootstrap=$BOOT
echo; echo "### 4. who leads what"
cli metadata --topic=orders --bootstrap=$BOOT
if [ "${1:-}" = "--kill" ]; then
  LEADER=$(cli metadata --topic=orders --bootstrap=$BOOT | awk '$1=="orders" && $2==0 {print $3}')
  echo; echo "### 5. kill the leader of partition 0 (broker $LEADER) while a producer runs"
  ( cli produce --topic=orders --count=600 --keys=30 --value=during --bootstrap=$BOOT --retries=40 & ) 
  sleep 0.5
  docker kill "ember$LEADER" >/dev/null
  sleep 6
  echo "--- cluster status with broker $LEADER dead:"
  cli cluster status --admin=$ADMIN
  echo; echo "--- every record that was acknowledged is still readable:"
  cli consume --topic=orders --from-beginning --bootstrap=$BOOT | tail -1
  echo; echo "### 6. start broker $LEADER again; it catches up by itself"
  docker start "ember$LEADER" >/dev/null
  for _ in $(seq 1 60); do cli cluster status --admin=$ADMIN | grep -q "NOT READY\|UNREACHABLE" || break; sleep 1; done
  cli cluster status --admin=$ADMIN
fi
echo; echo "Done. Metrics:  curl -s localhost:18081/metrics | grep ember_raft_role     Stop:  scripts/demo.sh --down"
