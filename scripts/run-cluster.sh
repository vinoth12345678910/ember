#!/bin/bash
# Starts N local Ember brokers (default 3) as separate processes with separate data dirs and ports.
#   scripts/run-cluster.sh [N] [BASE_PORT] [DATA_ROOT] [extra broker flags...]
#   e.g. scripts/run-cluster.sh 3 19001 ./cluster-data --unsafe-no-raft-fsync
# Writes DATA_ROOT/broker-<i>.pid and DATA_ROOT/broker-<i>.log. Needs: mvn -q package -DskipTests
set -e
N=${1:-3}; BASE=${2:-19001}; ROOT=${3:-./cluster-data}; shift 3 2>/dev/null || shift $# 2>/dev/null || true
EXTRA=("$@")
HERE="$(cd "$(dirname "$0")/.." && pwd)"
JAR=$(ls "$HERE"/ember-broker/target/ember-broker-*-all.jar 2>/dev/null | head -1)
[ -f "$JAR" ] || { echo "broker jar not found: run 'mvn -q package -DskipTests' first" >&2; exit 1; }
mkdir -p "$ROOT"
CLUSTER=""
for i in $(seq 1 "$N"); do CLUSTER="$CLUSTER${CLUSTER:+,}$i@localhost:$((BASE + i - 1))"; done
for i in $(seq 1 "$N"); do
  if [ -f "$ROOT/broker-$i.pid" ] && kill -0 "$(cat "$ROOT/broker-$i.pid")" 2>/dev/null; then
    echo "broker $i already running (pid $(cat "$ROOT/broker-$i.pid"))"; continue
  fi
  nohup java ${JAVA_OPTS:-} -jar "$JAR" --node-id="$i" --cluster="$CLUSTER" --data-dir="$ROOT/data-$i" "${EXTRA[@]}" \
      > "$ROOT/broker-$i.log" 2>&1 &
  echo $! > "$ROOT/broker-$i.pid"
done
for i in $(seq 1 "$N"); do
  for _ in $(seq 1 100); do grep -q "Ember broker started" "$ROOT/broker-$i.log" 2>/dev/null && break; sleep 0.1; done
  grep -q "Ember broker started" "$ROOT/broker-$i.log" || { echo "broker $i did not start; see $ROOT/broker-$i.log" >&2; cat "$ROOT/broker-$i.log" >&2; exit 1; }
done
echo "cluster of $N brokers started: bootstrap=localhost:$BASE (ports $BASE..$((BASE + N - 1))), logs and pids in $ROOT"
