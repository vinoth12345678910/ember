#!/bin/bash
# Stops the brokers started by run-cluster.sh (graceful SIGTERM; the shutdown hook closes everything cleanly).
#   scripts/stop-cluster.sh [DATA_ROOT]
ROOT=${1:-./cluster-data}
for pidfile in "$ROOT"/broker-*.pid; do
  [ -f "$pidfile" ] || continue
  pid=$(cat "$pidfile")
  if kill -0 "$pid" 2>/dev/null; then kill -TERM "$pid"; fi
done
for pidfile in "$ROOT"/broker-*.pid; do
  [ -f "$pidfile" ] || continue
  pid=$(cat "$pidfile")
  for _ in $(seq 1 100); do kill -0 "$pid" 2>/dev/null || break; sleep 0.1; done
  kill -0 "$pid" 2>/dev/null && { echo "pid $pid did not stop; killing"; kill -9 "$pid"; }
  rm -f "$pidfile"
done
echo "cluster stopped"
