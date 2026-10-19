#!/bin/bash
# Phase 5 benchmarks with REAL broker processes and the REAL CLI. Usage: scripts/bench-phase5.sh [DATA_ROOT]
HERE="$(cd "$(dirname "$0")/.." && pwd)"; ROOT=${1:-./bench-data}
CLIJAR=$(ls "$HERE"/ember-client/target/ember-client-*-all.jar | head -1)
now() { perl -MTime::HiRes=time -e 'printf "%.3f", time'; }
run_bench() {  # label n extra-broker-flags...
  label=$1; n=$2; shift 2
  rm -rf "$ROOT"; "$HERE"/scripts/run-cluster.sh "$n" 19201 "$ROOT" "$@" >/dev/null || exit 1
  boot=""; for i in $(seq 1 "$n"); do boot="$boot${boot:+,}localhost:$((19200 + i))"; done; sleep 3
  echo "=== $label"; java -jar "$CLIJAR" bench --bootstrap="$boot" --partitions=4 --producers=4 --value-bytes=100 --records=50000 2>&1 | grep -E "^produce:|^retried|^verify" 
  "$HERE"/scripts/stop-cluster.sh "$ROOT" >/dev/null
}
run_bench "A1 single broker with Raft, fsync ON" 1
run_bench "A2 3 brokers, fsync ON" 3
run_bench "A3 3 brokers, --unsafe-no-raft-fsync" 3 --unsafe-no-raft-fsync

