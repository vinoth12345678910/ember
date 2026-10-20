#!/bin/bash
# Soak test: steady produce + 3 group consumers + a gentle nemesis (one fault every 30-60 s) against 3 REAL broker processes
# (or the docker cluster), sampling /metrics every 10 s; then the checker and the trend analysis (heap after GC, threads, fds).
#   scripts/soak.sh [minutes=15] [rate records/s total] [process|docker]
set -eu
HERE="$(cd "$(dirname "$0")/.." && pwd)"
MINUTES=${1:-15}; RATE=${2:-300}; MODE=${3:-process}
JAR=$(ls "$HERE"/ember-chaos/target/ember-chaos-*-all.jar | head -1)
OUT="$HERE/soak-$(date +%H%M%S)"; mkdir -p "$OUT"
export JAVA_OPTS="${SOAK_JAVA_OPTS:--Xmx256m}"
if [ "$MODE" = "docker" ]; then
  docker compose up -d --build --wait
  BOOT=localhost:19092,localhost:19093,localhost:19094; ADMIN=localhost:18081,localhost:18082,localhost:18083
  DRIVER="--driver=docker --container-prefix=ember --network=ember-net"
else
  "$HERE"/scripts/run-cluster.sh 3 19601 "$OUT/cluster" --enable-debug-endpoints --admin-host=127.0.0.1 >/dev/null
  BOOT=localhost:19601,localhost:19602,localhost:19603; ADMIN=localhost:20601,localhost:20602,localhost:20603
  DRIVER="--driver=process --root=$OUT/cluster --run-script=$HERE/scripts/run-cluster.sh --base-port=19601 --broker-flags=--enable-debug-endpoints"
fi
# A tight heap makes the garbage collector run old-generation collections regularly, so "heap after GC" is a meaningful live-set reading
# (with the default multi-GB heap it can stay stale for minutes). Override with SOAK_JAVA_OPTS.
export JAVA_OPTS="${SOAK_JAVA_OPTS:--Xmx256m}"
trap '[ "$MODE" = "docker" ] || "$HERE"/scripts/stop-cluster.sh "$OUT/cluster" >/dev/null 2>&1' EXIT
java -Xmx3g -jar "$JAR" --mode=external $DRIVER --bootstrap=$BOOT --admin=$ADMIN \
     --nemesis=crash,pause --duration=$((MINUTES * 60)) --rate="$RATE" --producers="${PRODUCERS:-8}" --min-gap-ms=15000 --max-gap-ms=30000 \
     --sample-admin=$ADMIN --samples-csv="$OUT/samples.csv" --history="$OUT/history.jsonl" --seed="${SEED:-20261019}" | tee "$OUT/result.txt"
echo "results in $OUT"
