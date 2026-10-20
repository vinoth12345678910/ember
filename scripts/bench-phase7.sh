#!/bin/bash
# Phase 7 benchmarks with REAL broker processes and the REAL CLI / chaos tool. Raw CSVs and logs go to docs/benchmarks/.
#   scripts/bench-phase7.sh [BASE_PORT] [ROOT]        ONLY=ABCDEF selects sections
#  (A) 3 brokers, Raft fsync ON : 4 producers, 100 B and 1 KB values, 3 and 12 partitions, closed loop
#  (B) the same with --unsafe-no-raft-fsync     (C) 1 broker vs 3 brokers
#  (D) fixed-rate latency at 25/50/75% of the measured max (single-record requests)
#  (E) failover: 20 leader kill -9, default Raft timings and election 150-300 ms     (F) catch-up of a broker that missed 100,000 records
set -u
BASE=${1:-19701}; ROOT=${2:-./bench7-data}
HERE="$(cd "$(dirname "$0")/.." && pwd)"
CLIJAR=$(ls "$HERE"/ember-client/target/ember-client-*-all.jar | head -1)
CHAOSJAR=$(ls "$HERE"/ember-chaos/target/ember-chaos-*-all.jar | head -1)
OUT="$HERE/docs/benchmarks"; mkdir -p "$OUT"
want() { [[ "${ONLY:-ABCDEF}" == *$1* ]]; }
cleanup() { "$HERE"/scripts/stop-cluster.sh "$ROOT" >/dev/null 2>&1; }
trap cleanup EXIT

bootstrap() { local n=$1 b=""; for i in $(seq 1 "$n"); do b="$b${b:+,}localhost:$((BASE + i - 1))"; done; echo "$b"; }
admins() { local n=$1 b=""; for i in $(seq 1 "$n"); do b="$b${b:+,}localhost:$((BASE + 1000 + i - 1))"; done; echo "$b"; }
start() {  # brokers flags...
  local n=$1; shift
  "$HERE"/scripts/stop-cluster.sh "$ROOT" >/dev/null 2>&1; rm -rf "$ROOT"
  "$HERE"/scripts/run-cluster.sh "$n" "$BASE" "$ROOT" "$@" >/dev/null || exit 1
  for _ in $(seq 1 100); do java -jar "$CLIJAR" healthcheck --admin="$(admins "$n" | cut -d, -f1)" >/dev/null 2>&1 && break; sleep 0.5; done
  sleep 2
}
bench() {  # label brokers records valueBytes partitions [extra bench flags]
  local label=$1 n=$2 records=$3 vb=$4 parts=$5; shift 5
  local topic="b-$label"
  echo "=== $label: $n broker(s), $parts partitions, ${vb}B values, $records records, 4 producers"
  java -jar "$CLIJAR" bench --bootstrap="$(bootstrap "$n")" --topic="$topic" --partitions="$parts" --producers=4 --records="$records" \
       --value-bytes="$vb" --batch=100 --warmup-records=2000 --csv="$OUT/$label.csv" "$@" 2>&1 | grep -E "^bench:|^measured:|^latency|failed requests|retried|^verify" | tee "$OUT/$label.txt"
}
echo "machine: $(sysctl -n machdep.cpu.brand_string), $(sysctl -n hw.ncpu) cores, $(( $(sysctl -n hw.memsize) / 1073741824 )) GiB RAM, macOS $(sw_vers -productVersion), $(java -version 2>&1 | head -1)"

if want A; then
  echo; echo "##### A: 3 brokers, Raft fsync ON"
  start 3
  for vb in 100 1024; do for parts in 3 12; do
    recs=50000; [ "$vb" = 1024 ] && recs=30000
    bench "A-3brokers-fsync-${vb}B-${parts}p" 3 $recs $vb $parts
  done; done
fi
if want B; then
  echo; echo "##### B: 3 brokers, --unsafe-no-raft-fsync"
  start 3 --unsafe-no-raft-fsync
  for vb in 100 1024; do for parts in 3 12; do
    recs=100000; [ "$vb" = 1024 ] && recs=60000
    bench "B-3brokers-nofsync-${vb}B-${parts}p" 3 $recs $vb $parts
  done; done
fi
if want C; then
  echo; echo "##### C: 1 broker (Raft, fsync ON) vs 3 brokers (A row 100B/3p)"
  start 1
  bench "C-1broker-fsync-100B-3p" 1 50000 100 3
  start 1 --unsafe-no-raft-fsync
  bench "C-1broker-nofsync-100B-3p" 1 100000 100 3
fi
if want D; then
  echo; echo "##### D: fixed-rate latency, 3 brokers fsync ON, single-record requests (batch=1)"
  start 3
  java -jar "$CLIJAR" bench --bootstrap="$(bootstrap 3)" --topic=d-max --partitions=3 --producers=4 --records=4000 --value-bytes=100 --batch=1 --warmup-records=400 2>&1 | grep -E "^measured|^latency" | tee "$OUT/D-max.txt"
  MAX=$(grep -o '= [0-9,]* records/s' "$OUT/D-max.txt" | head -1 | tr -d ', =' | sed 's/records\/s//')
  echo "measured max (closed loop, batch=1, 4 producers): $MAX records/s" | tee -a "$OUT/D-max.txt"
  for pct in 25 50 75; do
    rate=$(( MAX * pct / 100 )); recs=$(( rate * 20 )); [ "$recs" -gt 20000 ] && recs=20000
    echo "--- $pct% of max = $rate records/s, $recs records"
    java -jar "$CLIJAR" bench --bootstrap="$(bootstrap 3)" --topic="d-$pct" --partitions=3 --producers=4 --records="$recs" --value-bytes=100 --batch=1 \
         --warmup-records=$(( recs / 10 )) --rate="$rate" --csv="$OUT/D-rate-$pct.csv" 2>&1 | grep -E "^bench:|^measured|^latency" | tee "$OUT/D-rate-$pct.txt"
  done
fi
if want E; then
  echo; echo "##### E: failover, 20 x kill -9 of the partition leader"
  for variant in "default:" "election150-300:--raft-election-min-ms=150,--raft-election-max-ms=300,--raft-heartbeat-ms=50"; do
    name=${variant%%:*}; flags=${variant#*:}
    start 3 $(echo "$flags" | tr ',' ' ')
    echo "--- timings: $name"
    java -jar "$CHAOSJAR" --bench=failover --kills=20 --driver=process --root="$ROOT" --run-script="$HERE/scripts/run-cluster.sh" --base-port="$BASE" \
         ${flags:+--broker-flags=$flags} --bootstrap="$(bootstrap 3)" --admin="$(admins 3)" 2>&1 | grep -E "^failover" | tee "$OUT/E-failover-$name.txt"
  done
fi
if want F; then
  echo; echo "##### F: restart catch-up of a broker that missed 100,000 records (3 repetitions)"
  : > "$OUT/F-catchup.txt"
  for rep in 1 2 3; do
    start 3
    java -jar "$CHAOSJAR" --bench=catchup --records=100000 --victim=3 --driver=process --root="$ROOT" --run-script="$HERE/scripts/run-cluster.sh" --base-port="$BASE" \
         --bootstrap="$(bootstrap 3)" --admin="$(admins 3)" 2>&1 | grep -E "^catch-up" | tee -a "$OUT/F-catchup.txt"
  done
fi
