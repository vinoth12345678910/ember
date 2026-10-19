#!/bin/bash
# Phase 6 measurements with REAL processes.   scripts/bench-phase6.sh [BASE_PORT] [ROOT] [RUNS]
#  (A) consumer kill -9 -> partitions reassigned / consumption resumed   (B) coordinator kill -9 -> commits available again
#  (C) one group consumer vs the manual consumer, 200,000 records        (D) OFFSET_COMMIT latency, Raft fsync ON vs OFF
set -u
BASE=${1:-19501}; ROOT=${2:-./bench6-data}; RUNS=${3:-5}
HERE="$(cd "$(dirname "$0")/.." && pwd)"
CLIJAR=$(ls "$HERE"/ember-client/target/ember-client-*-all.jar | head -1)
BOOT=localhost:$BASE,localhost:$((BASE+1)),localhost:$((BASE+2))
GROUP=bench; TOPIC=orders
want() { [[ "${ONLY:-ABCD}" == *$1* ]]; }      # ONLY=AC runs just sections A and C
. "$HERE/scripts/lib-groups.sh"
cleanup() { stop_consumers; for f in "$ROOT"/*.bgpid; do [ -f "$f" ] && kill -9 "$(cat "$f")" 2>/dev/null; done; "$HERE"/scripts/stop-cluster.sh "$ROOT" >/dev/null 2>&1; }
trap cleanup EXIT
stats() {  # prints min / median / max of the numbers on stdin
  sort -n | awk '{a[NR]=$1} END{ if (NR==0) {print "no data"; exit} m=(NR%2)?a[(NR+1)/2]:(a[NR/2]+a[NR/2+1])/2; printf "min %d  median %d  max %d  (n=%d)\n", a[1], m, a[NR], NR }'
}

echo "machine: $(sysctl -n machdep.cpu.brand_string 2>/dev/null), $(sysctl -n hw.ncpu 2>/dev/null) cores, $(( $(sysctl -n hw.memsize 2>/dev/null || echo 0) / 1073741824 )) GiB RAM, macOS $(sw_vers -productVersion 2>/dev/null), $(java -version 2>&1 | head -1)"

# ------------------------------------------------------------------ A and B
rm -rf "$ROOT"; mkdir -p "$ROOT"
"$HERE"/scripts/run-cluster.sh 3 "$BASE" "$ROOT" >/dev/null || exit 1
sleep 2
cli create-topic --topic=$TOPIC --partitions=6 >/dev/null
cli produce --topic=$TOPIC --count=3000 --keys=100 --value=seed >/dev/null
for n in 1 2 3; do start_consumer $n 3000; done
wait_for "3 members owning 6 partitions" 60 stable_with 3 6 || exit 1
wait_for "lag 0" 90 lag_is_zero || exit 1

if want A; then
echo; echo "=== A: consumer kill -9 -> reassignment (session timeout 3000 ms, heartbeat 1000 ms, expiry tick 750 ms)"
A_REASSIGN=(); A_RESUME=()
for run in $(seq 1 "$RUNS"); do
  refresh_describe
  DEAD=$(echo "$describe_cache" | sed -n 's/.*client c3 .*assigned \(.*\)$/\1/p' | tr ',' ' ' | sed 's/'$TOPIC'-//g')
  nohup java -jar "$CLIJAR" group watch --group=$GROUP --interval-ms=50 --for-ms=20000 --bootstrap="$BOOT" > "$ROOT/watch.log" 2>&1 &
  echo $! > "$ROOT/watch.bgpid"; sleep 1
  T_KILL=$(now_ms); kill -9 "$(cat "$ROOT/consumer-3.pid")"
  # records for the dead consumer's partitions arrive only AFTER the kill, so they can reach nobody until the partitions move
  (java -jar "$CLIJAR" produce --topic=$TOPIC --count=300 --keys=100 --value="r$run" --bootstrap="$BOOT" >/dev/null 2>&1 &)
  sleep 12
  kill -9 "$(cat "$ROOT/watch.bgpid")" 2>/dev/null
  REASSIGN=$(awk -v k="$T_KILL" '{ts=substr($1,4)+0} ts>=k && /members=2 owned=6/ {print ts-k; exit}' "$ROOT/watch.log")
  RESUME=$(cat "$ROOT"/consumer-1.log "$ROOT"/consumer-2.log | awk -v k="$T_KILL" -v dead="$DEAD" -v v="value=r$run-" '
      BEGIN{n=split(dead,d," "); for(i=1;i<=n;i++) isdead[d[i]]=1}
      { ts=substr($1,4)+0; split($3,p,"="); if (ts>=k && (p[2] in isdead) && index($0,v)>0) { if (!best || ts<best) best=ts } }
      END{ if (best) print best-k }')
  echo "run $run: c3 owned [$DEAD]; reassigned after ${REASSIGN:-?} ms; first record from its old partitions delivered to another consumer after ${RESUME:-?} ms"
  [ -n "${REASSIGN:-}" ] && A_REASSIGN+=("$REASSIGN"); [ -n "${RESUME:-}" ] && A_RESUME+=("$RESUME")
  start_consumer 3 3000
  wait_for "3 members owning 6 partitions" 60 stable_with 3 6 || exit 1
  wait_for "lag 0" 90 lag_is_zero || exit 1
done
echo "A reassigned:       $(printf '%s\n' "${A_REASSIGN[@]}" | stats)"
echo "A consumption resumed: $(printf '%s\n' "${A_RESUME[@]}" | stats)"
fi
stop_consumers

if want B; then
echo; echo "=== B: coordinator broker kill -9 -> next successful OFFSET_COMMIT (probe commits every 20 ms)"
B_GAP=(); B_FIRST=()
for run in $(seq 1 "$RUNS"); do
  refresh_describe >/dev/null; COORD=$(cli group describe --group=$GROUP | sed -n 's/^coordinator: node \([0-9]*\).*/\1/p')
  nohup java -jar "$CLIJAR" group commit-probe --group="probe$run" --topic=$TOPIC --for-ms=16000 --interval-ms=20 --bootstrap="$BOOT" > "$ROOT/probe.log" 2>&1 &
  echo $! > "$ROOT/probe.bgpid"
  wait_for "probe ready" 40 grep -q ready "$ROOT/probe.log" || exit 1
  sleep 2
  T_KILL=$(now_ms); kill -9 "$(cat "$ROOT/broker-$COORD.pid")"
  sleep 15
  kill -9 "$(cat "$ROOT/probe.bgpid")" 2>/dev/null
  GAP=$(awk '/ ok /{ts=substr($1,4)+0; if (prev && ts-prev>g) g=ts-prev; prev=ts} END{print g+0}' "$ROOT/probe.log")
  FIRST=$(awk -v k="$T_KILL" '/ ok /{ts=substr($1,4)+0; split($3,t,"="); took=t[2]+0; if (ts-took>k) {print ts-k; exit}}' "$ROOT/probe.log")
  echo "run $run: killed coordinator broker $COORD; first commit STARTED after the kill succeeded ${FIRST:-?} ms later; longest gap between two successful commits ${GAP} ms"
  [ -n "${FIRST:-}" ] && B_FIRST+=("$FIRST"); B_GAP+=("$GAP")
  "$HERE"/scripts/run-cluster.sh 3 "$BASE" "$ROOT" >/dev/null; sleep 6
done
echo "B first commit after kill: $(printf '%s\n' "${B_FIRST[@]}" | stats)"
echo "B longest commit gap:      $(printf '%s\n' "${B_GAP[@]}" | stats)"
fi

# ------------------------------------------------------------------ C
if want C; then
echo; echo "=== C: reading 200,000 x 100 B records from a 6-partition topic on 3 brokers"
cli bench --topic=b6 --partitions=6 --producers=4 --records=200000 --value-bytes=100 --batch=500 2>&1 | grep -E '^produce:|^verify'
for rep in 1 2 3; do
  echo "-- repetition $rep"
  cli consume-bench --topic=b6 --expect=200000 --mode=manual
  cli consume-bench --topic=b6 --expect=200000 --mode=group --auto-commit=true --commit-interval-ms=5000
  cli consume-bench --topic=b6 --expect=200000 --mode=group --auto-commit=false
  cli consume-bench --topic=b6 --expect=200000 --mode=group --auto-commit=true --commit-interval-ms=100
done
fi
"$HERE"/scripts/stop-cluster.sh "$ROOT" >/dev/null

# ------------------------------------------------------------------ D
want D && for mode in "fsync ON:" "fsync OFF:--unsafe-no-raft-fsync"; do
  label=${mode%%:*}; flag=${mode#*:}
  rm -rf "$ROOT"; mkdir -p "$ROOT"
  "$HERE"/scripts/run-cluster.sh 3 "$BASE" "$ROOT" $flag >/dev/null || exit 1
  sleep 2; cli create-topic --topic=cb --partitions=1 >/dev/null
  echo; echo "=== D ($label): 2,000 synchronous OFFSET_COMMITs"
  cli group commit-bench --group=commitbench --topic=cb --count=2000
  "$HERE"/scripts/stop-cluster.sh "$ROOT" >/dev/null
done
