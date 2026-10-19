#!/bin/bash
# Phase 6 real-process demo: 3 shaded broker processes, 3 REAL `consume --group` processes, kill -9 of a consumer, then of the
# coordinator broker, and a fresh consumer that does not re-read committed records.   scripts/demo-groups.sh [BASE_PORT] [ROOT]
set -u
BASE=${1:-19401}; ROOT=${2:-./demo6-data}
HERE="$(cd "$(dirname "$0")/.." && pwd)"
CLIJAR=$(ls "$HERE"/ember-client/target/ember-client-*-all.jar | head -1)
BOOT=localhost:$BASE,localhost:$((BASE+1)),localhost:$((BASE+2))
GROUP=demo; TOPIC=orders
. "$HERE/scripts/lib-groups.sh"
rm -rf "$ROOT"; mkdir -p "$ROOT"
cleanup() { stop_consumers; "$HERE"/scripts/stop-cluster.sh "$ROOT" >/dev/null 2>&1; }
trap cleanup EXIT

echo "### 1. three real broker processes (Raft fsync ON)"
"$HERE"/scripts/run-cluster.sh 3 "$BASE" "$ROOT" || exit 1
sleep 2
echo; echo "### 2. a 6-partition topic and 3,000 keyed records"
cli create-topic --topic=$TOPIC --partitions=6
cli produce --topic=$TOPIC --count=3000 --keys=100 --value=order
cli metadata --topic=$TOPIC | awk '$1=="'$TOPIC'"'

echo; echo "### 3. three consumer PROCESSES in group '$GROUP' (session timeout 3 s)"
for n in 1 2 3; do start_consumer $n 3000; done
wait_for "3 members owning 6 partitions" 60 stable_with 3 6 || exit 1
wait_for "lag 0" 90 lag_is_zero || exit 1
cli group describe --group=$GROUP
echo "distinct values delivered to the group so far: $(distinct_values) of 3000"

echo; echo "### 4. kill -9 consumer c3 (no goodbye); 600 more records arrive meanwhile"
DEAD_PARTS=$(echo "$describe_cache" | sed -n 's/.*client c3 .*assigned \(.*\)$/\1/p' | tr ',' ' ')
echo "c3 owned: $DEAD_PARTS"
cli produce --topic=$TOPIC --count=600 --keys=100 --value=late
T_KILL=$(now_ms); kill -9 "$(cat "$ROOT/consumer-3.pid")"
echo "kill -9 at $T_KILL"
wait_for "2 members owning 6 partitions" 60 stable_with 2 6 || exit 1
T_REASSIGNED=$(now_ms)
echo "reassigned (seen by polling describe) $((T_REASSIGNED - T_KILL)) ms after the kill (includes ~0.5 s of polling overhead)"
wait_for "lag 0" 90 lag_is_zero || exit 1
cli group describe --group=$GROUP
echo "distinct values delivered: $(distinct_values) of 3600 expected"

echo; echo "### 5. kill -9 the COORDINATOR broker while consumers run"
refresh_describe; COORD=$(field coordinator)
echo "coordinator is broker $COORD"
kill -9 "$(cat "$ROOT/broker-$COORD.pid")"
cli produce --topic=$TOPIC --count=300 --keys=100 --value=after --bootstrap="$BOOT"
wait_for "lag 0 after the coordinator died" 90 lag_is_zero || exit 1
cli group describe --group=$GROUP | sed -n '1,3p;/TOPIC/,$p'
echo "distinct values delivered: $(distinct_values) of 3900 expected"
"$HERE"/scripts/run-cluster.sh 3 "$BASE" "$ROOT" >/dev/null; sleep 5

echo; echo "### 6. stop the consumers gracefully (commit + leave), start a FRESH consumer in the same group"
stop_consumers
cli group offsets --group=$GROUP | head -8
echo "--- fresh consumer, same group, nothing new produced:"
cli consume --group=$GROUP --topic=$TOPIC --session-ms=3000
echo "--- 10 new records, then another fresh consumer:"
cli produce --topic=$TOPIC --count=10 --keys=100 --value=final
cli consume --group=$GROUP --topic=$TOPIC --session-ms=3000 | tail -3
echo; echo "### 7. stop the cluster"
