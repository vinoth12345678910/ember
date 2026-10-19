#!/bin/bash
# Real-process failover demo: 3 shaded broker processes, the real CLI, kill -9 of a partition leader while producing.
#   scripts/demo-failover.sh [BASE_PORT] [DATA_ROOT]
# Prints the evidence; leaves no processes running.
set -u
BASE=${1:-19001}; ROOT=${2:-./demo-data}
HERE="$(cd "$(dirname "$0")/.." && pwd)"
CLIJAR=$(ls "$HERE"/ember-client/target/ember-client-*-all.jar | head -1)
cli() { java -jar "$CLIJAR" "$@" --bootstrap="localhost:$BASE,localhost:$((BASE+1)),localhost:$((BASE+2))"; }
now() { perl -MTime::HiRes=time -e 'printf "%.3f", time'; }
rm -rf "$ROOT"; mkdir -p "$ROOT"
trap '"$HERE"/scripts/stop-cluster.sh "$ROOT" >/dev/null 2>&1' EXIT

echo "### 1. start 3 real broker processes (fsync ON, default Raft timings)"
"$HERE"/scripts/run-cluster.sh 3 "$BASE" "$ROOT"
echo; echo "### 2. create a topic with 3 partitions through the CLI"
sleep 2
cli create-topic --topic=orders --partitions=3
echo; echo "### 3. produce 200 keyed records (user-0..user-9), one CLI process each"
: > "$ROOT/acked.txt"
for i in $(seq 0 199); do
  out=$(cli produce --topic=orders --key=user-$((i % 10)) --value=order-$i 2>&1) && echo "order-$i $out" >> "$ROOT/acked.txt"
done
echo "acknowledged: $(wc -l < "$ROOT/acked.txt") of 200"
echo; echo "### 4. metadata: who leads what"
cli metadata --topic=orders
LEADER=$(cli metadata --topic=orders | awk '$1=="orders" && $2==0 {print $3}')
PIDFILE="$ROOT/broker-$LEADER.pid"
echo; echo "### 5. a CLI producer loop keeps running while we kill -9 the leader of partition 0 (broker $LEADER, pid $(cat "$PIDFILE"))"
: > "$ROOT/loop.txt"
( for i in $(seq 1000 1300); do
    t0=$(now)
    out=$(cli produce --topic=orders --key=user-$((i % 10)) --value=order-$i 2>&1) && echo "OK $(now) order-$i $out" >> "$ROOT/loop.txt" || echo "FAIL $(now) order-$i $out" >> "$ROOT/loop.txt"
  done ) &
LOOP=$!
sleep 6
KILL_AT=$(now)
kill -9 "$(cat "$PIDFILE")"
echo "kill -9 sent at $KILL_AT"
wait $LOOP
echo "loop finished: $(grep -c '^OK' "$ROOT/loop.txt") ok, $(grep -c '^FAIL' "$ROOT/loop.txt") failed"
FIRST_OK_P0=$(awk -v k="$KILL_AT" '$1=="OK" && $2>k && $0 ~ /partition=0 /{print $2; exit}' "$ROOT/loop.txt")
echo "time from the kill to the first successful produce to partition 0: $(perl -e "printf '%.2f', $FIRST_OK_P0 - $KILL_AT") s"
echo; echo "### 6. consume everything and verify that nothing acknowledged is missing"
cli consume --topic=orders --from-beginning > "$ROOT/consumed.txt"
tail -1 "$ROOT/consumed.txt"
cat "$ROOT/acked.txt" <(grep '^OK' "$ROOT/loop.txt" | awk '{print $3, $4, $5}') > "$ROOT/all-acked.txt"
MISSING=0
while read -r value rest; do grep -q "value=$value\$" "$ROOT/consumed.txt" || { echo "MISSING: $value"; MISSING=$((MISSING+1)); }; done < "$ROOT/all-acked.txt"
echo "acknowledged records checked: $(wc -l < "$ROOT/all-acked.txt"), missing: $MISSING"
echo; echo "### 7. restart the killed broker and watch it catch up (each broker's own end offsets)"
"$HERE"/scripts/run-cluster.sh 3 "$BASE" "$ROOT"
sleep 8
for i in 1 2 3; do
  echo "--- view of broker $i"
  java -jar "$CLIJAR" metadata --topic=orders --bootstrap="localhost:$((BASE+i-1))" | awk '$1=="orders"'
done
echo; echo "### 8. stop with SIGTERM"
"$HERE"/scripts/stop-cluster.sh "$ROOT"
grep -h "shut down cleanly" "$ROOT"/broker-*.log | sort | uniq -c
trap - EXIT
