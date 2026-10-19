#!/bin/bash
# Helpers shared by demo-groups.sh and bench-phase6.sh (source this file).
# Needs: HERE (repo root), CLIJAR, BOOT (bootstrap list), ROOT (work dir), GROUP, TOPIC.
cli() { java -jar "$CLIJAR" "$@" --bootstrap="$BOOT"; }
now_ms() { perl -MTime::HiRes=time -e 'printf "%d", time*1000'; }

# start_consumer N [session-ms]: a real `consume --group` process; pid in consumer-N.pid, output (with timestamps) in consumer-N.log
start_consumer() {
  local n=$1 session=${2:-3000}
  nohup java -jar "$CLIJAR" consume --group="$GROUP" --topic="$TOPIC" --follow --session-ms="$session" \
      --commit-interval-ms=500 --print-time --client-id="c$n" --bootstrap="$BOOT" > "$ROOT/consumer-$n.log" 2>&1 &
  echo $! > "$ROOT/consumer-$n.pid"
}

# group_field NAME: members | owned | lag | generation   (from `group describe`)
describe_cache=""
refresh_describe() { describe_cache=$(cli group describe --group="$GROUP" 2>&1); }
field() {
  case "$1" in
    members)    echo "$describe_cache" | sed -n 's/^group .* members \([0-9]*\).*/\1/p' | head -1 ;;
    generation) echo "$describe_cache" | sed -n 's/^group .* generation \([0-9]*\).*/\1/p' | head -1 ;;
    lag)        echo "$describe_cache" | sed -n 's/^total lag: \([0-9]*\).*/\1/p' | head -1 ;;
    owned)      echo "$describe_cache" | awk '/^'"$TOPIC"' /{ if ($NF != "(unowned)") n++ } END{print n+0}' ;;
    coordinator) echo "$describe_cache" | sed -n 's/^coordinator: node \([0-9]*\).*/\1/p' | head -1 ;;
  esac
}

# wait_for "description" timeout-seconds command...   (polls every 0.5 s; returns 1 on timeout)
wait_for() {
  local what=$1 timeout=$2; shift 2
  local deadline=$(( $(date +%s) + timeout ))
  until "$@"; do
    [ "$(date +%s)" -ge "$deadline" ] && { echo "TIMEOUT waiting for: $what" >&2; return 1; }
    sleep 0.5
  done
}

stable_with() {  # members owned
  refresh_describe
  [ "$(field members)" = "$1" ] && [ "$(field owned)" = "$2" ]
}
lag_is_zero() { refresh_describe; [ "$(field lag)" = "0" ]; }

# distinct_values: how many different values the running/finished consumers printed
distinct_values() { cat "$ROOT"/consumer-*.log 2>/dev/null | grep -o 'value=[^ ]*' | sort -u | wc -l | tr -d ' '; }

stop_consumers() {
  for f in "$ROOT"/consumer-*.pid; do
    [ -f "$f" ] || continue
    kill -TERM "$(cat "$f")" 2>/dev/null
  done
  for f in "$ROOT"/consumer-*.pid; do
    [ -f "$f" ] || continue
    for _ in $(seq 1 100); do kill -0 "$(cat "$f")" 2>/dev/null || break; sleep 0.1; done
    kill -9 "$(cat "$f")" 2>/dev/null
    rm -f "$f"
  done
}
