#!/bin/bash
# Runs the chaos tool against the docker compose cluster (crash = docker kill/start, pause = docker pause, isolate = docker network
# disconnect). Start the cluster first: docker compose up -d --build --wait
#   scripts/chaos-docker.sh [duration-seconds=300] [seed]
set -eu
HERE="$(cd "$(dirname "$0")/.." && pwd)"
JAR=$(ls "$HERE"/ember-chaos/target/ember-chaos-*-all.jar 2>/dev/null | head -1 || true)
[ -n "$JAR" ] || { echo "building (mvn -q package -DskipTests)"; (cd "$HERE" && mvn -q package -DskipTests); JAR=$(ls "$HERE"/ember-chaos/target/ember-chaos-*-all.jar | head -1); }
DURATION=${1:-300}
SEED=${2:-$RANDOM$RANDOM}
HISTORY="$HERE/chaos-history-$SEED.jsonl"
java -jar "$JAR" --mode=external --driver=docker --container-prefix=ember --network=ember-net \
     --bootstrap=localhost:19092,localhost:19093,localhost:19094 --admin=localhost:18081,localhost:18082,localhost:18083 \
     --nemesis=crash,pause,isolate,client_pause --duration="$DURATION" --seed="$SEED" --history="$HISTORY" \
     --min-gap-ms=3000 --max-gap-ms=9000
