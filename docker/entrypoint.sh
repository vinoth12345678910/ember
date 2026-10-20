#!/bin/sh
# Turns environment variables into broker flags, then EXECs java so that SIGTERM reaches the JVM and the broker shuts down cleanly.
#   EMBER_NODE_ID      (required)  this broker's id
#   EMBER_CLUSTER      (required)  1@host:port[/advertisedHost:port],2@...   the same list on every broker
#   EMBER_ADVERTISE    (optional)  host:port clients should use for THIS broker (overrides the third part of its own entry)
#   EMBER_DATA_DIR     (default /data)
#   EMBER_ADMIN_PORT   (default 8080)    EMBER_ADMIN_HOST (default 0.0.0.0 inside the container)
#   EMBER_OPTS         (optional)  extra broker flags, e.g. "--enable-debug-endpoints --raft-election-min-ms=150"
#   EMBER_JAVA_OPTS    (optional)  extra JVM flags
set -eu
: "${EMBER_NODE_ID:?EMBER_NODE_ID is required}"
: "${EMBER_CLUSTER:?EMBER_CLUSTER is required}"
DATA="${EMBER_DATA_DIR:-/data}"
set -- --node-id="$EMBER_NODE_ID" --cluster="$EMBER_CLUSTER" --data-dir="$DATA" \
       --admin-port="${EMBER_ADMIN_PORT:-8080}" --admin-host="${EMBER_ADMIN_HOST:-0.0.0.0}"
if [ -n "${EMBER_ADVERTISE:-}" ]; then
  set -- "$@" --advertise="$EMBER_ADVERTISE"
fi
# MaxRAMPercentage: the heap follows the container's memory limit; the direct-memory cap bounds Netty's off-heap buffers;
# ExitOnOutOfMemoryError: a broker that ran out of memory must die and restart, not limp along half-working.
# shellcheck disable=SC2086
exec java -XX:MaxRAMPercentage=60 -XX:MaxDirectMemorySize=256m -XX:+ExitOnOutOfMemoryError ${EMBER_JAVA_OPTS:-} \
     -jar /app/ember-broker.jar "$@" ${EMBER_OPTS:-}
