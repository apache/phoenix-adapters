#!/usr/bin/env bash
set -euo pipefail

log() { echo "[phoenix-adapters][$(date -u +%H:%M:%S)] $*"; }

wait_for() {
    local host="$1" port="$2"
    log "Waiting for ${host}:${port} ..."
    until nc -z "${host}" "${port}" 2>/dev/null; do
        sleep 2
    done
    log "${host}:${port} is reachable."
}

zk_quorum="${ZOO_KEEPER_QUORUM:-zookeeper:2181}"
zk_host="${zk_quorum%%:*}"
zk_port="${zk_quorum##*:}"
[[ "${zk_host}" == "${zk_port}" ]] && zk_port=2181

wait_for "${zk_host}" "${zk_port}"
wait_for "${HBASE_MASTER_HOST:-hbase-master}" "${HBASE_MASTER_PORT:-16000}"

# Give the master a moment to finish initialising hbase:meta before the
# first Phoenix connection bootstraps SYSTEM.* tables.
sleep "${PHOENIX_BOOTSTRAP_SLEEP_SECONDS:-5}"

log "Starting Phoenix Adapters REST on :${PHOENIX_REST_PORT} (ZK=${zk_quorum})"

CLASSPATH="${PHOENIX_ADAPTERS_CONF_DIR}:${PHOENIX_ADAPTERS_HOME}/lib/*"

exec "${JAVA_HOME}/bin/java" \
    -Dproc_rest \
    -XX:+UseG1GC \
    -XX:OnOutOfMemoryError="kill -9 %p" \
    -XX:+HeapDumpOnOutOfMemoryError \
    -XX:HeapDumpPath="${PHOENIX_ADAPTERS_LOG_DIR}" \
    -Dphoenix.adapters.log.dir="${PHOENIX_ADAPTERS_LOG_DIR}" \
    -Dlog4j2.configurationFile="file:${PHOENIX_ADAPTERS_CONF_DIR}/log4j2.properties" \
    -cp "${CLASSPATH}" \
    org.apache.phoenix.ddb.rest.RESTServer \
        start \
        -p "${PHOENIX_REST_PORT}" \
        -z "${zk_quorum}"
