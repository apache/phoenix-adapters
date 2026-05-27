#!/usr/bin/env bash
set -euo pipefail

log() { echo "[phoenix-adapters][$(date -u +%H:%M:%S)] $*"; }

# Guard against accidental reintroduction of the 3.4.x hadoop client jars.
# Dockerfile.phoenix-adapters strips them because they reference
# org.apache.hadoop.fs.WithErasureCoding (only present in hadoop-common
# 3.4+), which poisons the client JVM via FileSystem ServiceLoader the
# first time HBase returns a remote exception. If anyone re-adds them,
# fail fast with a clear pointer instead of dying mid-bootstrap.
shopt -s nullglob
stray=( "${PHOENIX_ADAPTERS_HOME}/lib/hadoop-hdfs-"*.jar \
        "${PHOENIX_ADAPTERS_HOME}/lib/hadoop-hdfs-client-"*.jar \
        "${PHOENIX_ADAPTERS_HOME}/lib/hadoop-yarn-"*.jar \
        "${PHOENIX_ADAPTERS_HOME}/lib/hadoop-mapreduce-client-"*.jar \
        "${PHOENIX_ADAPTERS_HOME}/lib/hadoop-distcp-"*.jar )
shopt -u nullglob
if [[ ${#stray[@]} -gt 0 ]]; then
    log "ERROR: assembly contains hadoop 3.4.x jars that must be stripped:"
    for j in "${stray[@]}"; do log "  - ${j##*/}"; done
    log "See the 'rm -f hadoop-hdfs-*' block in docker/Dockerfile.phoenix-adapters."
    exit 1
fi

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
