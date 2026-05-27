#!/usr/bin/env bash
#
# Usage:  entrypoint.sh <role>
#   role := hbase-master | hbase-regionserver | bash | help
#
set -euo pipefail

ROLE="${1:-help}"

log()  { echo "[hbase-entrypoint][$(date -u +%H:%M:%S)] $*"; }
fail() { log "ERROR: $*"; exit 1; }

wait_for() {
    local host="$1" port="$2"
    log "Waiting for ${host}:${port} ..."
    until nc -z "${host}" "${port}" 2>/dev/null; do
        sleep 2
    done
    log "${host}:${port} is reachable."
}

case "${ROLE}" in
    hbase-master)
        wait_for "${ZOOKEEPER_HOST:-zookeeper}" "${ZOOKEEPER_PORT:-2181}"
        wait_for "${NAMENODE_HOST:-namenode}"   "${NAMENODE_PORT:-9000}"
        exec "${HBASE_HOME}/bin/hbase" master start
        ;;

    hbase-regionserver)
        wait_for "${ZOOKEEPER_HOST:-zookeeper}"  "${ZOOKEEPER_PORT:-2181}"
        wait_for "${HMASTER_HOST:-hbase-master}" "${HMASTER_PORT:-16000}"
        exec "${HBASE_HOME}/bin/hbase" regionserver start
        ;;

    bash|shell)
        exec /bin/bash
        ;;

    help|*)
        cat <<EOF
Usage: docker run ... phoenix-adapters/hbase-phoenix:latest <role>

Roles:
    hbase-master         Run the HBase Master.
    hbase-regionserver   Run an HBase RegionServer.
    bash                 Drop into a shell inside the image.

Versions:
    HBase    ${HBASE_VERSION}-${HBASE_FLAVOR}
    Phoenix  ${PHOENIX_VERSION} (phoenix-hbase-${PHOENIX_HBASE_LINE})
EOF
        [[ "${ROLE}" == "help" ]] && exit 0
        fail "Unknown role: ${ROLE}"
        ;;
esac
