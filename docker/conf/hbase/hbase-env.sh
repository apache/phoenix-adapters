#!/usr/bin/env bash
export JAVA_HOME=${JAVA_HOME:-/opt/java/openjdk}
export HBASE_MANAGES_ZK=false
export HBASE_LOG_DIR=/var/log/hbase
export HBASE_PID_DIR=/var/run/hbase

# Sized to fit the whole stack in ~4 GB of Docker memory.
export HBASE_HEAPSIZE=1G
export HBASE_OFFHEAPSIZE=256m

# Strip JDK11-specific GC flags HBase ships with; we run on JDK8.
export HBASE_OPTS="-XX:+UseG1GC -XX:+UnlockExperimentalVMOptions"
export HBASE_MASTER_OPTS="${HBASE_OPTS} -Xms256m"
export HBASE_REGIONSERVER_OPTS="${HBASE_OPTS} -Xms512m"

unset HBASE_JSHELL_ARGS
