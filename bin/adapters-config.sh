#
#/**
# * Licensed to the Apache Software Foundation (ASF) under one
# * or more contributor license agreements.  See the NOTICE file
# * distributed with this work for additional information
# * regarding copyright ownership.  The ASF licenses this file
# * to you under the Apache License, Version 2.0 (the
# * "License"); you may not use this file except in compliance
# * with the License.  You may obtain a copy of the License at
# *
# *     http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# */

# resolve links - "${BASH_SOURCE-$0}" may be a softlink

this="${BASH_SOURCE-$0}"
while [ -h "$this" ]; do
  ls=`ls -ld "$this"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '.*/.*' > /dev/null; then
    this="$link"
  else
    this=`dirname "$this"`/"$link"
  fi
done

# convert relative path to absolute path
bin=`dirname "$this"`
script=`basename "$this"`
bin=`cd "$bin">/dev/null; pwd`
this="$bin/$script"

if [ -z "$PHOENIX_ADAPTERS_HOME" ]; then
  bin_dir=$(cd "$bin" && pwd)
  export PHOENIX_ADAPTERS_HOME=$(dirname "$bin_dir")
fi

echo "PHOENIX_ADAPTERS_HOME is set to: $PHOENIX_ADAPTERS_HOME"

if [ -z "$PHOENIX_ADAPTERS_CONF_DIR" ]; then
  export PHOENIX_ADAPTERS_CONF_DIR="$PHOENIX_ADAPTERS_HOME/conf"
fi

if [ -z "$PHOENIX_ADAPTERS_LOG_DIR" ]; then
  export PHOENIX_ADAPTERS_LOG_DIR="$PHOENIX_ADAPTERS_HOME/logs"
fi

if [ -z "$PHOENIX_ADAPTERS_PID_DIR" ]; then
  export PHOENIX_ADAPTERS_PID_DIR="$PHOENIX_ADAPTERS_HOME/pid"
fi

export GREP="${GREP-grep}"
export SED="${SED-sed}"

# Newer versions of glibc use an arena memory allocator that causes virtual
# memory usage to explode. Tune the variable down to prevent vmem explosion.
export MALLOC_ARENA_MAX=${MALLOC_ARENA_MAX:-4}

# Now having JAVA_HOME defined is required
if [ -z "$JAVA_HOME" ]; then
  cat 1>&2 <<EOF
+======================================================================+
|                    Error: JAVA_HOME is not set                       |
+----------------------------------------------------------------------+
| Please download the latest Sun JDK from the Sun Java web site        |
|     > http://www.oracle.com/technetwork/java/javase/downloads        |
|                                                                      |
| Phoenix adapters requires Java 8 or later.                               |
+======================================================================+
EOF
  exit 1
fi

function read_java_version() {
  # Avoid calling java repeatedly
  if [ -z "$read_java_version_cached" ]; then
    properties="$("${JAVA_HOME}/bin/java" -XshowSettings:properties -version 2>&1)"
    read_java_version_cached="$(echo "${properties}" | "${GREP}" java.runtime.version | head -1 | "${SED}" -e 's/.* = \([^ ]*\)/\1/')"
  fi
  echo "$read_java_version_cached"
}

# Inspect the system properties exposed by this JVM to identify the major
# version number. Normalize on the popular version number, thus consider JDK
# 1.8 as version "8".
function parse_java_major_version() {
  complete_version=$1
  # split off suffix version info like '-b10' or '+10' or '_10'
  # careful to not use GNU Sed extensions
  version="$(echo "$complete_version" | "${SED}" -e 's/+/_/g' -e 's/-/_/g' | cut -d'_' -f1)"
  case "$version" in
  1.*)
    echo "$version" | cut -d'.' -f2
    ;;
  *)
    echo "$version" | cut -d'.' -f1
    ;;
  esac
}

java_version="$(read_java_version)"
major_version_number="$(parse_java_major_version "$java_version")"
if [ "${major_version_number}" -lt 8 ] ; then
  echo "Phoenix REST Server can only be run on JDK8 and later. Detected JDK version is ${java_version}".
  exit 1
fi
