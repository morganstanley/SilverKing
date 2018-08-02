#!/bin/bash

export __SK_CLASSPATH_TYPE="cloud"
export __SK_TOOL_EXEC_TOOL_CLASS="com.ms.silverking.cloud.dht.management.SKCloudAdmin"
`dirname $0`/ToolExec.sh "$@"
