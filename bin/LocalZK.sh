#!/bin/bash

export __SK_TOOL_EXEC_TOOL_CLASS="com.ms.silverking.cloud.zookeeper.LocalZKImpl"
`dirname $0`/ToolExec.sh "$@"
