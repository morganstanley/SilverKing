#!/bin/bash

export __SK_TOOL_EXEC_TOOL_CLASS="com.ms.silverking.cloud.dht.daemon.storage.convergence.management.DHTRingMaster"
`dirname $0`/ToolExec.sh "$@"
