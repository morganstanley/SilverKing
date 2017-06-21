#!/bin/bash

export __SK_TOOL_EXEC_TOOL_CLASS="com.ms.silverking.net.WaitForHostPort"
`dirname $0`/ToolExec.sh "$@"
