#!/bin/ksh

source `dirname $0`/../lib/run_scripts_from_any_path.snippet
source lib/common.lib

$LIB_ROOT/$ZK_VERSION/bin/zkServer.sh "start"
