#!/bin/ksh

source `dirname $0`/../lib/run_scripts_from_any_path.snippet

cd ..
source lib/common.lib
cd -

source lib/common.lib

$TESTING_DIR/$STOP_INSTANCE_SCRIPT_NAME
$AWS_DIR/$ZK_STOP_SCRIPT_NAME