#!/bin/ksh

source `dirname $0`/../lib/run_scripts_from_any_path.snippet

cd ..
source lib/common.lib
cd -

source lib/common.lib

f_aws_generatePrivateKey
$AWS_DIR/$ZK_START_SCRIPT_NAME
$TESTING_DIR/$START_INSTANCE_SCRIPT_NAME