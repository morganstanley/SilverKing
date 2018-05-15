#!/bin/ksh

source `dirname $0`/../lib/run_scripts_from_any_path.snippet

cd ..
source lib/common.lib
cd -

source lib/common.lib

f_aws_generatePrivateKey
$BUILD_DIR/aws/aws_zk_start.sh
$TESTING_OUTPUT_DIR/$START_INSTANCE_SCRIPT_NAME