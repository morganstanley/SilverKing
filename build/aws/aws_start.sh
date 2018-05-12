#!/bin/ksh

cd ..
source lib/common.lib
cd -

source lib/common.lib

f_aws_generatePrivateKey
cd $BUILD_DIR/aws
./aws_zk.sh start
cd $TESTING_OUTPUT_DIR
./$START_INSTANCE_SCRIPT_NAME