#!/bin/ksh

cd ..
source lib/common.lib
cd -

source lib/common.lib

cd $TESTING_OUTPUT_DIR
./$STOP_INSTANCE_SCRIPT_NAME
cd $BUILD_DIR/aws
./aws_zk.sh stop