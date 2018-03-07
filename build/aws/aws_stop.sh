#!/bin/ksh

cd ..
source lib/common.lib
cd -

source lib/common.lib

cd $TESTING_OUTPUT_DIR
./stop_daemons.sh
cd $BUILD_DIR/aws
./aws_zk.sh stop