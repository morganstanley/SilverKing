#!/bin/ksh

cd ..
source lib/common.lib
cd -

source lib/common.lib

f_generatePrivateKey
cd $BUILD_DIR/aws
./aws_zk.sh start
cd $TESTING_OUTPUT_DIR
./start_daemons.sh