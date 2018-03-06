#!/bin/ksh

source lib/common.lib

f_generatePrivateKey
./aws_zk.sh start
cd ~/SilverKing/build/testing
./start_daemons.sh