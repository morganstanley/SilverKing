#!/bin/ksh

source lib/common.lib

if [[ -z $1 ]]; then
	echo "Need to know 'start' or 'stop'"
	exit
fi

~/$ZK_VERSION/bin/zkServer.sh $1
