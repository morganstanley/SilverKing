#!/bin/ksh

if [[ -z $1 ]]; then
	echo "Need to know 'start' or 'stop'"
	exit
fi

~/zookeeper-3.4.11/bin/zkServer.sh $1
