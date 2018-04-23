#!/bin/ksh

cd ..
source lib/common.lib
cd -

source lib/common.lib

function f_aws_removeSkfsD {
    typeset ssh_options="-v -x -o StrictHostKeyChecking=no"
    while read host; do
        ssh $ssh_options $host "rm -rv $BIN_SKFS_DIR/$SKFS_EXEC_NAME"
    done < $NONLAUNCH_HOST_LIST_FILENAME
}

f_printSection "STOPPING"
./aws_stop.sh

f_printSection "UN-PREPPING NONLAUNCH MACHINES"
f_aws_removeSkfsD

