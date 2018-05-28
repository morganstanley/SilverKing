#!/bin/ksh

source `dirname $0`/../lib/run_scripts_from_any_path.snippet

cd ..
source lib/common.lib
cd -

source lib/common.lib

function f_aws_removeSkfsD {
    f_printSubSection "Removing skfsd symlink on all machines"
    
    while read host; do
        ssh $SSH_OPTIONS $host "echo -n \"$host: \"; rm -rv $BIN_SKFS_DIR/$SKFS_EXEC_NAME" &
    done < $NONLAUNCH_HOST_LIST_FILENAME
    
    sleep 5
}

f_printSection "STOPPING"
./stop.sh

f_printSection "UN-PREPPING NONLAUNCH MACHINES"
f_aws_removeSkfsD

