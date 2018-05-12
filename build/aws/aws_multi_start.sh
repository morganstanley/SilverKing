#!/bin/ksh

cd ..
source lib/common.lib
cd -

source lib/common.lib

function f_aws_updateServersList {
    typeset launchIp=`hostname -i`
    typeset serverList="$launchIp"
    
    typeset count=1
    while read host; do
        serverList+=",$host"
        ((count++))
    done < $NONLAUNCH_HOST_LIST_FILENAME
    
    f_printSubSection "Updating server list to $count machines: $serverList"
    f_overrideBuildConfigVariable "SK_SERVERS" "$serverList"
}

function f_aws_copyGc {
    typeset gcFile=$SK_GRID_CONFIG_DIR/$SK_GRID_CONFIG_NAME.env
    while read host; do
        scp $gcFile $USER@$host:$SK_GRID_CONFIG_DIR
    done < $NONLAUNCH_HOST_LIST_FILENAME
}

function f_aws_symlinkSkfsD {
    typeset ssh_options="-v -x -o StrictHostKeyChecking=no"
    while read host; do
        ssh $ssh_options $host "ln -sv $SKFS_D $BIN_SKFS_DIR/$SKFS_EXEC_NAME" &
    done < $NONLAUNCH_HOST_LIST_FILENAME
}

f_printSection "PREPPING LAUNCH MACHINE"
f_aws_updateServersList
./aws_zk.sh start
f_runStaticInstanceCreator

f_printSection "PREPPING NONLAUNCH MACHINES"
f_aws_copyGc
f_aws_symlinkSkfsD

f_printSection "STARTING"
f_runSkAdmin "StartNodes"
f_skUserProcessCheck "1"
f_listSkProcesses

f_startSkfs



