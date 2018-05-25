#!/bin/ksh

source `dirname $0`/../lib/run_scripts_from_any_path.snippet

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

function f_aws_addPublicKeyToAuthorizedKeys {
    f_printSubSection "Adding public key to authorized_keys"
    ssh-keygen -y -f ~/.ssh/id_rsa >> ~/.ssh/authorized_keys
}

function f_aws_copyPrivateKeyToAllMachines {
    f_printSubSection "Copying Private Key to all machines"
    
    typeset  srcDir=~/.ssh
    typeset destDir=$srcDir
    typeset keyFile=$srcDir/id_rsa
    
    chmod 600 $keyFile
    f_aws_scp_helper "$destDir" "$keyFile"
}

function f_aws_copyGc {
    f_printSubSection "Copying GC to all machines"
    
    typeset  srcDir=$SK_GRID_CONFIG_DIR
    typeset destDir=$srcDir
    typeset  gcFile=$srcDir/$SK_GRID_CONFIG_NAME.env
    
    f_aws_scp_helper "$destDir" "$gcFile"
}

function f_aws_scp_helper {
    typeset destDir=$1
    typeset    file=$2
    
    while read host; do
        scp $file $USER@$host:$destDir
    done < $NONLAUNCH_HOST_LIST_FILENAME
}

function f_aws_symlinkSkfsD {
    f_printSubSection "Symlinking skfsd on all machines"
    
    while read host; do
        ssh $SSH_OPTIONS $host "ln -sv $SKFS_D $BIN_SKFS_DIR/$SKFS_EXEC_NAME" &
    done < $NONLAUNCH_HOST_LIST_FILENAME
}

f_printSection "PREPPING LAUNCH MACHINE"
f_aws_updateServersList
f_aws_addPublicKeyToAuthorizedKeys
f_aws_copyPrivateKeyToAllMachines
./$ZK_START_SCRIPT_NAME
f_runStaticInstanceCreator

f_printSection "PREPPING NONLAUNCH MACHINES"
f_aws_copyGc
f_aws_symlinkSkfsD

f_printSection "STARTING"
f_runSkAdmin "StartNodes"
f_skUserProcessCheck "1"
f_listSkProcesses

f_startSkfs



