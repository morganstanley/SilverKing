#!/bin/bash

source lib/common.lib

function f_checkIfAnySshCommandsAreStillRunning {
    f_checkIfAnySshCommandsAreStillRunningHelper "Checking if any ssh cmd's are still running" 5
}

function f_checkIfAnySshCommandsAreStillRunningHelper {
    typeset msg=$1
    typeset printCount=$2

    echo "$1"
    
    typeset previousCount=0
    typeset stillRunningCount=$(f_getStillRunningCount)
    while [[ $stillRunningCount -ne 0 ]]; do
        echo -e "\t$stillRunningCount remaining"
        if [[ -n $printCount && $stillRunningCount -le $printCount ]]; then
            if [[ $stillRunningCount -ne $previousCount ]]; then
                f_getStillRunning
                previousCount=$stillRunningCount
            fi
        fi
        sleep 10
        
        stillRunningCount=$(f_getStillRunningCount)
    done
}

function f_getStillRunning {
    ps uxww | grep "$TIMEOUT_SSH" | grep "$SSH_CMD"
}

function f_getStillRunningCount {
    ps uxww | grep "$TIMEOUT_SSH" | grep -c "$SSH_CMD"
}

function f_findErrorsOnHosts {
    echo "Running on $HOST_COUNT hosts"
    echo -en "\t"
    
    typeset count=1
    while read host; do
        echo -n "$count "
        f_findErrors "$host" "$SSH_CMD"
        usleep 100000
        ((count++))
    done < $HOSTS_FILE
    
    echo
}

function f_findErrors {
    typeset   host=$1
    typeset sshCmd=$2
    
    $TIMEOUT_SSH $host "$sshCmd" </dev/null > $RUN_DIR/$host 2>/dev/null &
}

function f_waitForAllErrorsToBeCollected {
    f_checkIfAnySshCommandsAreStillRunningHelper "Waiting for all hosts to complete" 3
}


        HOSTS_FILE=$1
     SKFS_LOG_PATH=$2
           RUN_DIR=$3
typeset  ninetySecs=90
typeset  TIMEOUT_SSH="timeout $ninetySecs ssh"
typeset SSH_CMD="grep -R -i -H -n fatal $SKFS_LOG_PATH'"

typeset HOST_COUNT=$(f_getNumberOfLines "$HOSTS_FILE")

f_checkIfAnySshCommandsAreStillRunning
f_findErrorsOnHosts
f_waitForAllErrorsToBeCollected