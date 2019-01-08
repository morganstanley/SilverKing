#!/bin/bash

function f_createDummyDirIfItDoesntExist  {
    if [[ ! -e  $DUMMY_DIR ]]; then
        echo "Making `basename $DUMMY_DIR`"
        mkdir -p $DUMMY_DIR
    fi
}

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
        $TIMEOUT_SSH $host "$SSH_CMD" </dev/null > $RUN_DIR/$host 2>/dev/null &
        usleep 100000
        ((count++))
    done < $HOSTS_FILE
    
    echo
}

function f_waitForAllErrorsToBeCollected {
    f_checkIfAnySshCommandsAreStillRunningHelper "Waiting for all hosts to complete" 3
}

function f_compareOldAndNewErrorReports {
    echo "Comparing run reports"
    
    typeset lastTwoRunDirs=`ls -rtd $ALL_RUNS_OUTPUT_DIR/*/ | tail -n 2`
    typeset         count=`echo "$lastTwoRunDirs" | wc -l`
    typeset    lastRunDir=`echo "$lastTwoRunDirs" | head -n 1`
    typeset currentRunDir=`echo "$lastTwoRunDirs" | tail -n 1`
    if [[ $count -eq 2 ]]; then
        typeset isCurrentRunDirTheRunDir=`echo "$currentRunDir" | grep -c "$RUN_DIR"`
        if [[ $isCurrentRunDirTheRunDir -ne 1 ]]; then
            ERRORS+="expected currentRunDir: '$RUN_DIR', actual: '$currentRunDir'\n"
        fi
        
        typeset isDummyTheLastDir=`echo "$lastRunDir" | grep -c "dummy"`
        if [[ $isDummyTheLastDir -eq 1 ]]; then
            f_createDummyFiles "$currentRunDir"
            lastRunDir=$DUMMY_DIR
        fi
    else
        echo "ERROR: something went wrong"
    fi
    
    f_checkForAnyZeroSizeFiles "$currentRunDir"
    f_runDiff "$lastRunDir" "$currentRunDir"
}

function f_createDummyFiles {
    typeset referenceDir=$1
    
    echo -en "\tFirst run - creating dummy files based off of '`basename $referenceDir`'..."
    
    for file in `ls $referenceDir`; do
        touch $DUMMY_DIR/$file
    done
    
    # trick to make referenceDir modified more recently than dummy for f_compareOldAndNewErrorReports when we sort on time
    touch $referenceDir/test
    rm    $referenceDir/test
    
    echo "done"
}

function f_checkForAnyZeroSizeFiles {
    typeset dir=$1
    
    echo -en "\tChecking for zero size files in '`basename $dir`'..."
    
    for file in `ls $dir`; do
        typeset filesize=`stat $dir/$file | grep st_size | tr -s [:blank:] | cut -f 3 -d " "`
        if [[ $filesize -eq 0 ]]; then
            ERRORS+=`echo -e "zero size file: $file (this shouldn't happen - grep didn't finish or timedout?)\n"`
        fi
    done 
    
    echo "done"
}

function f_runDiff {
    typeset old=$1
    typeset new=$2

    echo -en "\tDiffing '`basename $old`' v '`basename $new`'..."
    diff $old $new > $DIFF_OUTPUT_FILE
    sed -E -i "s#(^diff .*)#\n\1#g"       $DIFF_OUTPUT_FILE    # -E so I don't have to escape the parenthesis for the capture groups. there's also this (https://unix.stackexchange.com/questions/121161/how-to-insert-text-after-a-certain-string-in-a-file), but I preferred to go with substitution.
    sed    -i "s#$ALL_RUNS_OUTPUT_DIR##g" $DIFF_OUTPUT_FILE
    echo "done"
}

function f_sendEmail {
	typeset to=$1
	typeset from=$2
	typeset subject=$3
	typeset attachments=$4
    
    typeset diffOutputLineCount=`wc -l $attachments | cut -f 1 -d " "`
    typeset result;
    typeset body;
	if [[ $diffOutputLineCount -eq 0 && -z $ERRORS ]] ; then
		attachments=""
        result="PASS ($HOST_COUNT)"
    else
        typeset fails=`grep -P '^diff ' $attachments | cut -d " " -f 2 | grep -P -o "\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}"`
        typeset failCount=`echo "$fails" | wc -l`
        result="FAIL ($failCount/$HOST_COUNT)"
        # for whatever reason, '\n' at the end of the echo's aren't doing anything in the email... but putting them at the beginning is having an affect
        if [[ -n $ERRORS ]]; then
            body+=`echo -e "ERRORS:"`
            body+=`echo -e "\n$ERRORS"`
        fi
        body+=`echo -e "\nFAILS ($failCount/$HOST_COUNT):"`
        body+=`echo -e "\n$fails"`
        body+=`echo -e "\n\nDETAILS:"`
        body+=`cat $DIFF_OUTPUT_FILE`
	fi
    
    f_sendEmailHelper "$to" "$from" "$subject - $result - `basename $RUN_DIR`" "$body" "$attachments"
}

function f_sendEmailHelper {
	typeset to=$1
	typeset from=$2
	typeset subject=$3
	typeset body=$4
	typeset attachments=$5
	
    echo -n "Sending email..."
    
	typeset attachmentsList;
	if [[ -n $attachments ]] ; then
		attachmentsList="-a $attachments"
	fi
	
	echo -e "$body" | $MUTT -e "my_hdr From:$from" $to -s "$subject" $attachmentsList  
    
    echo "done"
}

  HOSTS_FILE=$1
SKFS_LOG_DIR=$2
     RUN_DIR=$3
      RUN_ID=$4
        MUTT=$5
      EMAILS=$6
      
findErrorsScript=$PWD/find_errors.sh
typeset tenMinsInSecs=600
typeset TIMEOUT_SSH="timeout $tenMinsInSecs ssh"
typeset SSH_CMD="$findErrorsScript $SKFS_LOG_DIR"

typeset delim="-"
typeset ALL_RUNS_OUTPUT_DIR=`dirname $RUN_DIR`
typeset           DUMMY_DIR=$ALL_RUNS_OUTPUT_DIR/dummy
typeset    DIFF_OUTPUT_FILE="/tmp/diff_${RUN_ID}.out"
typeset          HOST_COUNT=`wc -l $HOSTS_FILE | cut -f 1 -d " "`
typeset ERRORS;

f_checkIfAnySshCommandsAreStillRunning
f_createDummyDirIfItDoesntExist
f_findErrorsOnHosts
f_waitForAllErrorsToBeCollected
f_compareOldAndNewErrorReports
f_sendEmail "$EMAILS" "sk_health_check_${RUN_ID}@the_real_silverking.com" "SK Health Report - $RUN_ID" "$DIFF_OUTPUT_FILE"