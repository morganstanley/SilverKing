#!/bin/bash

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
    ps uxww | grep "$TIMEOUT_SSH" | grep   "$SK_SSH_CMD"
    ps uxww | grep "$TIMEOUT_SSH" | grep "$SKFS_SSH_CMD"
}

function f_getStillRunningCount {
    typeset   skRunningCount=`ps uxww | grep "$TIMEOUT_SSH" | grep -c   "$SK_SSH_CMD"`
    typeset skfsRunningCount=`ps uxww | grep "$TIMEOUT_SSH" | grep -c "$SKFS_SSH_CMD"`
    typeset totalRunningCount=$((skRunningCount+skfsRunningCount))
    
    echo $totalRunningCount
}

function f_findErrorsOnHosts {
    echo "Running on $HOST_COUNT hosts"
    echo -en "\t"
    
    typeset count=1
    while read host; do
        echo -n "$count "
        f_findSkErrors   "$host"
        f_findSkfsErrors "$host"
        usleep 100000
        ((count++))
    done < $HOSTS_FILE
    
    echo
}

function f_findSkErrors {
    f_findErrorsHelper "$1" "sk" "$SK_SSH_CMD"
}

function f_findSkfsErrors {
    f_findErrorsHelper "$1" "skfs" "$SKFS_SSH_CMD"
}

function f_findErrorsHelper {
    typeset       host=$1
    typeset fileEnding=$2
    typeset     sshCmd=$3
    
    typeset file="${host}_${fileEnding}"
    typeset startLineNumber=$(f_getStartLineNumber "$file")
    
    echo "$host $sshCmd $startLineNumber" >> $TMP_OUTPUT_RUN_DIR/ssh.cmds
    
    $TIMEOUT_SSH $host "$sshCmd $startLineNumber" </dev/null > $RUN_DIR/$file 2>/dev/null &
}

function f_getStartLineNumber {
    typeset file=$1
    
    typeset startLineNumber;
    typeset previousLineNumber=$(f_getPreviousLineNumber "$file")
    if [[ -n $previousLineNumber && $previousLineNumber -gt 0 ]]; then
        startLineNumber=$((previousLineNumber+1))
    else
        startLineNumber=1
    fi 
    
    echo $startLineNumber
}

function f_getPreviousLineNumber {
    typeset file=$1
    
    typeset previousLineNumber;
    typeset previousRunDir=$(f_getPreviousRunDir)
    if [[ -n $previousRunDir && -e $previousRunDir/$file ]]; then
        # get the second line
        previousLineNumber=`head -n 2 $previousRunDir/$file | tail -n 1`
    else
        previousLineNumber="previousLineNumberError: $file"
        ERRORS+="f_getPreviousLineNumber: $file\n"
    fi
      
    echo $previousLineNumber      
}

function f_getPreviousRunDir {
    typeset lastTwoRunDirs=`ls -rtd $ALL_RUNS_OUTPUT_DIR/*/ | tail -n 2`    # /* is important so we get absolute names. and ending '/' is important b/c if only '$ALL_RUNS_OUTPUT_DIR/*', that doesn't work with -d it still lists all the files too, so we need /*/
    typeset          count=`echo "$lastTwoRunDirs" | wc -l`
    typeset previousRunDir=`echo "$lastTwoRunDirs" | head -n 1`
    typeset  currentRunDir=`echo "$lastTwoRunDirs" | tail -n 1`
    if [[ $count -eq 2 ]]; then
        typeset isCurrentRunDirTheRunDir=`echo "$currentRunDir" | grep -c "$RUN_DIR"`
        if [[ $isCurrentRunDirTheRunDir -ne 1 ]]; then
            ERRORS+="expected currentRunDir: '$RUN_DIR', actual: '$currentRunDir'\n"
        fi
    elif [[ $count -eq 1 ]]; then
        previousRunDir=""
    else
        ERRORS+="f_getPreviousRunDir: couldn't find correctly\n"
    fi
    
    echo $previousRunDir
}

function f_waitForAllErrorsToBeCollected {
    f_checkIfAnySshCommandsAreStillRunningHelper "Waiting for all hosts to complete" 3
}

function f_rerunAnyZeroSizeFiles {
    echo "Checking for zero size files in '`basename $RUN_DIR`'..."
    f_checkForAnyZeroSizeFiles "$RUN_DIR"
    
    if [[ -e $EMPTY_FILES ]]; then
        echo -e "\tRerunning files on hosts"
        typeset count=1
        while read file; do
            echo -e "\t\t$count - $file"
            typeset host=`echo "$file" | cut -d '_' -f 1`
            if [[ $file =~ sk$ ]]; then
                f_findSkErrors   "$host"
            elif [[ $file =~ skfs$ ]]; then
                f_findSkfsErrors "$host"
            else
                echo "we have an issue: $file"
            fi
            usleep 100000
            ((count++))
        done < $EMPTY_FILES
        
        f_waitForAllErrorsToBeCollected
    fi
}

function f_checkForAnyZeroSizeFiles {
    typeset dir=$1
        
    for file in `ls $dir`; do
        typeset numOfLines=$(f_getNumberOfLines "$dir/$file")
        if [[ $numOfLines -eq 0 ]]; then
            echo "$file" >> $EMPTY_FILES
        fi
    done 
}

function f_scrubFilesForIgnorableErrors {
    echo "Removing ignorable errors from files"
    find $RUN_DIR -name '*_sk*' -type f | xargs sed -i -r -e '/WARNING: 20..:..:.. ..:..:.. EST addSuspect: [[:digit:]]{1,3}\.[[:digit:]]{1,3}\.[[:digit:]]{1,3}\.[[:digit:]]{1,3}:[[:digit:]]+ ReplicaTimeout$/d'  
}

function f_runErrorReport {
    echo "Running error report"
    
    while read host; do
        f_checkFilename "$RUN_DIR/${host}_sk"
        f_checkFilename "$RUN_DIR/${host}_skfs"
    done < $HOSTS_FILE
    
    typeset previousRunDir=$(f_getPreviousRunDir)
    if [[ -n $previousRunDir ]]; then
        f_runDiffOnHostFiles "$previousRunDir" "$RUN_DIR"
    fi
}

# if oldExist and no exist For current
# if currentExist and no exists for previous = server got re-included, or last run timedout 
# check if line ranges are good, if startOld > startPrev, chance file changed
function f_checkFilename {
    typeset file=$1

    typeset filenameOnly=`basename $file`
        
    typeset previousFileExists=""
    typeset previousRunDir=$(f_getPreviousRunDir)
    if [[ -n $previousRunDir ]]; then
        if [[ -e $previousRunDir/$filenameOnly ]]; then
            previousFileExists="true"
        fi
    fi
    
    if [[ ! -e $file ]]; then
        ERRORS+="'$filenameOnly' doesn't exist: Did it timeout? Is it ssh'able?\n"
    else     
        typeset numOfLines=$(f_getNumberOfLines "$file")
        if [[ $numOfLines -lt 0 ]]; then
            ERRORS+="'$filenameOnly' has a negative # of lines\n"
        elif [[ $numOfLines -eq 0 ]]; then
            ERRORS+="'$filenameOnly' has 0 lines ($numOfLines). Did it timeout? Is it ssh'able?\n"
        elif [[ $numOfLines -eq 1 ]]; then
            ERRORS+="'$filenameOnly' has only 1 line: `cat $file`\n"
        else               
            # compare log filenames
            if [[ -n $previousFileExists ]]; then
                typeset  currentLogFilename=`head -n 1 $file`
                typeset previousLogFilename=`head -n 1 $previousRunDir/$filenameOnly`
                if [[ $currentLogFilename != $previousLogFilename ]]; then
                    ERRORS+="'$filenameOnly' skfs log file changed. current='$currentLogFilename', prev='$previousLogFilename'\n"
                fi
            fi
        
            if [[ $numOfLines -gt 2 ]]; then
                FAILS+="$filenameOnly\n"
                DETAILS+="> $filenameOnly\n"
                DETAILS+=`tail -n +3 $file`
                DETAILS+="\n\n"
            elif [[ $numOfLines -eq 2 ]]; then
                # fixme
                typeset fixme;
                # ERRORS+="hi\n"
            fi
        fi
    fi
}

function f_runDiffOnHostFiles {
    typeset oldDir=$1
    typeset newDir=$2

    echo -en "\tDiffing '`basename $oldDir`' v '`basename $newDir`'..."
    typeset oldTmpDir=$TMP_OUTPUT_RUN_DIR/diff/old
    typeset newTmpDir=$TMP_OUTPUT_RUN_DIR/diff/new
    rm -rf $oldTmpDir/* $newTmpDir/*
    mkdir -p $oldTmpDir $newTmpDir
    cp $oldDir/*.txt $oldTmpDir
    cp $newDir/*.txt $newTmpDir
    diff $oldTmpDir $newTmpDir > $HOST_DIFF_OUTPUT_FILE
    sed -E -i "s#(^diff .*)#\n\1#g"           $HOST_DIFF_OUTPUT_FILE    # -E so I don't have to escape the parenthesis for the capture groups. there's also this (https://unix.stackexchange.com/questions/121161/how-to-insert-text-after-a-certain-string-in-a-file), but I preferred to go with substitution.
    sed    -i "s#$TMP_OUTPUT_RUN_DIR/diff##g" $HOST_DIFF_OUTPUT_FILE
    echo "done"
}

function f_sendEmail {
	typeset to=$1
	typeset from=$2
	typeset subject=$3
	typeset attachments=$4
    
    typeset diffOutputLineCount=$(f_getNumberOfLines "$attachments")
    typeset result;
    typeset reportFile=$TMP_OUTPUT_RUN_DIR/report.out
    
    touch $reportFile
	
    if [[ $diffOutputLineCount -eq 0 && -z $FAILS && -z $ERRORS && -z $DETAILS ]] ; then
		attachments=""
        result="PASS ($HOST_COUNT)"
    else
        typeset failHostCount=`echo -en "$FAILS" | cut -d '_' -f 1 | sort -n | uniq | wc -l`
        result="FAIL ($failHostCount/$HOST_COUNT)"
        # for whatever reason, '\n' at the end of the echo's aren't doing anything in the email... but putting them at the beginning is having an affect
        if [[ -n $ERRORS ]]; then
            echo    "ERRORS:"   >> $reportFile
            echo -e "$ERRORS\n" >> $reportFile
        fi
        echo "FAILS ($failHostCount/$HOST_COUNT):" >> $reportFile
        echo -e "$FAILS" >> $reportFile
        echo    "DETAILS:" >> $reportFile
        echo -e "$DETAILS\n"  >> $reportFile
        if [[ -e $HOST_DIFF_OUTPUT_FILE ]]; then
            cat $HOST_DIFF_OUTPUT_FILE >> $reportFile
        fi
	fi
    
    typeset body=`cat $reportFile`
    f_sendEmailHelper "$to" "$from" "$subject - $result - `basename $RUN_DIR`" "$body" "$reportFile"
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

function f_getNumberOfLines {
    typeset file=$1
    
    wc -l $file | cut -f 1 -d ' '
}

        HOSTS_FILE=$1
        SK_LOG_DIR=$2
      SKFS_LOG_DIR=$3
           RUN_DIR=$4
            RUN_ID=$5
TMP_OUTPUT_RUN_DIR=$6
              MUTT=$7
            EMAILS=$8

typeset findErrorsScript=$PWD/find_errors.sh
typeset tenMinsInSecs=600
typeset TIMEOUT_SSH="timeout $tenMinsInSecs ssh"
typeset   SK_SSH_CMD="$findErrorsScript   $SK_LOG_DIR"
typeset SKFS_SSH_CMD="$findErrorsScript $SKFS_LOG_DIR"

typeset   ALL_RUNS_OUTPUT_DIR=`dirname $RUN_DIR`
typeset HOST_DIFF_OUTPUT_FILE=$TMP_OUTPUT_RUN_DIR/diff_hosts.out
typeset            HOST_COUNT=$(f_getNumberOfLines "$HOSTS_FILE")
typeset           EMPTY_FILES=$TMP_OUTPUT_RUN_DIR/empty_files.out
typeset ERRORS;
typeset FAILS;
typeset DETAILS;

f_checkIfAnySshCommandsAreStillRunning
f_findErrorsOnHosts
f_waitForAllErrorsToBeCollected
f_rerunAnyZeroSizeFiles
f_scrubFilesForIgnorableErrors
f_runErrorReport
f_sendEmail "$EMAILS" "sk_health_check_${RUN_ID}_2@the_real_silverking.com" "SK Health Report - $RUN_ID" "$HOST_DIFF_OUTPUT_FILE"