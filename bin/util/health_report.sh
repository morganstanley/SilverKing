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
    
    f_logSshCmd "$host $sshCmd $startLineNumber"    
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
        f_logError "f_getPreviousLineNumber: $file"
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
            f_logError "expected currentRunDir: '$RUN_DIR', actual: '$currentRunDir'"
        fi
    elif [[ $count -eq 1 ]]; then
        previousRunDir=""
    else
        f_logError "f_getPreviousRunDir: couldn't find correctly"
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
            f_logEmptyFile "$file"
        fi
    done 
}

function f_scrubFilesForIgnorableErrors {
    echo "Removing ignorable errors from files"
    find $RUN_DIR -name '*_sk' -type f | xargs sed -i -r -e '/WARNING: 20..:..:.. ..:..:.. EST addSuspect: [[:digit:]]{1,3}\.[[:digit:]]{1,3}\.[[:digit:]]{1,3}\.[[:digit:]]{1,3}:[[:digit:]]+ ReplicaTimeout$/d'  
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
        f_logError "'$filenameOnly' doesn't exist: Did it timeout? Is it ssh'able?"
    else     
        typeset numOfLines=$(f_getNumberOfLines "$file")
        if [[ $numOfLines -lt 0 ]]; then
            f_logError "'$filenameOnly' has a negative # of lines"
        elif [[ $numOfLines -eq 0 ]]; then
            f_logError "'$filenameOnly' has 0 lines ($numOfLines). Did it timeout? Is it ssh'able?"
        elif [[ $numOfLines -eq 1 ]]; then
            f_logError "'$filenameOnly' has only 1 line: `cat $file`"
        else               
            # compare log filenames
            if [[ -n $previousFileExists ]]; then
                typeset  currentLogFilename=`head -n 1 $file`
                typeset previousLogFilename=`head -n 1 $previousRunDir/$filenameOnly`
                if [[ $currentLogFilename != $previousLogFilename ]]; then
                    f_logError "'$filenameOnly' log file changed. current='$currentLogFilename', prev='$previousLogFilename'"
                fi
            fi
        
            if [[ $numOfLines -gt 2 ]]; then
                f_logFail "$filenameOnly"
                f_logDetail "> $filenameOnly"
                f_logDetail "`tail -n +3 $file`"
                f_logDetail "\n"
            elif [[ $numOfLines -eq 2 ]]; then
                # fixme - this is the passing case, do I need to do something here?
                typeset fixme;
            fi
        fi
    fi
}

function f_logError {
    f_logHelper "$1" "$ERRORS_FILE"
}

function f_logFail {
    f_logHelper "$1" "$FAILS_FILE"
}

function f_logDetail {
    f_logHelper "$1" "$DETAILS_FILE"
}

function f_logEmptyFile {
    f_logHelper "$1" "$EMPTY_FILES"
}

function f_logSshCmd {
    f_logHelper "$1" "$TMP_OUTPUT_RUN_DIR/ssh.cmds"
}

function f_logHelper {
    typeset  msg=$1
    typeset file=$2
    
    echo -e "$msg" >> $file
}

function f_runDiffOnHostFiles {
    typeset oldDir=$1
    typeset newDir=$2

    echo -en "\tDiffing '`basename $oldDir`' v '`basename $newDir`'..."
    typeset oldTmpDir=$TMP_OUTPUT_RUN_DIR/diff/old
    typeset newTmpDir=$TMP_OUTPUT_RUN_DIR/diff/new
    
    rm -rf   $oldTmpDir/* $newTmpDir/*
    mkdir -p $oldTmpDir   $newTmpDir
    
    cp $oldDir/*.txt $oldTmpDir
    cp $newDir/*.txt $newTmpDir
    
    diff $oldTmpDir $newTmpDir > $HOST_DIFF_OUTPUT_FILE
    typeset numOfLines=$(f_getNumberOfLines "$HOST_DIFF_OUTPUT_FILE")
    if [[ $numOfLines -gt 0 ]]; then
        sed -E -i "s#(^diff .*)#\n\1#g"           $HOST_DIFF_OUTPUT_FILE    # -E so I don't have to escape the parenthesis for the capture groups. there's also this (https://unix.stackexchange.com/questions/121161/how-to-insert-text-after-a-certain-string-in-a-file), but I preferred to go with substitution.
        sed    -i "s#$TMP_OUTPUT_RUN_DIR/diff##g" $HOST_DIFF_OUTPUT_FILE
    else
        rm $HOST_DIFF_OUTPUT_FILE
    fi
    
    echo "done"
}

function f_sendEmail {
	typeset to=$1
	typeset from=$2
	typeset subject=$3
    
    touch $REPORT_FILE
    
    typeset result;
    if [[ (! -e $ERRORS_FILE) && (! -e $FAILS_FILE) && (! -e $DETAILS_FILE) && (! -e $HOST_DIFF_OUTPUT_FILE) ]]; then
        result="PASS ($HOST_COUNT)"
    else
        typeset failHostCount=`cut -d '_' -f 1 $FAILS_FILE | sort -n | uniq | wc -l`
        result="FAIL ($failHostCount/$HOST_COUNT)"
        
        f_logReportSection "ERRORS"  "$ERRORS_FILE"
        f_logReportSection "FAILS"   "$FAILS_FILE" " ($failHostCount/$HOST_COUNT)"
        f_logReportSection "DETAILS" "$DETAILS_FILE"
        
        if [[ -e $HOST_DIFF_OUTPUT_FILE ]]; then
            result+="(-)"
            cat $HOST_DIFF_OUTPUT_FILE >> $REPORT_FILE
        fi
	fi
    
    echo -e "\tresult: $result"
    
    typeset reportFileSize=$(f_getFileSize "$REPORT_FILE")
    typeset tenMegabytes=10000000
    typeset body;
    typeset attachment;
    if [[ $reportFileSize -lt $tenMegabytes ]]; then
        body=`cat $REPORT_FILE`
        attachment="$REPORT_FILE"
    else 
        typeset reportFileSnapshot="$TMP_OUTPUT_RUN_DIR/report_snapshot.out"
        head -n 50000 $REPORT_FILE > $reportFileSnapshot
        body=`echo "reportFile is too big (size=$reportFileSize). Here's a snippet below. Check '$REPORT_FILE' for the full details."`
        body+=`cat $reportFileSnapshot`
        attachment="$reportFileSnapshot"
    fi
    
    
    f_sendEmailHelper "$to" "$from" "$subject - $result - `basename $RUN_DIR`" "$body" "$attachment"
}

function f_logReportSection {
    typeset sectionName=$1
    typeset sectionFile=$2
    typeset sectionExtra=$3

    if [[ -e $sectionFile ]]; then
        echo "${sectionName}${sectionExtra}:" >> $REPORT_FILE
        cat $sectionFile                      >> $REPORT_FILE
        echo ""                               >> $REPORT_FILE
    fi
}

function f_getFileSize {
    typeset file=$1
    ls -l "$file" | cut -d ' ' -f 5
}

function f_sendEmailHelper {
	typeset to=$1
	typeset from=$2
	typeset subject=$3
	typeset body=$4
	typeset attachments=$5
	
    echo -n "Sending email..."
    
	typeset attachmentsList;
	if [[ -n $attachments ]]; then
		attachmentsList="-a $attachments"
	fi
	
	echo -e "$body" | $MUTT -e "my_hdr From:$from" $to -s "$subject" $attachmentsList  
    
    echo "done"
}

function f_getNumberOfLines {
    typeset file=$1
    
    typeset numOfLines=0;
    if [[ -e $file ]]; then
        numOfLines=`wc -l $file | cut -f 1 -d ' '`
    fi
    
    echo $numOfLines
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
typeset fiveMinsInSecs=300
typeset TIMEOUT_SSH="timeout $fiveMinsInSecs ssh"
typeset   SK_SSH_CMD="$findErrorsScript   $SK_LOG_DIR"
typeset SKFS_SSH_CMD="$findErrorsScript $SKFS_LOG_DIR"

typeset   ALL_RUNS_OUTPUT_DIR=`dirname $RUN_DIR`
typeset HOST_DIFF_OUTPUT_FILE="$TMP_OUTPUT_RUN_DIR/diff_hosts.out"
typeset            HOST_COUNT=$(f_getNumberOfLines "$HOSTS_FILE")
typeset           EMPTY_FILES="$TMP_OUTPUT_RUN_DIR/empty_files.out"
typeset           ERRORS_FILE="$TMP_OUTPUT_RUN_DIR/run.errors"
typeset            FAILS_FILE="$TMP_OUTPUT_RUN_DIR/run.fails"
typeset          DETAILS_FILE="$TMP_OUTPUT_RUN_DIR/run.details"
typeset           REPORT_FILE="$TMP_OUTPUT_RUN_DIR/report.out"

f_checkIfAnySshCommandsAreStillRunning
f_findErrorsOnHosts
f_waitForAllErrorsToBeCollected
f_rerunAnyZeroSizeFiles
f_scrubFilesForIgnorableErrors
f_runErrorReport
f_sendEmail "$EMAILS" "sk_health_check_${RUN_ID}_2@the_real_silverking.com" "SK Health Report - $RUN_ID"