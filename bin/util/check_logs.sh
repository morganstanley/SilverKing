#!/bin/bash

function f_checkIfAnySshCommandsAreStillRunning {
    f_checkIfAnySshCommandsAreStillRunningHelper "Checking if any ssh cmd's are still running" 5
}

function f_checkIfAnySshCommandsAreStillRunningHelper {
    typeset msg=$1
    typeset printCount=$2

    echo "$msg"
    
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
    
    typeset filename="${host}_${fileEnding}"
    typeset startLineNumber=$(f_getStartLineNumber "$filename")
    
    f_logSshCmd "$TIMEOUT_SSH $host $sshCmd $startLineNumber"    
    $TIMEOUT_SSH $host "$sshCmd $startLineNumber" </dev/null > $RUN_DIR/$filename 2>/dev/null &
}

function f_getStartLineNumber {
    typeset filename=$1
    
    typeset startLineNumber;
    typeset previousLineNumber=$(f_getPreviousLineNumber "$filename")
    if [[ -n $previousLineNumber && $previousLineNumber -gt 0 ]]; then
        startLineNumber=$((previousLineNumber+1))
    else
        startLineNumber=$START_LINE_NUMBER
    fi 
    
    echo $startLineNumber
}

function f_getPreviousLineNumber {
    typeset filename=$1
    
    typeset previousLineNumber;
    typeset previousRunDir=$(f_getPreviousRunDir "$filename")
    if [[ -n $previousRunDir ]]; then
        # get the line number line
        previousLineNumber=`head -n $NUM_OF_META_DATA_LINES $previousRunDir/$filename | tail -n 1`
    else
        previousLineNumber="previousLineNumberError: $filename"
        f_logError "f_getPreviousLineNumber: $filename"
    fi
      
    echo $previousLineNumber      
}

function f_getPreviousRunDir {
    typeset filename=$1
    typeset maxNumOfPreviousDirectoriesToWalkBack=8

    typeset desiredNumOfDirectories=$((maxNumOfPreviousDirectoriesToWalkBack + 1))    # +1 b/c includes the current
    typeset             lastRunDirs=`ls -td $ALL_RUNS_OUTPUT_DIR/*/ | head -n $desiredNumOfDirectories`    # /* is important so we get absolute names. and ending '/' is important b/c if only '$ALL_RUNS_OUTPUT_DIR/*', that doesn't work with -d it still lists all the files too, so we need /*/
    typeset  actualDirectoriesCount=`echo "$lastRunDirs" | wc -l`
    
    typeset previousRunDir="";
    if [[ $actualDirectoriesCount -eq 0 ]]; then
        f_logError "f_getPreviousRunDir: didn't find any directories at all. not even the running dir."
    elif [[ $actualDirectoriesCount -eq 1 ]]; then
        typeset filler_so_this_if_statement_can_exist   # this is the case when it's the first run
    else
        typeset            currentRunDir=`echo "$lastRunDirs" | head -n 1`
        typeset isCurrentRunDirTheRunDir=`echo "$currentRunDir" | grep -c "$RUN_DIR"`
        if [[ $isCurrentRunDirTheRunDir -ne 1 ]]; then
            f_logError "expected currentRunDir: '$RUN_DIR', actual: '$currentRunDir'"
        else
            typeset actualNumOfPreviousDirs=$((actualDirectoriesCount - 1))    # -1 to exclude the current
            if [[ -z $filename ]]; then
                previousRunDir=$(f_parsePreviousDirectoryFrom "$lastRunDirs" "$actualNumOfPreviousDirs")
                typeset filler_so_this_if_statement_can_exist   # this is the case where we only want the immediate previous directory
            else
                typeset previousDirNumber=1
                typeset         dirsCount=$actualNumOfPreviousDirs
                while [[ $previousDirNumber -le $actualNumOfPreviousDirs ]]; do
                    previousRunDir=$(f_parsePreviousDirectoryFrom "$lastRunDirs" "$dirsCount")
                    if [[ -e $previousRunDir/$filename && $(f_getNumberOfLines "$previousRunDir/$filename") -ge $NUM_OF_META_DATA_LINES ]]; then
                        # found it
                        break;
                    fi
                    previousRunDir=""
                    ((previousDirNumber++))
                    ((dirsCount--))
                done
            fi
        fi
    fi
    
    echo $previousRunDir
}

function f_parsePreviousDirectoryFrom {
    typeset dirsList=$1
    typeset numOfDirsToGrabStartingFromTheBottom=$2
    
    echo "$dirsList" | tail -n $numOfDirsToGrabStartingFromTheBottom | head -n 1
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
        while read filename; do
            echo -e "\t\t$count - $filename"
            typeset host=`echo "$filename" | cut -d '_' -f 1`
            if [[ $filename =~ sk$ ]]; then
                f_findSkErrors   "$host"
            elif [[ $filename =~ skfs$ ]]; then
                f_findSkfsErrors "$host"
            else
                echo "we have an issue: $filename"
            fi
            usleep 100000
            ((count++))
        done < $EMPTY_FILES
        
        f_waitForAllErrorsToBeCollected
    fi
}

function f_checkForAnyZeroSizeFiles {
    typeset dir=$1
        
    for filename in `ls $dir | grep -v '.txt$'`; do
        typeset numOfLines=$(f_getNumberOfLines "$dir/$filename")
        if [[ $numOfLines -eq 0 ]]; then
            f_logEmptyFile "$filename"
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
        f_checkFile "$RUN_DIR/${host}_sk"
        f_checkFile "$RUN_DIR/${host}_skfs"
    done < $HOSTS_FILE
    
    typeset previousRunDir=$(f_getPreviousRunDir)
    if [[ -n $previousRunDir ]]; then
        f_runDiffOnHostFiles "$previousRunDir" "$RUN_DIR"
    fi
}

# if oldExist and no exist For current
# if currentExist and no exists for previous = server got re-included, or last run timedout 
# check if line ranges are good, if startOld > startPrev, chance file changed
function f_checkFile {
    typeset file=$1

    typeset filename=`basename $file`
    
    if [[ ! -e $file ]]; then
        f_logError "'$filename' doesn't exist: Did it timeout? Is it ssh'able?"
    else     
        typeset numOfLines=$(f_getNumberOfLines "$file")
        if [[ $numOfLines -lt 0 ]]; then
            f_logError "'$filename' has a negative # of lines"
        elif [[ $numOfLines -eq 0 ]]; then
            f_logError "'$filename' has 0 lines ($numOfLines). Did it timeout? Is it ssh'able?"
        elif [[ $numOfLines -eq 1 ]]; then
            f_logError "'$filename' has only 1 line: `cat $file`"
        else           
            typeset previousRunDir=$(f_getPreviousRunDir "$filename")
            # compare log filenames
            if [[ -n $previousRunDir ]]; then
                typeset  currentLogFilename=`head -n 1 $file`
                typeset previousLogFilename=`head -n 1 $previousRunDir/$filename`
                if [[ $currentLogFilename != $previousLogFilename ]]; then
                    f_logError "'$filename' log file changed. current='$currentLogFilename', prev='$previousLogFilename'"
                    sed -i "${LINE_NUMBER_LINE}s#.*#$START_LINE_NUMBER#" $file # override old line number back to start
                fi
            fi
        
            if [[ $numOfLines -gt $NUM_OF_META_DATA_LINES ]]; then
                f_logFail "$filename"
                f_logDetail "> $filename"
                f_logDetail "`tail -n +$((NUM_OF_META_DATA_LINES+1)) $file`"
                f_logDetail "\n"
            elif [[ $numOfLines -eq $NUM_OF_META_DATA_LINES ]]; then
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
        sed -ri "s#(^diff .*)#\n\1#g"           $HOST_DIFF_OUTPUT_FILE    # -E so I don't have to escape the parenthesis for the capture groups. there's also this (https://unix.stackexchange.com/questions/121161/how-to-insert-text-after-a-certain-string-in-a-file), but I preferred to go with substitution.
        sed -i  "s#$TMP_OUTPUT_RUN_DIR/diff##g" $HOST_DIFF_OUTPUT_FILE
    else
        rm $HOST_DIFF_OUTPUT_FILE
    fi
    
    echo "done"
}

function f_sendEmail {
	typeset to=$1
	typeset from=$2
    
    touch $REPORT_FILE
    
    typeset result;
    if [[ (! -e $ERRORS_FILE) && (! -e $FAILS_FILE) && (! -e $DETAILS_FILE) && (! -e $HOST_DIFF_OUTPUT_FILE) ]]; then
        result="PASS ($HOST_COUNT)"
    else
        typeset failHostCount=0
        if [[ -e $FAILS_FILE ]]; then
            failHostCount=$(f_getUniqueHosts "$FAILS_FILE")
            result="INFO ($failHostCount/$HOST_COUNT)"
            
            typeset thresholdPercent=10
            typeset actualPercent=$((failHostCount*100 / HOST_COUNT))   # *100 is to get a percentage - if you *100 at the end, the failHostCount / HOST_COUNT will be 0 *100
            if [[ $actualPercent -ge $thresholdPercent ]]; then
                result+="(${actualPercent}%)"
            fi
            
            typeset fatalFails="pbr_read received error from fbr_read|TIMEOUT"
            # typeset fatalFails="*** check this machine"
            typeset warningFails="KeeperErrorCode = ConnectionLoss for |KeeperException| wf_write_block_sync failed | EST sendFailed/|java.lang.UnsupportedOperationException|A fatal error has been detected by the Java Runtime Environment|fbr_read failed|terminate called after throwing an instance of"
                
            typeset   fatalCount=`grep -Pc "$fatalFails"   "$REPORT_FILE"`
            typeset warningCount=`grep -Pc "$warningFails" "$REPORT_FILE"`
            
            if [[ $fatalCount -gt 0 ]]; then
                result="FATAL ($fatalCount) ($HOST_COUNT)"
            elif [[ $warningCount -gt 0 ]]; then
                result="WARNING ($warningCount) ($HOST_COUNT)"
            fi
        else
            typeset errorHostCount=$(f_getUniqueHosts "$ERRORS_FILE")
            result="ERROR ($errorHostCount/$HOST_COUNT)"
        fi
        
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
        typeset reportFileSizeInMb=$((reportFileSize/(1024*1024)))
        body=`echo "reportFile is too big (size=${reportFileSizeInMb}MB). Here's a snippet below. Check '$REPORT_FILE' on \`hostname\` for the full details."`
        body+=`cat $reportFileSnapshot`
        attachment="$reportFileSnapshot"
    fi
    
    f_sendEmailHelper "$to" "$from" "$RUN_ID $result - `basename $RUN_DIR`" "$body" "$attachment"
}

function f_getUniqueHosts {
    cut -d '_' -f 1 $1 | sort -n | uniq | wc -l
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
	
	echo -e "$body" | $MUTT -e "set copy=no" -e "my_hdr From:$from" $to -s "$subject" $attachmentsList  
    
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

typeset findErrorsScript=$PWD/find_log_errors.sh
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

typeset NUM_OF_META_DATA_LINES=2
typeset   FILE_NAME_LINE=1
typeset LINE_NUMBER_LINE=2
typeset START_LINE_NUMBER=1

f_checkIfAnySshCommandsAreStillRunning
f_findErrorsOnHosts
f_waitForAllErrorsToBeCollected
f_rerunAnyZeroSizeFiles
f_scrubFilesForIgnorableErrors
f_runErrorReport
f_sendEmail "$EMAILS" "sk_health_report_log-check_${RUN_ID}@the_real_silverking.com"