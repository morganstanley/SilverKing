#!/bin/ksh

source lib/common.lib

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
        previousLineNumber=-1
        f_logError "f_getPreviousLineNumber: $filename"
        # printf -u2 "no previousLineNumber: $filename"         # not working # this way we can still get this to the screen/output, but not picked up in the return value by people who call this function
        # echo "no previousLineNumber: $filename" /dev/stderr   # not working # since above isn't working: getting 'print: command not found'
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
    find $RUN_DIR -name '*_sk' -type f | xargs sed -i -r -e '/WARNING: 20..:..:.. ..:..:.. \S+ addSuspect: [[:digit:]]{1,3}\.[[:digit:]]{1,3}\.[[:digit:]]{1,3}\.[[:digit:]]{1,3}:[[:digit:]]+ ReplicaTimeout$/d'  
}

function f_runErrorReport {
    echo "Running error report"
    
    while read host; do
        f_checkFile "$RUN_DIR/${host}_sk"
        f_checkFile "$RUN_DIR/${host}_skfs"
    done < $HOSTS_FILE
    
    typeset previousRunDir=$(f_getPreviousRunDir)
    if [[ -n $previousRunDir ]]; then
        typeset hostFiles="*.txt"
        f_runDiff "$previousRunDir" "$RUN_DIR" "$hostFiles" "$TMP_OUTPUT_RUN_DIR/diff_hosts" "$HOST_DIFF_OUTPUT_FILE"
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
    f_logHelper "$1" "$SSH_CMDS_FILE"
}

function f_logFatalBreakdown {
    f_logHelper "$1" "$FATAL_COUNT_BREAKDOWN_FILE"
}

function f_sendEmail {
	typeset   to=$1
	typeset from=$2
    
    touch $REPORT_FILE
    
    typeset result;
    typeset resultInfo;
    if [[ (! -e $ERRORS_FILE) && (! -e $FAILS_FILE) && (! -e $DETAILS_FILE) && (! -e $HOST_DIFF_OUTPUT_FILE) ]]; then
        result="PASS"
        resultInfo="($HOST_COUNT)"
    else
        typeset  failHostCount=$(f_getUniqueHosts "$FAILS_FILE")
        typeset errorHostCount=$(f_getUniqueHosts "$ERRORS_FILE")
        typeset     fatalCount=0
        if [[ -e $FAILS_FILE ]]; then
            result="INFO"
            resultInfo="($failHostCount/$HOST_COUNT)"
            
            typeset -a fatalFails
            fatalFails+=("pbr_read received error from fbr_read")
            fatalFails+=("OutOfMemoryError")
            fatalFails+=(" TIMEOUT") # FIXME:bph: change " TIMEOUT" -> "\s+TIMEOUT" so we actually match these as FATAL
            fatalFails+=(" wf_write_block_sync failed ") 
            fatalFails+=("Fatal Error") 
            # add "*** check this machine" ? 
            typeset lastIndex=$((${#fatalFails[@]}-1))
            for i in {0..$lastIndex}; do
                typeset pattern=${fatalFails[$i]}
                typeset patternCount=`grep -Pc "$pattern" "$DETAILS_FILE"`
                if [[ $patternCount -gt 0 ]]; then
                    typeset breakDown=`printf "%40s -> %s" "$pattern" "$patternCount"`
                    f_logFatalBreakdown "$breakDown"
                    fatalCount=$((fatalCount+patternCount))
                fi
            done
            
            typeset warningFails="KeeperErrorCode = ConnectionLoss for |KeeperException| EST sendFailed/|java.lang.UnsupportedOperationException|A fatal error has been detected by the Java Runtime Environment|fbr_read failed|terminate called after throwing an instance of"
            typeset warningCount=`grep -Pc "$warningFails" "$DETAILS_FILE"`
            
            if [[ $fatalCount -gt 0 ]]; then
                result="FATAL"
                resultInfo="($fatalCount) | ($failHostCount/$HOST_COUNT)"
                to+=",$FATAL_EMAILS"
            elif [[ $warningCount -gt 0 ]]; then
                result="WARNING"
                resultInfo="($warningCount) | ($failHostCount/$HOST_COUNT)"
            fi
        else
            result="ERROR"
            resultInfo="($errorHostCount/$HOST_COUNT)"
        fi
        
        f_logReportSection "$REPORT_FILE" "FATAL BREAKDOWN"  "$FATAL_COUNT_BREAKDOWN_FILE"  " ($fatalCount)"
        f_logReportSection "$REPORT_FILE" "ERRORS"           "$ERRORS_FILE"                 " ($errorHostCount)"
        f_logReportSection "$REPORT_FILE" "FAILS"            "$FAILS_FILE"                  " ($failHostCount)"
        f_logReportSection "$REPORT_FILE" "DETAILS"          "$DETAILS_FILE"
                
        typeset failIndicators;
        if [[ -e $HOST_DIFF_OUTPUT_FILE ]]; then
            failIndicators="(-)"
            f_logReportSection "$REPORT_FILE" "HOST_EXCLUSIONS" "$HOST_DIFF_OUTPUT_FILE"
        fi
        
        typeset thresholdPercent=10
        typeset actualPercent=$((failHostCount*100 / HOST_COUNT))   # *100 is to get a percentage - if you *100 at the end, the failHostCount / HOST_COUNT will be 0 *100
        if [[ $actualPercent -ge $thresholdPercent ]]; then
            failIndicators+="(${actualPercent}%)"
        fi
        
        resultInfo+="$failIndicators"
    fi
    
    echo "$result" >> $RESULT_FILE
    typeset resultLine="$result $resultInfo"
    echo -e "\tresult: $resultLine"
    
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
        body+=`echo ""`
        body+=`cat $reportFileSnapshot`
        attachment="$reportFileSnapshot"
    fi
    
    typeset subject="$RUN_ID $resultLine - `basename $RUN_DIR`"
    echo "$subject" > "$TMP_OUTPUT_RUN_DIR/email.subject"
    
    f_sendEmailHelper "$to" "$from" "$subject" "$body" "$attachment" "$MUTT"
}

function f_getUniqueHosts {
    typeset file=$1
    
    typeset numUniqHosts=0;
    if [[ -e $file ]]; then
        numUniqHosts=`cut -d '_' -f 1 $file | sort -n | uniq | wc -l`
    fi
    
    echo $numUniqHosts
}

function f_getFileSize {
    typeset file=$1
    ls -l "$file" | cut -d ' ' -f 5
}

function f_sendText {
    typeset to=$1
    typeset from=$2
    
    if [[ -n $to ]]; then
        if [[ `cat $RESULT_FILE` == "FATAL" ]]; then 
            f_sendTextHelper "$to" "$from" "Fatal" "check it" "" "$MUTT" 
        fi
    fi
}

        HOSTS_FILE=$1
        SK_LOG_DIR=$2
      SKFS_LOG_DIR=$3
           RUN_DIR=$4
            RUN_ID=$5
TMP_OUTPUT_RUN_DIR=$6
           SSH_CMD=$7
              MUTT=$8
            EMAILS=$9
      FATAL_EMAILS=${10}
     PHONE_NUMBERS=${11}

typeset findErrorsScript=$PWD/find_log_errors.sh
typeset fiveMinsInSecs=300
typeset TIMEOUT_SSH="timeout $fiveMinsInSecs $SSH_CMD"
typeset   SK_SSH_CMD="$findErrorsScript   $SK_LOG_DIR"
typeset SKFS_SSH_CMD="$findErrorsScript $SKFS_LOG_DIR"

typeset        ALL_RUNS_OUTPUT_DIR=`dirname $RUN_DIR`
typeset      HOST_DIFF_OUTPUT_FILE="$TMP_OUTPUT_RUN_DIR/diff_hosts.out"
typeset                 HOST_COUNT=$(f_getNumberOfLines "$HOSTS_FILE")
typeset                EMPTY_FILES="$TMP_OUTPUT_RUN_DIR/empty_files.out"
typeset                RESULT_FILE="$TMP_OUTPUT_RUN_DIR/run.result"
typeset                ERRORS_FILE="$TMP_OUTPUT_RUN_DIR/run.errors"
typeset                 FAILS_FILE="$TMP_OUTPUT_RUN_DIR/run.fails"
typeset               DETAILS_FILE="$TMP_OUTPUT_RUN_DIR/run.details"
typeset                REPORT_FILE="$TMP_OUTPUT_RUN_DIR/report.out"
typeset              SSH_CMDS_FILE="$TMP_OUTPUT_RUN_DIR/ssh.cmds"
typeset FATAL_COUNT_BREAKDOWN_FILE="$TMP_OUTPUT_RUN_DIR/fatals.breakdown"

typeset NUM_OF_META_DATA_LINES=2
typeset         FILE_NAME_LINE=1
typeset       LINE_NUMBER_LINE=2
typeset      START_LINE_NUMBER=1

f_checkIfAnySshCommandsAreStillRunning
f_findErrorsOnHosts
f_waitForAllErrorsToBeCollected
f_rerunAnyZeroSizeFiles
f_scrubFilesForIgnorableErrors
f_runErrorReport
f_sendEmail "$EMAILS" "sk_health_report_log-check_${RUN_ID}@ms_silverking.com"
# f_sendText "$PHONE_NUMBERS" "alerts@silverking.com"
