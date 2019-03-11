#!/bin/bash

source lib/common.lib

function f_createDummyDirIfItDoesntExist  {
    if [[ ! -e $DUMMY_DIR ]]; then
        echo "Making `basename $DUMMY_DIR`"
        mkdir -p $DUMMY_DIR
    fi
}

function f_runRingIntegrityCheck {
    export GC_DEFAULT_BASE=$GC_DEFAULT_BASE_PATH; ../RingIntegrityCheck.sh -g $GC_NAME > $RUN_OUTFILE
}

function f_getNumberOfSegments {
    grep -Pc "\[.*\]\s+\d+\s+\d+" $RUN_OUTFILE
}

function f_runErrorReport {
    echo "Running error report"
    
    grep -Pi "setsExcluded:|sets have|setsize|set size" $RUN_OUTFILE > $DETAILS_FILE
    f_logReplicaSetSizesOf "0" "$ZEROS_FILE"
    f_logReplicaSetSizesOf "1" "$ONES_FILE"
    f_logReplicaSetSizesOf "2" "$TWOS_FILE"
}

function f_logReplicaSetSizesOf {
    typeset setSize=$1
    typeset logFile=$2
    
    grep -P "\[.*\]\s+\d+\s+${setSize}" $RUN_OUTFILE > $logFile
    if [[ $(f_getNumberOfLines "$logFile") -eq 0 ]]; then
        rm $logFile
    fi
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
            f_logError "expected currentRunDir: '$RUN_DIR', actual: '$currentRunDir'\n"
        fi
        
        typeset isDummyTheLastDir=`echo "$lastRunDir" | grep -c "dummy"`
        if [[ $isDummyTheLastDir -eq 1 ]]; then
            f_createDummyFiles "$currentRunDir"
            lastRunDir=$DUMMY_DIR
        fi
    else
        f_logError "ERROR: something went wrong"
    fi
    
    f_runDiff "$lastRunDir" "$currentRunDir" "run.zeros" "$currentRunDir/diff_zeros" "$DIFF_ZEROS_FILE"
    f_runDiff "$lastRunDir" "$currentRunDir" "run.ones"  "$currentRunDir/diff_ones"  "$DIFF_ONES_FILE"
    f_runDiff "$lastRunDir" "$currentRunDir" "run.twos"  "$currentRunDir/diff_twos"  "$DIFF_TWOS_FILE"
}

function f_createDummyFiles {
    typeset referenceDir=$1
    
    echo -en "\tFirst run - creating dummy files based off of '`basename $referenceDir`'..."
    
    for file in `ls $referenceDir`; do
        touch $DUMMY_DIR/$file
    done
    
    # trick to make referenceDir modified more recently than dummy for f_compareOldAndNewErrorReports when we sort on time
    typeset testFile=$referenceDir/test
    touch $testFile
    rm    $testFile
    
    echo "done"
}

function f_sendEmail {
	typeset   to=$1
	typeset from=$2
    
    touch $REPORT_FILE
    
    if [[ -e $DIFF_ZEROS_FILE ]]; then
        result="FATAL"
    elif [[ -e $DIFF_ONES_FILE || -e $DIFF_TWOS_FILE ]]; then
        result="WARNING"
    else
        result="PASS"
    fi
    resultInfo="($RING_SEGMENTS_COUNT)"
        
    f_logDiffResult "$REPORT_FILE" "ZEROS" "$DIFF_ZEROS_FILE" "$ZEROS_FILE"
    f_logDiffResult "$REPORT_FILE" "ONES"  "$DIFF_ONES_FILE"  "$ONES_FILE"
    f_logDiffResult "$REPORT_FILE" "TWOS"  "$DIFF_TWOS_FILE"  "$TWOS_FILE"
    f_logReportSection "$REPORT_FILE" "DETAILS" "$DETAILS_FILE"
    
    echo "$result" >> $RESULT_FILE
    typeset resultLine="$result $resultInfo"
    echo -e "\tresult: $resultLine"
    cat $DETAILS_FILE
    
    typeset body=`cat $REPORT_FILE`
    typeset subject="$RUN_ID $resultLine - `basename $RUN_DIR`"
    echo "$subject" > "$RUN_DIR/email.subject"
    
    if [[ $result != "PASS" ]]; then
        f_sendEmailHelper "$to" "$from" "$subject" "$body" "" "$MUTT"
    fi
}

function f_logDiffResult {
    typeset  reportFile=$1
    typeset sectionName=$2
    typeset    diffFile=$3
    typeset     runFile=$4
    
    typeset sectionFile="$diffFile"
    typeset pattern="^Only in /new"
    if [[ $(f_getNumberOfLines "$diffFile") -eq 1 && `cat $diffFile` =~ $pattern ]]; then
        sectionFile="$runFile"
    fi
    
    f_logReportSection "$reportFile" "$sectionName" "$sectionFile"
}

             RUN_DIR=$1
             GC_NAME=$2
GC_DEFAULT_BASE_PATH=$3
              RUN_ID=$4
                MUTT=$5
              EMAILS=$6

typeset ALL_RUNS_OUTPUT_DIR=`dirname $RUN_DIR`
typeset           DUMMY_DIR=$ALL_RUNS_OUTPUT_DIR/dummy
typeset DIFF_ZEROS_FILE="$RUN_DIR/diff_zeros.out"
typeset  DIFF_ONES_FILE="$RUN_DIR/diff_ones.out"
typeset  DIFF_TWOS_FILE="$RUN_DIR/diff_twos.out"
typeset     RUN_OUTFILE="$RUN_DIR/run.out"
typeset     RESULT_FILE="$RUN_DIR/run.result"
typeset      ZEROS_FILE="$RUN_DIR/run.zeros"
typeset       ONES_FILE="$RUN_DIR/run.ones"
typeset       TWOS_FILE="$RUN_DIR/run.twos"
typeset     ERRORS_FILE="$RUN_DIR/run.errors"
typeset    DETAILS_FILE="$RUN_DIR/run.details"
typeset     REPORT_FILE="$RUN_DIR/report.out"

f_createDummyDirIfItDoesntExist
f_runRingIntegrityCheck
typeset RING_SEGMENTS_COUNT=$(f_getNumberOfSegments "$RUN_OUTFILE")
f_runErrorReport
f_compareOldAndNewErrorReports
f_sendEmail "$EMAILS" "sk_health_report_ring-check_${RUN_ID}@ms_silverking.com"