#!/bin/bash

typeset       dir=$1
typeset startLine=$2

typeset logType;
typeset file;
typeset grepLine;
if [[ $dir =~ logs$ ]]; then
    logType=skfs
    typeset latestLogFile=`ls -rt $dir/* | tail -n 1`   # /* so we get absolute filenames
    file="$latestLogFile"
    grepLine="timeout|fbr_read| fail |failed |failure|exception|error|terminate"
    
    if [[ ! `basename $file` =~ ^fuse.log. ]]; then
        echo "Error $file: file doesn't start with 'fuse.log.'"
        ls -AltrFh $dir
        exit
    fi
else
    logType=sk
    file=$dir/Daemon.out
    grepLine="timeout|fail|exception|error|terminate"
    
    if [[ ! -e $file ]]; then
        echo "Error $file: file doesn't exist"
        ls -l `dirname $file`
    fi
fi

typeset lastLine=`wc -l $file | cut -d ' ' -f 1`
echo $file
echo $lastLine

typeset errorStreakThreshold=2
typeset counterFile=/tmp/cvaCounterFile.$logType
if [[ $startLine -gt $lastLine ]]; then
    typeset currentStreak;
    if [[ -e $counterFile ]]; then
        typeset currentCount=`cat $counterFile`
        currentStreak=$((currentCount+1))
        echo "$currentStreak" > $counterFile
    else
        echo "1" > $counterFile
    fi
    
    typeset errorMsg;
    if [[ $currentStreak -ge $errorStreakThreshold ]]; then
        errorMsg="*** check this machine this is atleast the ${currentStreak}th time in a row this has happened"
    fi
    echo "Error $file: $startLine > $lastLine. startLine=$startLine, but there are only $lastLine lines in this file $errorMsg"
else
    rm $counterFile
    tail -n +$startLine $file | grep -Pni "$grepLine"
fi
