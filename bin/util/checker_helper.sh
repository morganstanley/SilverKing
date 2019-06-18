#!/bin/bash

source lib/common.lib
    
typeset                 scriptName=$1
typeset                 folderDate=$2
typeset     datacenterAbbreviation=$3
typeset             datacenterName=$4
typeset           rootDataBasePath=$5
typeset runFrequencyIntervalInSecs=$6
typeset              gcDefaultBase=$7
typeset                     gcName=$8
typeset                  hostsFile=$9
typeset         passiveHostsGroups=${10}
typeset              exclusionFile=${11}
typeset    healthReportExcludeFile=${12}
typeset                  skLogPath=${13}
typeset               skFolderName=${14}
typeset                     sshCmd=${15}
typeset                       mutt=${16}
typeset                     emails=${17}
typeset                fatalEmails=${18}
typeset               phoneNumbers=${19}

f_checkFolderAndDatacenter "$scriptName" "$folderDate" "$datacenterAbbreviation"

if [[ -z "$rootDataBasePath" ]]; then
    rootDataBasePath="/tmp"
fi

if [[ -z "$gcDefaultBase" ]]; then
    echo "gcDefaultBase needs to be set"
    exit
fi

if [[ -z "$gcName" ]]; then
    echo "gcName needs to be set"
    exit
fi

if [[ -z "$hostsFile" ]]; then
    echo "hostsFile needs to be set"
    exit
fi

if [[ -z "$passiveHostsGroups" ]]; then
    echo "passiveHostsGroups needs to be set"
    exit
fi

if [[ -z "$exclusionFile" ]]; then
    echo "exclusionFile needs to be set"
    exit
fi

if [[ -z "$healthReportExcludeFile" ]]; then
    echo "healthReportExcludeFile needs to be set"
    exit
fi

if [[ -z "$skLogPath" ]]; then
    skLogPath="silverking"
fi

if [[ -z "$skFolderName" ]]; then
    skFolderName="silverking"
fi

if [[ -z "$sshCmd" ]]; then
    sshCmd="ssh -o StrictHostKeyChecking=no"
fi

if [[ -z "$mutt" ]]; then
    mutt="mutt"
fi

if [[ -z "$emails" ]]; then
    echo "emails needs to be set"
    exit
fi

if [[ -z "$fatalEmails" ]]; then
    echo "fatalEmails needs to be set"
    exit
fi

typeset      rootDataPath="$rootDataBasePath/health-report/$scriptName/$datacenterName/$folderDate"
typeset   RUNS_OUTPUT_DIR="$rootDataPath/hosts-data"
typeset REPORT_OUTPUT_DIR="$rootDataPath/report-data"

typeset delim="-"
typeset dateAndTime=`date +"%Y${delim}%m${delim}%d${delim}${delim}%H${delim}%M${delim}%S"`
typeset output_filename="/tmp/${USER}__${dateAndTime}__${datacenterName}__${scriptName}.out"
{
    typeset runNumber=0
    while [[ 1 ]]; do
        ((runNumber++))
        typeset startTime=$(f_startTimer)
        
        typeset runFolder=`date +"%Y${delim}%m${delim}%d__%H${delim}%M${delim}%S"`
        typeset           run_dir="$RUNS_OUTPUT_DIR/$runFolder"
        typeset report_output_dir="$REPORT_OUTPUT_DIR/$runFolder"
        
        echo "*** RUN $runNumber" 
    
        f_createRunDir "$run_dir"
        f_createReportDir "$report_output_dir"
        
        typeset hostsNotExcludedFile="$run_dir/hosts_not_excluded_sorted.txt"
        f_getHostsNotExcluded "$gcDefaultBase" "$run_dir" "$report_output_dir" "$gcName" "$hostsFile" "$passiveHostsGroups" "$exclusionFile" "$healthReportExcludeFile" "$hostsNotExcludedFile" 
        if [[ $scriptName =~ log_checker.sh$ ]]; then
            check_logs.sh       "$hostsNotExcludedFile" "/tmp/$skLogPath" "/var/tmp/$skFolderName/skfs/logs" "$run_dir" "$datacenterName" "$report_output_dir" "$sshCmd" "$mutt" "$emails" "$fatalEmails" "$phoneNumbers"
            f_sendNetcoolAlert  "$datacenterName" "$datacenterAbbreviation" "$report_output_dir"
        elif [[ $scriptName =~ mount_checker.sh$ ]]; then
            check_skfs_mount.sh "$hostsNotExcludedFile" "/var/tmp/$skFolderName/skfs/skfs_mnt/skfs"          "$run_dir" "$datacenterName" "$report_output_dir"           "$mutt" "$emails"
        elif [[ $scriptName =~ process_checker.sh$ ]]; then
            check_processes.sh  "$hostsNotExcludedFile" "/var/tmp/$skFolderName/skfs/skfs_mnt"               "$run_dir" "$datacenterName" "$report_output_dir"           "$mutt" "$emails"
        else
            echo "Unknown script to run: '$scriptName'"
            exit
        fi
        
        f_printElapsed $startTime
        echo "Next run: "`date +"%D @ %r" --date "now + $runFrequencyIntervalInSecs secs"`
        echo
        sleep $runFrequencyIntervalInSecs
    done
} 2>&1 | tee $output_filename