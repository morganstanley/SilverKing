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
        f_logSshCmd "$TIMEOUT_SSH $host $SSH_CMD"   
        $TIMEOUT_SSH $host "$SSH_CMD" </dev/null > $RUN_DIR/$host 2>/dev/null &
        usleep 100000
        ((count++))
    done < $HOSTS_FILE
    
    echo
}

function f_runErrorReport {
    echo "Checking for zero size files in '`basename $RUN_DIR`'..."
    f_checkForAnyZeroSizeFiles "$RUN_DIR"
}
    
function f_checkForAnyZeroSizeFiles {
    typeset dir=$1
        
    for file in `ls $dir | grep -v ".txt$"`; do
        typeset numOfLines=$(f_getNumberOfLines "$dir/$file")
        if [[ $numOfLines -ne 3 ]]; then
            f_logFailFile "$file"
        fi
    done 
}

function f_logFailFile {
    f_logHelper "$1" "$FAILS_FILE"
}

function f_logSshCmd {
    f_logHelper "$1" "$TMP_OUTPUT_RUN_DIR/ssh.cmds"
}

function f_logHelper {
    typeset  msg=$1
    typeset file=$2
    
    echo -e "$msg" >> $file
}

function f_sendEmail {
	typeset to=$1
	typeset from=$2
    
    touch $REPORT_FILE
    
    typeset result;
    if [[ ! -e $FAILS_FILE ]]; then
        result="PASS ($HOST_COUNT)"
    else
        typeset failHostCount=$(f_getNumberOfLines "$FAILS_FILE")
        result="FAIL ($failHostCount/$HOST_COUNT)"
        
        typeset thresholdPercent=10
        typeset actualPercent=$((failHostCount / HOST_COUNT))
        if [[ $actualPercent -ge $thresholdPercent ]]; then
            result+="(${actualPercent}%)"
        fi
        
        f_logReportSection "FAILS" "$FAILS_FILE"
	fi
    
    echo -e "\tresult: $result"
    
    typeset body=`cat $REPORT_FILE`
    typeset attachment="$REPORT_FILE"
    
    f_sendEmailHelper "$to" "$from" "$RUN_ID pc $result - `basename $RUN_DIR`" "$body" "$attachment"
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
     SKFS_MNT_PATH=$2
           RUN_DIR=$3
            RUN_ID=$4
TMP_OUTPUT_RUN_DIR=$5
              MUTT=$6
            EMAILS=$7
typeset fifteenSecs=15
typeset TIMEOUT_SSH="timeout $fifteenSecs ssh"
typeset   SSH_CMD="pgrep -fl com.ms.silverking.cloud.dht.daemon.DHTNode; pgrep -fl 'skfsd --mount=$SKFS_MNT_PATH'"

typeset HOST_COUNT=$(f_getNumberOfLines "$HOSTS_FILE")
typeset            FAILS_FILE="$TMP_OUTPUT_RUN_DIR/run.fails"
typeset           REPORT_FILE="$TMP_OUTPUT_RUN_DIR/report.out"

f_checkIfAnySshCommandsAreStillRunning
f_findErrorsOnHosts
f_runErrorReport
f_sendEmail "$EMAILS" "sk_health_report_process-check_${RUN_ID}@the_real_silverking.com"