#!/bin/ksh

cd ..
source lib/common.lib
cd -

function f_header {
    f_print "host" "SK" "SKFS"
}

function f_printBanner {
    f_print "---------------" "-----" "-----"
}

function f_print {
    printf "%-15s %-5s %-5s\n" "$1" "$2" "$3"
}

function f_status {
    typeset host=$1
    typeset skOutput=$2
    typeset skfsErrorCode=$3
    
    # echo "output: $skOutput"
    typeset actualCount=$(f_getProcessCount "$skOutput")
    # echo "actual: "$actualCount
    typeset skResult=$FAIL_TEXT
    if [[ $actualCount -eq 1 ]]; then
        skResult=$PASS_TEXT
    fi
    
    typeset skfsResult=$FAIL_TEXT
	if [[ $skfsErrorCode -eq 0 ]]; then
        skfsResult=$PASS_TEXT
    fi
    
    f_print "$host" "$skResult" "$skfsResult"
}

PASS_TEXT="x"
FAIL_TEXT=" "

toSecs="20s"

f_header
f_printBanner
typeset servers=`echo $SK_SERVERS | tr "," " "`
for server in $servers; do
    typeset pgrepCommand="pgrep -fl $SK_PROCESS_PATTERN"
    typeset skOutput=`timeout $toSecs ssh $server "$pgrepCommand | grep -v '$pgrepCommand'"`    # grep -v is to remove this actual ssh command from the count when the machine we are running this script from is also a server    # https://serverfault.com/questions/349454/making-ssh-truly-quiet
    
    timeout $toSecs ssh $server "ls -l $SKFS_MNT_AREA" > /dev/null 2>&1   # std out and err to /dev/null
    typeset skfsErrorCode=$?
    
    f_status "$server" "$skOutput" "$skfsErrorCode"
done


