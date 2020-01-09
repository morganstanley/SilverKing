#!/bin/bash

old_dir=`pwd`
cd `dirname $0`
bin_dir=`pwd`

source lib/common.lib

allArgs="$@"
internalArgs="-Dcom.ms.silverking.process.SafeThread.DefaultUncaughtExceptionHandler=com.ms.silverking.process.LogAndExitUncaughtExceptionHandler"

f_sourceSkConfigAndConfigureClasspath "$__SK_CLASSPATH_TYPE"

toolClass=$__SK_TOOL_EXEC_TOOL_CLASS
cmd="$skAdminJavaCommandHeader $skJavaHome/bin/java -ea -cp $classpath $internalArgs $toolClass $allArgs"
f_logStart "$toolClass" "$allArgs"
typeset skAdminPattern="SKAdmin"
if [[ $toolClass =~ ${skAdminPattern}$ ]]; then   # quotes around "SKAdmin$" makes the if statement fail for some reason...
    export PATH=$PATH:/usr/bin
    export      skKillCommand=`pwd`/kill_process_and_children.pl
    export skCheckSKFSCommand=`pwd`/skfs/check_skfs.sh
    echo $cmd
    $cmd 2>&1 | tee -a /tmp/${skAdminPattern}.$$.stdout
    javaExitCode=${PIPESTATUS[0]}
else
    $cmd
    javaExitCode=$?
fi
    
f_logStop "$toolClass" "$allArgs"
cd $old_dir
f_exit "$javaExitCode"
