#!/bin/bash

old_dir=`pwd`
cd `dirname $0`
bin_dir=`pwd`

source lib/common.lib

allArgs="$@"

f_sourceSkConfigAndConfigureClasspath "$__SK_CLASSPATH_TYPE"

toolClass=$__SK_TOOL_EXEC_TOOL_CLASS
cmd="$skJavaHome/bin/java -ea -cp $classpath $toolClass $allArgs"
$cmd
javaExitCode=$?

cd $old_dir
f_exit "$javaExitCode"
