#!/bin/bash

old_dir=`pwd`
cd `dirname $0`
bin_dir=`pwd`

source lib/common.lib

allArgs="$@"

f_sourceGlobalVars
f_configureClasspath 

toolClass=${__SK_TOOL_EXEC_TOOL_CLASS}
cmd="${skJava} -ea -cp $classpath ${toolClass} ${allArgs}"
${cmd}
exitCode=$?

cd $old_dir
exit $exitCode 
