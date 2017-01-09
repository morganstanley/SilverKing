#!/bin/bash

allArgs="$@"

old_dir=`pwd`
cd `dirname $0`
bin_dir=`pwd`

#####################################################################################
cd "${bin_dir}"
source sk.global.vars

#####################################################################################

toolClass=${__SK_TOOL_EXEC_TOOL_CLASS}

if [[ -n "${skGlobalCodebase}" ]]; then      
    echo "Using skGlobalCodebase"
    classpath=${skGlobalCodebase}
    skJava=${skDevJavaHome}/bin/java
else
    cd ../lib
    cpBase=`pwd`
    cp=""
    for f in `ls $cpBase/*.jar`; do
            cp=$cp:$f
    done
    classpath=$cp
    cd "${bin_dir}"
fi

#####################################################################################

cd "${bin_dir}"

cmd="${skJava} -ea -cp $classpath ${toolClass} ${allArgs}"

${cmd}

cd $old_dir
