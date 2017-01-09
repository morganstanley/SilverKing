#!/bin/bash

allArgs="$@"

export PATH=$PATH:/usr/bin

old_dir=`pwd`
cd `dirname $0`
bin_dir=`pwd`

#####################################################################################
cd "${bin_dir}"
source sk.global.vars

export skKillCommand=`pwd`/kill_process_and_children.pl
export skCheckSKFSCommand=`pwd`/skfs/check_skfs.sh

#####################################################################################

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

mainClass=com.ms.silverking.cloud.dht.management.SKAdmin
cmd="${skAdminJavaCommandHeader} ${skJava} -cp ${classpath} ${mainClass} ${allArgs}"
echo ${cmd}
${cmd} 2>&1 | tee -a /tmp/SKAdmin.$$.stdout
echo $?

cd $old_dir
