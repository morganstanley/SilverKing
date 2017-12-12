#!/bin/bash

old_dir=`pwd`
cd `dirname $0`
bin_dir=`pwd`

source lib/common.lib

allArgs="$@"

f_sourceGlobalVars
f_configureClasspath

export PATH=$PATH:/usr/bin
export      skKillCommand=`pwd`/kill_process_and_children.pl
export skCheckSKFSCommand=`pwd`/skfs/check_skfs.sh

mainClass=com.ms.silverking.cloud.dht.management.SKAdmin
cmd="${skAdminJavaCommandHeader} ${skJava} -cp ${classpath} ${mainClass} ${allArgs}"
echo ${cmd}
${cmd} 2>&1 | tee -a /tmp/SKAdmin.$$.stdout
javaExitCode=${PIPESTATUS[0]}

cd $old_dir
f_exit "$javaExitCode"
