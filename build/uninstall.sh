#!/bin/ksh

source `dirname $0`/lib/run_scripts_from_any_path.snippet
source lib/common.lib

./$UNINSTALL_SILVERKING_SCRIPT_NAME
./$UNINSTALL_SILVERKING_FS_SCRIPT_NAME