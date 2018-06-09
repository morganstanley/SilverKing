#!/bin/ksh

source `dirname $0`/lib/run_scripts_from_any_path.snippet
source lib/common.lib

./$INSTALL_SILVERKING_SCRIPT_NAME
./$INSTALL_SILVERKING_FS_SCRIPT_NAME