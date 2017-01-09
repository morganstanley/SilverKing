#!/bin/ksh

source lib_common.sh

./$UNINSTALL_SILVERKING_FS_SCRIPT_NAME

f_printSubSection "Install: moving new install files"

# copy skfsd to bin/skfs 
f_copyVerbose "$SKFS_D" "../$BIN_FOLDER_NAME/skfs"  