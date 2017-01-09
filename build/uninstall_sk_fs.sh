#!/bin/ksh

source lib_common.sh

f_printSubSection "Uninstall: removing old install files"

f_removeVerbose "../$BIN_FOLDER_NAME/skfs/$SKFS_EXEC_NAME"