#!/bin/ksh

source lib/common.lib

./$UNINSTALL_SILVERKING_SCRIPT_NAME

f_printSubSection "Install: moving new install files"

f_copyVerbose "$SILVERKING_JAR"  "../$LIB_FOLDER_NAME"	# copy silverking jar to lib
f_copy        "$OUT_JAVADOC_DIR" "../$DOC_FOLDER_NAME"	# copy javadoc to doc

# cp -rv $INSTALL_ARCH_BIN_DIR $../$BIN_FOLDER_NAME	# no inline, creates a bin/ folder inside bin/
# the "*" makes it copy each file and directory, inline (directly into bin)
f_copyVerbose "$INSTALL_ARCH_BIN_DIR/*" "../$BIN_FOLDER_NAME" # copy silverking_install/arch_output_area/bin to bin	
f_copyVerbose "$INSTALL_ARCH_LIB_DIR/*" "../$LIB_FOLDER_NAME" # copy silverking_install/arch_output_area/lib to lib

f_copy		  "$INSTALL_COMMON_INC_DIR" "../$INCLUDE_FOLDER_NAME"
f_remove      "../$INCLUDE_FOLDER_NAME/jace"