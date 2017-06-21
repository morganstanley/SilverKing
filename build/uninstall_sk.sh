#!/bin/ksh

source lib/common.lib

f_printSubSection "Uninstall: removing old install files"

f_removeVerbose "../$LIB_FOLDER_NAME/$SILVERKING_JAR_NAME"
f_remove        "../$DOC_FOLDER_NAME/$JAVADOC_FOLDER_NAME"

f_removeVerboseList "cltoolc exampledht kill_process_and_children.pl q3testdht.q skcc testdht testdht.pl testdht.q"                "../$BIN_FOLDER_NAME"
f_removeVerboseList "kdb kdb3 perl5 perl5.8 perl5.10 perl5.14 libjsilverking.a libjsilverking.so libsilverking.a libsilverking.so" "../$LIB_FOLDER_NAME"

f_remove        "../$INCLUDE_FOLDER_NAME"