#!/bin/ksh

source `dirname $0`/lib/run_scripts_from_any_path.snippet
source lib/common.lib

f_checkAndSetBuildTimestamp "$1"

typeset output_filename=$(f_getBuild_RunOutputFilename)
{
	date
	echo
	echo "Build Flow"
	echo "1. Build Silverking"
	echo "2. Build Silverking client"
	echo "3. Build Silverking fs"
	echo
	
	f_startGlobalTimer

	f_printStep "1" "Build Silverking"
	./$BUILD_SILVERKING_SCRIPT_NAME
	f_startSilverking
	./$TEST_SILVERKING_SCRIPT_NAME

	typeset cc=$GPP
	f_printStep "2" "Build Silverking client"
	# ./$BUILD_JACE_SCRIPT_NAME "$cc"
	./$BUILD_SILVERKING_CLIENT_SCRIPT_NAME "$cc" "$GCC_LIB"
	./$TEST_SILVERKING_CLIENT_SCRIPT_NAME
	
	f_printStep "3" "Build Silverking FS"
	./$BUILD_SILVERKING_FS_SCRIPT_NAME "$cc" "$FUSE_INC" "$FUSE_LIB" "$SKFS_CC_D_FLAGS"
	f_startSkfs
	./$TEST_SILVERKING_FS_SCRIPT_NAME

	f_stopSkfs
	f_stopSilverking
	
	f_printSummary_BuildFlow

	f_printGlobalElapsed
	f_printFileOutputLine "$output_filename"
} 2>&1 | tee $output_filename
    
# needs to be outside the {} or else exit code won't be picked up. I think the tee still runs something even if we exit, which then turns the code to 0
f_exitIfFailed "$output_filename"
