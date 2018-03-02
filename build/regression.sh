#!/bin/bash

source lib/common.lib
source lib/common_regression.lib

function f_copyBuildConfig {
	cd $REPO_NAME/$BUILD_NAME
	cp $build_area/$BUILD_CONFIG_FILE_NAME .
}

function f_run {
	#cd $REPO_NAME/$BUILD_NAME already cd'ing when we copy dependencies
	./$BUILD_SCRIPT_NAME
}

# params
typeset build_area=`pwd`
typeset regression_area=$1
typeset extra_options=$2
typeset email_addresses=$3 

f_checkAndCdToRegressionArea $regression_area
f_makeSetAndChangeToFolder
f_setBuildTimestamp "$FOLDER_NAME" "REGRESSION_${extra_options}"

typeset output_filename=$(f_getRegression_RunOutputFilename) # needs to be after f_setBuildTimestamp or else filename won't be set right
{
	f_checkoutRepo
	f_copyBuildConfig
	f_run
	f_removeOldRegressions "$regression_area"
	f_emailResults "$output_filename" "$email_addresses"	# this line is after remove regressions, because I want the removed regression list in the outputfile
	f_printFileOutputLine "$output_filename"
} 2>&1 | tee $output_filename