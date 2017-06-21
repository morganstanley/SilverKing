#!/bin/bash

source lib/common.lib
source lib/common_regression.lib

function f_copyBuildConfig {
	cd $OSS_REPO_NAME/$BUILD_NAME
	cp $BUILD_AREA/$BUILD_CONFIG_FILE .
}

function f_run {
	#cd $OSS_REPO_NAME/$BUILD_NAME already cd'ing when we copy dependencies
	./$BUILD_SCRIPT_NAME
}


     BUILD_AREA=`pwd`
REGRESSION_AREA=$1
  EXTRA_OPTIONS=$2
EMAIL_ADDRESSES=$3 

f_checkAndCdToRegressionArea $REGRESSION_AREA
f_makeSetAndChangeToFolder
f_setBuildTimestamp "$FOLDER_NAME" "REGRESSION_${EXTRA_OPTIONS}"

output_filename=$(f_getRegression_RunOutputFilename) # needs to be after f_setBuildTimestamp or else filename won't be set right
{
	f_checkoutOss
	f_copyBuildConfig
	f_run
	f_removeOldRegressions "$REGRESSION_AREA"
	f_emailResults "$output_filename" "$EMAIL_ADDRESSES"	# this line is after remove regressions, because I want the removed regression list in the outputfile
	f_printFileOutputLine "$output_filename"
} 2>&1 | tee $output_filename