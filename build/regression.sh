#!/bin/bash

source lib_common.sh
source lib_common_regression.sh

function f_run {
	f_setBuildTimestamp "$FOLDER_NAME" "$EXTRA_OPTIONS"
	cd $OSS_REPO_NAME/$BUILD_NAME
	./$BUILD_SCRIPT_NAME
}

REGRESSION_AREA=$1
  EXTRA_OPTIONS=$2

f_checkAndCdToRegressionArea $REGRESSION_AREA
f_makeSetAndChangeToFolder
f_checkoutOss
f_run
f_sendEmail $(f_getBuild_RunOutputFilename) "xxx_fill_me_in_xxx@fix_me_please.com"
