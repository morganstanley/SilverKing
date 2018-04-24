#!/bin/ksh

source lib/common_test.lib

f_clearOutEnvVariables
f_checkAndSetBuildTimestamp

typeset output_filename=$(f_getTestSk_RunOutputFilename)
{
	f_startLocalTimer;
	date;
	f_runTestAntScript "testSkOnly-small" "$output_filename" "$TEST_SILVERKING_SMALL_EXPECTED_COUNT";
	typeset vars="
	     GC_DEFAULT_BASE=$SK_GRID_CONFIG_DIR
	 SK_GRID_CONFIG_NAME=$SK_GRID_CONFIG_NAME
	          SK_SERVERS=$SK_SERVERS
	"
	f_runTestAntScript "testSkOnly-large" "$output_filename" "$TEST_SILVERKING_LARGE_EXPECTED_COUNT" "$vars";
	f_printTestSummary "$output_filename";
	f_printLocalElapsed;
 } 2>&1 | tee $output_filename

