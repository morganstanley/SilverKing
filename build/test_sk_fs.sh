#!/bin/ksh

source lib/common_test.lib

f_clearOutEnvVariables
f_checkAndSetBuildTimestamp

typeset output_filename=$(f_getTestSkfs_RunOutputFilename)
{
	f_startLocalTimer;
	date;
	typeset vars="
	 SK_FOLDER_NAME=$SK_FOLDER_NAME
	"
	f_runTestAntScript "testSkfsOnly-small" "$output_filename" "$TEST_SILVERKING_FS_SMALL_EXPECTED_COUNT" "$vars";
		
	typeset skClasspath=$(f_getSkClasspath)
	vars+="
	     IOZONE_BIN=$IOZONE_BIN
	   TRUNCATE_BIN=$TRUNCATE_BIN
	     SK_SERVERS=$SK_SERVERS
	       JAVA_BIN=$JAVA_8
	   SK_CLASSPATH=$skClasspath
	"
	f_runTestAntScript "testSkfsOnly-large" "$output_filename" "$TEST_SILVERKING_FS_LARGE_EXPECTED_COUNT" "$vars";
	f_printTestSummary "$output_filename";
	f_printLocalElapsed;
 } 2>&1 | tee $output_filename

