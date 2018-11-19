#!/bin/ksh

source `dirname $0`/lib/run_scripts_from_any_path.snippet
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
    typeset sshVarForFileWriteWithDelayTest=$(f_getSshVar)
	vars+="
	               IOZONE_BIN=$IOZONE_BIN
	             TRUNCATE_BIN=$TRUNCATE_BIN
	               SK_SERVERS=$SK_SERVERS
	                 JAVA_BIN=$JAVA_8
	             SK_CLASSPATH=$skClasspath
     SK_FILE_WRITER_FILE_SIZE=$SK_FILE_WRITER_FILE_SIZE
                   $sshVarForFileWriteWithDelayTest
	"
	f_runTestAntScript "testSkfsOnly-large" "$output_filename" "$TEST_SILVERKING_FS_LARGE_EXPECTED_COUNT" "$vars";
	f_printTestSummary "$output_filename";
	f_printLocalElapsed;
 } 2>&1 | tee $output_filename

