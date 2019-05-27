#!/bin/ksh

source `dirname $0`/lib/run_scripts_from_any_path.snippet
source lib/common_test.lib

f_clearOutEnvVariables
f_checkAndSetBuildTimestamp

typeset output_filename=$(f_getTestSkClient_RunOutputFilename)
{
    f_startLocalTimer;
    date;
    f_runGTests "$1" "$2" "$3" "$4" "$output_filename"
    f_printTestSummary "$output_filename"
    f_printLocalElapsed;
} 2>&1 | tee $output_filename

