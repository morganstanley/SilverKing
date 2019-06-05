#!/bin/ksh

source `dirname $0`/lib/run_scripts_from_any_path.snippet
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
                  SK_QUIET_OUTPUT=$SK_QUIET_OUTPUT
      SK_SKIP_MULTI_MACHINE_TESTS=$SK_SKIP_MULTI_MACHINE_TESTS
    "
    f_runTestAntScript "testSkOnly-large" "$output_filename" "$TEST_SILVERKING_LARGE_EXPECTED_COUNT" "$vars";
    f_printTestSummary "$output_filename";
    f_printLocalElapsed;
 } 2>&1 | tee $output_filename

