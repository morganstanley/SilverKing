#!/bin/ksh

source `dirname $0`/../lib/run_scripts_from_any_path.snippet

cd ..
source lib/common.lib
cd -

f_checkAndSetBuildTimestamp

typeset output_filename=$(f_getStartInstance_RunOutputFilename)
{
    f_startAll
} 2>&1 | tee $output_filename
