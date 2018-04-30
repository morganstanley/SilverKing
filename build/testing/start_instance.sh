#!/bin/ksh

cd ..
source lib/common.lib
cd -

f_checkAndSetBuildTimestamp

typeset output_filename=$(f_getStartInstance_RunOutputFilename)
{
    f_startAll
} 2>&1 | tee $output_filename
