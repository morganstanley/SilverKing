#!/bin/ksh

cd ..
source lib/common.lib
cd -

f_checkAndSetBuildTimestamp

typeset output_filename=$(f_getStopInstance_RunOutputFilename)
{
    f_stopAll
} 2>&1 | tee $output_filename