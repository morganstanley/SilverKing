#!/bin/bash

typeset dir=$1
typeset latestLogFile=`ls -rt $dir/* | tail -n 1`   # /* so we get absolute filenames
grep -Pni "timeout|fbr_read| fail |failed |failure|exception|error|terminate" $latestLogFile
