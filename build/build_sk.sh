#!/bin/ksh

source lib/build_sk.lib

f_clearOutEnvVariables
f_checkAndSetBuildTimestamp

typeset output_filename=$(f_getBuildSk_RunOutputFilename)
{
	f_startLocalTimer;
	date;
	# f_checkForRequiredExecutables;    currently not working how I want it to. Need to check them from build.config
	f_cleanOrMakeBuildDirectory;
	f_cleanOrMakeInstallDirectory;
	f_runBuildAntScript;
	f_printSummary "$output_filename";
	f_printLocalElapsed;
 } 2>&1 | tee $output_filename

