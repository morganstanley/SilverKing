#!/bin/ksh

source lib_build_sk.sh

f_clearOutEnvVariables
f_checkAndSetBuildTimestamp

# ANT_PATH=$(f_getExecutablePath ant);
# $(f_getExecutablePath java);
		
# need to export so that ant command will pick up new variable
# these variables "export scope" only lasts for this file, so no need to save/restore each value
export      PATH=$JAVA_8_HOME/bin:$ANT_9_HOME/bin:$PATH
export JAVA_HOME=$JAVA_8_HOME
export  ANT_HOME=$ANT_9_HOME

output_filename=$(f_getBuildSk_RunOutputFilename)
{
	f_startLocalTimer;
	date;
	f_checkForRequiredExecutables;
	f_cleanAndMakeBuildDirectory;
	f_cleanAndMakeInstallDirectory;
	f_runAntScript;
	f_printSummary "$output_filename";
	f_printLocalElapsed;
 } 2>&1 | tee $output_filename

