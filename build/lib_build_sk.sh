#!/bin/ksh

source lib_common.sh

function f_checkForRequiredExecutables {
	f_printSection "Checking for Required Executables"
	f_getExecutablePath "ant"
	f_getExecutablePath "java"
}

function f_cleanAndMakeBuildDirectory {
	f_printSection "Cleaning and Remaking Build directory"
	f_cleanOrMakeDirectory "$SILVERKING_OUTPUT_BUILD_DIR"
}

function f_cleanAndMakeInstallDirectory {
	f_printSection "Cleaning and Remaking Install directory"
	f_cleanOrMakeDirectory "$SILVERKING_INSTALL_DIR"
}

function f_runAntScript {
	f_printSection "Running Ant"
	ant -v -f "$BUILD_DIR/$ANT_BUILD_SCRIPT_NAME"
	f_runChecks
}

function f_runChecks {
	f_printSection "CHECKS"
	
	typeset expectedNumClasses=1219
	f_testGreaterThanOrEquals "$OUT_CLASSES_SRC_DIR" "*" "$expectedNumClasses"
	f_testEquals "$OUT_CLASSES_TEST_DIR" "*Test.class" "27"
	
	f_testEquals "$OUT_JAR_DIR" "*.jar" "1" 
	f_testExists "$SILVERKING_JAR"
	typeset count=`jar tf $SILVERKING_JAR | grep '.class$' | wc -l`
	typeset result=$(f_getGreaterThanOrEqualsResult "$count" "$expectedNumClasses")
	f_printResult "$result" "# of jar .class files >= /$SRC_FOLDER_NAME folder" "$count"

	f_testGreaterThanOrEquals "$OUT_JAVADOC_DIR" "*.html" "1132"
	
	f_checkJunitTest
}

function f_checkJunitTest {
	f_printChecking "Junit Tests";
	
	typeset     runCount=$(f_getTestCount "run")
	typeset    failCount=$(f_getTestCount "Failures")
	typeset   errorCount=$(f_getTestCount "Errors")
	typeset skippedCount=$(f_getTestCount "Skipped")
	
	typeset totalFails=$((failCount+errorCount+skippedCount))
	typeset passCount=$((runCount-totalFails))
	
	typeset result=$(f_getEqualsResult "$totalFails" "0")
	f_printResult "$result" "all pass, no fails/errors/skipped" "total:${runCount} (p:${passCount},f:${failCount},e:${errorCount},s:${skippedCount})"
}

function f_getTestCount {
	typeset type=$1
	typeset filename=$(f_getBuildSk_RunOutputFilename)
	typeset count=`grep '\sTests' -w $filename | awk -F "${type}:" '{print $2}' | awk -F ',' '{print $1}' | awk '{s+=$1} END {print s}'`
	echo $count
}			  
