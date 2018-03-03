#!/bin/ksh

source lib/common.vars	# really only wanted build.config, but we are using some variables like TRUE/FALSE

function f_check_AR {
	f_check_defined_and_which_and_ends_with ${!AR} "/bin/ar"
}
function f_check_CAT {
	f_check_defined_and_which_and_ends_with ${!CAT} "/bin/cat"
}
function f_check_SED {
	f_check_defined_and_which_and_ends_with ${!SED} "/bin/sed"
}
function f_check_CHMOD {
	f_check_defined_and_which_and_ends_with ${!CHMOD} "/bin/chmod"
}
function f_check_CP {
	f_check_defined_and_which_and_ends_with ${!CP} "/bin/cp"
}
function f_check_BASENAME {
	f_check_defined_and_which_and_ends_with ${!BASENAME} "/bin/basename"
}
function f_check_DIRNAME {
	f_check_defined_and_which_and_ends_with ${!DIRNAME} "/bin/dirname"
}
function f_check_ANT_9_HOME {
	typeset varName=${!ANT_9_HOME}
	f_check_defined $varName 
	f_check_dir_exists $varName
	f_check_which $varName "bin/ant"
	f_printResult $varName
}

function f_check_CREATE_STATIC_LIBS {
	typeset varName=${!CREATE_STATIC_LIBS}
	f_check_defined $varName
	f_check_boolean $varName
	f_printResult $varName
}
function f_check_MAKE_JOBS {
	typeset varName=${!MAKE_JOBS}
	f_check_defined $varName
	# cores = 'Cores per socket' x 'Sockets'	#https://unix.stackexchange.com/questions/218074/how-to-know-number-of-cores-of-a-system-in-linux
	typeset cores=$(( $(lscpu | awk '/^Core/{ print $4 }') * $(lscpu | awk '/^Socket/{ print $2 }') ))
	f_check_range $varName 1 $cores
	f_printResult $varName
}

function f_check_GPP {
	f_check_defined_and_which_and_ends_with ${!GPP} "/bin/g\\+\\+"
}
function f_check_GCC_LIB {
	typeset varName=${!GCC_LIB}
	f_check_defined $varName
	f_check_file_exists $varName "libgcc.a"
	f_check_file_exists $varName "libgcc_s.so"
	f_check_file_exists $varName "libgomp.so"
	f_check_file_exists $varName "libstdc++.so"
	f_printResult $varName
}

function f_check_JAVA_8_HOME {
	f_check_java_home_helper ${!JAVA_8_HOME}
}
function f_check_JAVA_8 {
	f_check_java_helper ${!JAVA_8}
}
function f_check_JAVA_7_HOME {
	f_check_java_home_helper ${!JAVA_7_HOME}
}
function f_check_JAVA_7 {
	f_check_java_helper ${!JAVA_7}
}
function f_check_JAVA_7_RT_JAR {
	typeset varName=${!JAVA_7_RT_JAR}
	f_check_defined $varName
	f_check_ends_with $varName "jre/lib/rt.jar"
	f_check_file_exists $varName
	f_printResult $varName
}
function f_check_java_home_helper {
	typeset varName=$1
	f_check_defined $varName
	f_check_dir_exists $varName
	f_check_dir_exists $varName "bin"
	f_check_dir_exists $varName "include"
	f_check_dir_exists $varName "jre"
	f_check_dir_exists $varName "lib"
	f_check_file_exists $varName "jre/lib/rt.jar"
	f_printResult $varName
}
function f_check_java_helper {
	f_check_defined_and_which_and_ends_with $1 "/bin/java"
}

function f_check_JAVA_INC {
	typeset varName=${!JAVA_INC}
	f_check_defined $varName
	f_check_ends_with $varName "/include"
	f_check_dir_exists $varName
	f_check_file_exists $varName "jawt.h"
	f_check_file_exists $varName "jdwpTransport.h"
	f_check_file_exists $varName "jni.h"
	f_check_file_exists $varName "jvmticmlr.h"
	f_check_file_exists $varName "jvmti.h"
	f_printResult $varName
}
function f_check_JAVA_OS_INC {
	typeset varName=${!JAVA_OS_INC}
	f_check_defined $varName
	f_check_ends_with $varName "/linux"
	f_check_dir_exists $varName
	f_check_file_exists $varName "jawt_md.h"
	f_check_file_exists $varName "jni_md.h"
	f_printResult $varName
}
function f_check_JAVA_LIB {
	typeset varName=${!JAVA_LIB}
	f_check_defined $varName
	f_check_ends_with $varName "/jre/lib/amd64/server"
	f_check_dir_exists $varName
	f_check_file_exists $varName "libjsig.so"
	f_check_file_exists $varName "libjvm.so"
	f_check_file_exists $varName "Xusage.txt"
	f_printResult $varName
}

function f_check_JACE_HOME {
	f_check_defined_and_dir_exists ${!JACE_HOME}
}
function f_check_JACE_INC {
	typeset varName=${!JACE_INC}
	f_check_defined $varName
	f_check_ends_with $varName "/include"
	f_check_dir_exists $varName
	f_check_dir_exists $varName "jace"
	f_printResult $varName
}
function f_check_JACE_JAR_LIB {
	typeset varName=${!JACE_JAR_LIB}
	f_check_defined $varName
	f_check_ends_with $varName "/lib"
	f_check_dir_exists $varName
	f_printResult $varName
}
function f_check_JACE_LIB {
	typeset varName=${!JACE_LIB}
	f_check_defined $varName
	f_check_ends_with $varName "/dynamic"
	f_check_dir_exists $varName
	f_check_file_exists $varName "libjace.a"
	f_check_file_exists $varName "libjace.so"
	f_printResult $varName
}
function f_check_JACE_CORE_JAR {
	typeset varName=${!JACE_CORE_JAR}
	f_check_defined $varName
	f_check_ends_with $varName "/jace-core.jar"
	f_check_file_exists $varName
	f_printResult $varName
}
function f_check_JACE_RUNTIME_JAR {
	typeset varName=${!JACE_RUNTIME_JAR}
	f_check_defined $varName
	f_check_ends_with $varName "/jace-runtime.jar"
	f_check_file_exists $varName
	f_printResult $varName
}

function f_check_BOOST_HOME {
	f_check_defined_and_dir_exists ${!BOOST_HOME}
}
function f_check_BOOST_INC {
	typeset varName=${!BOOST_INC}
	f_check_defined $varName
	f_check_ends_with $varName "/include"
	f_check_dir_exists $varName
	f_check_dir_exists $varName "boost"
	f_printResult $varName
}
function f_check_BOOST_LIB {
	typeset varName=${!BOOST_LIB}
	f_check_defined $varName
	f_check_ends_with $varName "/lib"
	f_check_dir_exists $varName
	f_check_file_exists $varName "libboost_date_time.so"
	f_check_file_exists $varName "libboost_system.so"
	f_check_file_exists $varName "libboost_thread.so"
	f_printResult $varName
}

function f_check_KDB_INC {
	f_check_kdb_helper ${!KDB_INC}
}
function f_check_KDB3_INC {
	f_check_kdb_helper ${!KDB3_INC}
}
function f_check_kdb_helper {
	typeset varName=$1
	f_check_defined $varName
	f_check_ends_with $varName "/include"
	f_check_dir_exists $varName
	f_check_file_exists $varName "k.h"
	f_printResult $varName
}


function f_check_SWIG_HOME {
	f_check_defined_and_dir_exists ${!SWIG_HOME}
}
function f_check_SWIG {
	f_check_defined_and_which_and_ends_with ${!SWIG} "/bin/swig"
}
function f_check_SWIG_INC {
	typeset varName=${!SWIG_INC}
	f_check_defined $varName
	f_check_ends_with $varName "/3.0.2"
	f_check_dir_exists $varName
	f_check_dir_exists $varName "java"
	f_check_dir_exists $varName "perl5"
	f_check_dir_exists $varName "std"
	f_check_file_exists $varName "carrays.i"
	f_check_file_exists $varName "cdata.i"
	f_check_file_exists $varName "cpointer.i"
	f_check_file_exists $varName "cstring.i"
	f_check_file_exists $varName "math.i"
	f_check_file_exists $varName "runtime.swg"
	f_check_file_exists $varName "swig.swg"
	f_printResult $varName
}
function f_check_PERL_5_8 {
	f_check_defined_and_which_and_ends_with ${!PERL_5_8} "/bin/perl"
}	

function f_check_FUSE_INC {
	typeset varName=${!FUSE_INC}
	f_check_defined $varName
	f_check_ends_with $varName "/fuse"
	f_check_dir_exists $varName
	f_check_file_exists $varName "fuse.h"
	f_check_file_exists $varName "fuse_common.h"
	f_check_file_exists $varName "fuse_common_compat.h"
	f_check_file_exists $varName "fuse_compat.h"
	f_check_file_exists $varName "fuse_lowlevel.h"
	f_check_file_exists $varName "fuse_lowlevel_compat.h"
	f_check_file_exists $varName "fuse_opt.h"
	f_printResult $varName

}
function f_check_FUSE_LIB {
	typeset varName=${!FUSE_LIB}
	f_check_defined $varName
	# f_check_ends_with $varName "/lib" # some are /lib64
	f_check_dir_exists $varName
	f_check_file_exists $varName "libfuse.so"
	f_printResult $varName
}

function f_check_ZLIB_HOME {
	f_check_defined_and_dir_exists ${!ZLIB_HOME}
}
function f_check_ZLIB_INC {
	typeset varName=${!ZLIB_INC}
	f_check_defined $varName
	f_check_ends_with $varName "/include"
	f_check_dir_exists $varName
	f_check_file_exists $varName "zconf.h"
	f_check_file_exists $varName "zlib.h"
	f_printResult $varName
}
function f_check_ZLIB_LIB {
	typeset varName=${!ZLIB_LIB}
	f_check_defined $varName
	# f_check_ends_with $varName "/lib" # some are /usr/lib64
	f_check_dir_exists $varName
	f_check_file_exists $varName "libz.so"
	f_printResult $varName
}
function f_check_VALGRIND_INC {
	typeset varName=${!VALGRIND_INC}
	f_check_defined $varName
	f_check_ends_with $varName "/include"
	f_check_dir_exists $varName
	f_check_dir_exists $varName "valgrind"
	# f_check_file_exists $varName "valgrind/callgrind.h"
	# f_check_file_exists $varName "valgrind/config.h"
	# f_check_file_exists $varName "valgrind/memcheck.h"
	f_check_file_exists $varName "valgrind/valgrind.h"	# i think this is all we need
	f_printResult $varName
}

function f_check_GIT {
	f_check_defined_and_which_and_ends_with ${!GIT} "/bin/git"
}
function f_check_MUTT {
	f_check_defined_and_which_and_ends_with ${!MUTT} "/bin/mutt"
}
function f_check_REPO_NAME {
	f_check_defined_and_equals ${!REPO_NAME} "SilverKing"
}
function f_check_REPO_URL {
	f_check_defined_and_equals ${!REPO_URL} "https://github.com/Morgan-Stanley/"
}

function f_check_G_TEST_HOME {
	f_check_defined_and_dir_exists ${!G_TEST_HOME}
}
function f_check_G_TEST_INC {
	typeset varName=${!G_TEST_INC}
	f_check_defined $varName
	f_check_ends_with $varName "/include"
	f_check_dir_exists $varName
	f_check_dir_exists $varName "gtest"
	f_check_file_exists $varName "gtest/gtest.h"
	f_check_file_exists $varName "gtest/gtest_prod.h"
	f_printResult $varName
}
function f_check_G_TEST_LIB {
	typeset varName=${!G_TEST_LIB}
	f_check_defined $varName
	f_check_ends_with $varName "/lib"
	f_check_dir_exists $varName
	f_check_file_exists $varName "libgtest.so"
	f_check_file_exists $varName "libgtest_main.so"
	f_printResult $varName
}

function f_check_SK_REPO_HOME {
	typeset varName=${!SK_REPO_HOME}
	f_check_defined $varName
	f_check_dir_exists $varName
	f_check_dir_exists $varName "bin"
	f_check_dir_exists $varName "build"
	f_check_dir_exists $varName "doc"
	f_check_dir_exists $varName "lib"
	f_check_dir_exists $varName "src"
	f_check_dir_exists $varName "test"
	f_printResult $varName
}
function f_check_SK_GRID_CONFIG_DIR {
	typeset varName=${!SK_GRID_CONFIG_DIR}
	f_check_defined $varName
	f_check_ends_with $varName "/build/testing"
	f_check_dir_exists $varName
	f_printResult $varName
}
function f_check_SK_GRID_CONFIG_NAME {
	typeset varName=${!SK_GRID_CONFIG_NAME}
	f_check_defined $varName
	f_check_begins_with $varName "GC_SK_"
	f_printResult $varName
}
function f_check_SK_DHT_NAME {
	typeset varName=${!SK_DHT_NAME}
	f_check_defined $varName
	f_check_begins_with $varName "SK_"
	f_printResult $varName
}
function f_check_SK_SERVERS {
	typeset varName=${!SK_SERVERS}
	f_check_defined $varName
	f_printResult $varName
}
function f_check_SK_REPLICATION {
	typeset varName=${!SK_REPLICATION}
	f_check_defined $varName
	f_check_range $varName 1 4
	f_printResult $varName
}
function f_check_SK_ZK_ENSEMBLE {
	typeset varName=${!SK_ZK_ENSEMBLE}
	f_check_defined $varName
	f_printResult $varName
}
function f_check_SK_FOLDER_NAME {
	typeset varName=${!SK_FOLDER_NAME}
	f_check_defined $varName
	f_check_begins_with $varName "silverking"
	f_printResult $varName
}
function f_check_SK_DATA_HOME {
	typeset varName=${!SK_DATA_HOME}
	f_check_defined $varName
	f_check_begins_with $varName "/var/tmp/silverking"
	f_printResult $varName
}
function f_check_SK_LOG_HOME {
	typeset varName=${!SK_LOG_HOME}
	f_check_defined $varName
	f_check_begins_with $varName "/tmp/silverking"
	f_printResult $varName
}
function f_check_SK_SKFS_CONFIG_FILE {
	typeset varName=${!SK_SKFS_CONFIG_FILE}
	f_check_defined $varName
	f_check_ends_with $varName "skfs.config"
	f_check_file_exists $varName
	f_printResult $varName
}

function f_check_IOZONE_BIN {
	f_check_defined_and_which_and_ends_with ${!IOZONE_BIN} "/bin/iozone"
}
function f_check_TRUNCATE_BIN {
	f_check_defined_and_which ${!TRUNCATE_BIN} "/bin/truncate"
}

	
function f_check_defined_and_which {
	f_check_defined $1
	f_check_which $1
	f_printResult $1
}
function f_check_defined_and_which_and_ends_with {
	f_check_defined $1
	f_check_which $1
	f_check_ends_with $1 $2
	f_printResult $1
}
function f_check_defined_and_dir_exists {
	f_check_defined $1
	f_check_dir_exists $1
	f_printResult $1
}
function f_check_defined_and_equals {
	f_check_defined $1
	f_check_equals $1 $2
	f_printResult $1
}

# should only be called from check_defined since all functions will do this check first
function f_resetFails {
	unset fails
}

function f_check_defined {
	f_resetFails
	
	typeset variableValue=$(f_getVariableValue "$1")
	if [[ -z $variableValue ]]; then
		fails+=("defined: no")
	fi
}

function f_check_which {
	typeset variableValue=$(f_getVariableValue "$1")
	typeset file=$2
	if [[ -n $file ]]; then
		variableValue+=/$file
	fi
	
	which "$variableValue" 1>/dev/null 2>/dev/null	# important to have no other statements between this line and the if statement, like a "typeset var" or "echo blah.." b/c those will then override the $? and we will lose the value from 'which'
	if [[ $? != 0 ]]; then
		fails+=("which: no '$variableValue'")
	fi
}

function f_check_boolean {
	typeset variableValue=$(f_getVariableValue "$1")	
	if [[ $variableValue != $TRUE && $variableValue != $FALSE ]]; then
		fails+=("boolean: '$variableValue' isn't ${TRUE}|${FALSE}")
	fi
}

function f_check_range {
	typeset variableValue=$(f_getVariableValue "$1")	
	typeset min=$2
	typeset max=$3
	if [[ $variableValue -lt $min || $variableValue -gt $max ]]; then
		fails+=("range: '$variableValue' isn't [$min,$max]")
	fi
}

function f_check_dir_exists {
	typeset variableValue=$(f_getVariableValue "$1")
	typeset dir=$2
	typeset pathToDir=$variableValue/$dir
	
	if [[ ! -d $pathToDir ]]; then
		fails+=("dir exists: no '$pathToDir'")
	fi
}

function f_check_file_exists {
	typeset variableValue=$(f_getVariableValue "$1")
	typeset file=$2
	if [[ -n $file ]]; then
		variableValue+=/$file
	fi
	
	if [[ ! -f $variableValue ]]; then
		fails+=("file exists: no '$variableValue'")
	fi
}

function f_check_begins_with {
	typeset variableValue=$(f_getVariableValue "$1")
	typeset begin=$2
	
	if [[ ! $variableValue =~ ^${begin} ]] ; then	# no quotes around "${begin}$" is important!, if you put quotes, then beginnings like g++/... will fail. something with the '+' screws it up b/c it has multiple meanings depending on the context (literal, regex, etc..)
		fails+=("starts with: '$variableValue' doesn't begin with '$begin'")
	fi
}

function f_check_ends_with {
	typeset variableValue=$(f_getVariableValue "$1")
	typeset end=$2
	
	if [[ ! $variableValue =~ ${end}$ ]] ; then	# no quotes around "${end}$" is important!, if you put quotes, then endings like .../g++ will fail. something with the '+' screws it up b/c it has multiple meanings depending on the context (literal, regex, etc..)
		fails+=("ends with: '$variableValue' doesn't end with '$end'")
	fi
}

function f_check_equals {
	typeset variableValue=$(f_getVariableValue "$1")
	typeset expectedValue=$2
	
	if [[ $variableValue != $expectedValue ]] ; then
		fails+=("equals: '$variableValue' != '$expectedValue'")
	fi
}

function f_getVariableValue {
	eval "echo \$$1"	# puts whatever is in $1, and makes it into a variable with that name, and then gets the value of that variable
}

function f_printResult {
	typeset numFails=${#fails[*]}
	typeset result;
	typeset failOutput;
	
	if [[ $numFails -eq 0 ]]; then
		result="PASS"
	else
		result="FAIL"
		typeset lastIndex=$((numFails-1))
		for i in {0..$lastIndex} ; do
			typeset fail=${fails[$i]}
			failOutput+="\tcheck - $fail"
			if [[ $i -ne $lastIndex ]]; then
				failOutput+="\n"
			fi
		done
	fi
	
	printf "%3s %s %s %4s\n" $((VARIABLE_ID++)) $1 "${padder:${#1}}" $result
	if [[ -n $failOutput ]]; then
		echo -e $failOutput
	fi
}

typeset padder="............................"
set -a fails
VARIABLE_ID=1

typeset count=`grep -P "\w+=" $BUILD_CONFIG_FILE | wc -l`
echo "Checking $count variables:"
f_check_AR
f_check_CAT
f_check_SED
f_check_CHMOD
f_check_CP
f_check_BASENAME
f_check_DIRNAME

f_check_ANT_9_HOME

f_check_CREATE_STATIC_LIBS
f_check_MAKE_JOBS

f_check_GPP
f_check_GCC_LIB

f_check_JAVA_8_HOME
f_check_JAVA_8
f_check_JAVA_7_HOME
f_check_JAVA_7
f_check_JAVA_7_RT_JAR

f_check_JAVA_INC
f_check_JAVA_OS_INC
f_check_JAVA_LIB

f_check_JACE_HOME
f_check_JACE_INC
f_check_JACE_JAR_LIB
f_check_JACE_LIB
f_check_JACE_CORE_JAR
f_check_JACE_RUNTIME_JAR
		
f_check_BOOST_HOME
f_check_BOOST_INC
f_check_BOOST_LIB
   
f_check_KDB_INC
f_check_KDB3_INC
	 
f_check_SWIG_HOME
f_check_SWIG
f_check_SWIG_INC
f_check_PERL_5_8
	 
f_check_FUSE_INC
f_check_FUSE_LIB

f_check_ZLIB_HOME
f_check_ZLIB_INC
f_check_ZLIB_LIB
f_check_VALGRIND_INC
	  
f_check_GIT
f_check_MUTT
f_check_REPO_NAME
f_check_REPO_URL

f_check_G_TEST_HOME
f_check_G_TEST_INC
f_check_G_TEST_LIB
	
f_check_SK_REPO_HOME
f_check_SK_GRID_CONFIG_DIR
f_check_SK_GRID_CONFIG_NAME
f_check_SK_DHT_NAME
f_check_SK_SERVERS
f_check_SK_REPLICATION
f_check_SK_ZK_ENSEMBLE
f_check_SK_FOLDER_NAME
f_check_SK_DATA_HOME
f_check_SK_LOG_HOME
f_check_SK_SKFS_CONFIG_FILE
	
f_check_IOZONE_BIN
f_check_TRUNCATE_BIN
   
