#!/bin/ksh

source lib/2_build_sk_client.lib

f_clearOutEnvVariables
f_checkAndSetBuildTimestamp

function f_checkParams {
	f_printHeader "PARAM CHECK"
	  
	echo "cc=$CC"
	  
	if [[ -z $CC ]] ; then
		echo "Need to pass in a C compiler"
		exit 1
	fi
}

## params
	     CC=$1
output_filename=$(f_getBuildSkClient_RunOutputFilename "$CC")
{ 
f_checkParams;

   CC_FLAGS="-g -O2 -std=c++11 -fPIC -Wall"
   #-g
   #-O2
   #-std
   #-fPIC - for shared lib
		 LD=$CC
    SWIG_CC=$CC
    SWIG_LD=$CC
	
	#CC_OPTS="$CC_FLAGS $LD_OPTS -enable-threads=posix -pipe -frecord-gcc-switches -Wall -Wno-unused-local-typedefs -DBOOST_NAMESPACE_OVERRIDE=$BOOST_NAMESPACE_OVERRIDE -D_GNU_SOURCE -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -DBOOST_SPIRIT_THREADSAFE -D_REENTRANT -D__STDC_LIMIT_MACROS -D__STDC_FORMAT_MACROS"
	#LIB_OPTS_1="$LIB_OPTS $LD_LIB_OPTS"
	
	f_startLocalTimer;
	date;
	
	f_cleanOrMakeDirectory $SILVERKING_BUILD_ARCH_DIR
	f_makeWithParents $SILVERKING_INSTALL_DIR
	f_makeWithParents $INSTALL_ARCH_BIN_DIR
	f_makeWithParents $INSTALL_ARCH_LIB_DIR
	[[ -d $GENERATED_SRC ]]           || f_abort "src dir $GENERATED_SRC does not exist";
	[[ -d $SILVERKING_INSTALL_DIR ]]  || f_abort "install dir $SILVERKING_INSTALL_DIR does not exist";
	
	# f_generateCppWrapper;
	# f_compileAndLinkProxiesIntoLib "$CC" "$CC_FLAGS" "" "$LD" "" "";
	f_installHeaderFiles;
	f_printLocalElapsed;
} 2>&1 | tee $output_filename
