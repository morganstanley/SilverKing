#!/bin/ksh

source lib/build_sk_client.vars

f_clearOutEnvVariables
f_checkAndSetBuildTimestamp

function f_checkParams {
	f_printHeader "PARAM CHECK"
	  
	echo "       cc=$cc"
	  
	if [[ -z $cc ]] ; then
		echo "Need to pass in a C compiler"
		exit 1
	fi
}

function f_compileAndLinkProxiesIntoLib {
	# params
	typeset cc=$1
	typeset cc_opts=$2
	typeset inc_opts=$3
	typeset ld=$4
	typeset ld_opts=$5
	typeset lib_opts=$6
		
	f_printSection "Compiling and linking proxies into lib"

	typeset buildObjDir=$SILVERKING_BUILD_ARCH_DIR/jaceLib
	typeset  objDirStatic=$buildObjDir/static
	typeset objDirDynamic=$buildObjDir/dynamic
	f_cleanOrMakeDirectory "$buildObjDir"
	f_cleanOrMakeDirectory "$objDirStatic"
	f_cleanOrMakeDirectory "$objDirDynamic"
		
	typeset libDir=$INSTALL_ARCH_LIB_DIR/jace/
	typeset  libDirStaticLoad=$libDir/static
	typeset libDirDynamicLoad=$libDir/dynamic
	f_cleanOrMakeDirectory "$libDir"
	f_cleanOrMakeDirectory "$libDirStaticLoad"
	f_cleanOrMakeDirectory "$libDirDynamicLoad"
	
	typeset proxy_src="../src/jace/source"
	### STATIC LOAD
	f_compileAssembleDirectoryTree "$proxy_src" "$objDirStatic" "$cc" "$cc_opts" "$inc_opts"
	# if [[ $CREATE_STATIC_LIBS == $TRUE ]]; then
		f_createStaticLibrary "$JACE_LIB_NAME" "$libDirStaticLoad" "$objDirStatic/$ALL_DOT_O_FILES" ""
	# fi
	f_createSharedLibrary "$JACE_LIB_NAME" "$libDirStaticLoad" "$objDirStatic/$ALL_DOT_O_FILES" "$ld" "$ld_opts" "$lib_opts"

	typeset expectedObjCount=36
	f_testEquals "$objDirStatic"  "$ALL_DOT_O_FILES" "$expectedObjCount"
	# if [[ $CREATE_STATIC_LIBS == $TRUE ]]; then
		f_testEquals "$libDirStaticLoad" "$JACE_LIB_STATIC_NAME" "1"
	# fi
	f_testEquals "$libDirStaticLoad" "$JACE_LIB_SHARED_NAME" "1"
	
	### DYNAMIC LOAD
	f_compileAssembleDirectoryTree "$proxy_src" "$objDirDynamic" "$cc" "$cc_opts -DJACE_WANT_DYNAMIC_LOAD" "$inc_opts"
	# if [[ $CREATE_STATIC_LIBS == $TRUE ]]; then
		f_createStaticLibrary "$JACE_LIB_NAME" "$libDirDynamicLoad" "$objDirDynamic/$ALL_DOT_O_FILES" ""
	# fi
	f_createSharedLibrary "$JACE_LIB_NAME" "$libDirDynamicLoad" "$objDirDynamic/$ALL_DOT_O_FILES" "$ld" "$ld_opts" "$lib_opts"

	f_testEquals "$objDirDynamic" "$ALL_DOT_O_FILES" "$expectedObjCount"
	# if [[ $CREATE_STATIC_LIBS == $TRUE ]]; then
		f_testEquals "$libDirDynamicLoad" "$JACE_LIB_STATIC_NAME" "1"
	# fi
	f_testEquals "$libDirDynamicLoad" "$JACE_LIB_SHARED_NAME" "1"
}

## params
typeset           cc=$1
typeset output_filename=$(f_getBuildJace_RunOutputFilename "$cc")
{
	f_checkParams;

	typeset cc_flags="-g -O2" 
	typeset ld=$cc
	typeset cc_opts="$cc_flags $LD_OPTS $CC_OPTS"
	typeset proxy_inc="../src/jace/include"
	f_startLocalTimer;
	date;
	
	f_compileAndLinkProxiesIntoLib "$cc" "$cc_opts" "$INC_OPTS_NO_JACE -I${proxy_inc}" "$ld" "$LD_OPTS" "$LIB_OPTS_NO_JACE";
	f_printSummary "$output_filename"
	f_printLocalElapsed;
} 2>&1 | tee $output_filename