#!/bin/ksh

source lib/build_sk_client.lib

f_clearOutEnvVariables
f_checkAndSetBuildTimestamp

function f_checkParams {
	f_printHeader "PARAM CHECK"
	  
	echo "        cc=$cc"
	echo " gcc_r_lib=$gcc_r_lib"
	echo " rpath_dir=$rpath_dir"
	echo "debug_flag=$debug_flag"
	  
	if [[ -z $cc || -z $gcc_r_lib ]] ; then
		echo "Need to pass in a C compiler and lib"
		exit 1
	fi

	if [[ -z $rpath_dir ]] ; then
		rpath_dir=$INSTALL_ARCH_LIB_DIR
		echo "Set rpath_dir=$rpath_dir"
	fi
}

## params
typeset             cc=$1
typeset output_filename=$(f_getBuildSkClient_RunOutputFilename "$cc")
{
	typeset  gcc_r_lib=$2
	typeset  rpath_dir=$3
	typeset debug_flag=$4
	f_checkParams;

	typeset      ld=$cc
    typeset swig_cc=$cc
    typeset swig_ld=$cc

	typeset use_debug_d=""
	typeset ld_opts="$LD_OPTS"
	if [[ $debug_flag == "-d" ]] ; then 
		use_debug_d="-D_DEBUG -finstrument-functions -finstrument-functions-exclude-file-list=/bits/stl,include/sys"
		ld_opts="$ld_opts -pg" 
	fi

	typeset use_jace_dl_d="";
	if [[ $JACE_DYNAMIC_LOADER != "" ]] ; then
		use_jace_dl_d="-DJACE_WANT_DYNAMIC_LOAD";
	fi

	typeset cc_flags="-g -O2" 
	typeset  cc_opts="$cc_flags $ld_opts -std=c++11 -frecord-gcc-switches -Wno-unused-local-typedefs $use_debug_d -DBOOST_NAMESPACE_OVERRIDE=$BOOST_NAMESPACE_OVERRIDE $CC_OPTS $use_jace_dl_d"
	typeset inc_opts_with_proxy="$INC_OPTS -I${PROXY_INC}"
	typeset lib_opts_1="$LIB_OPTS $LD_LIB_OPTS"
	typeset lib_opts_2=$LIB_OPTS
	if [[ $JACE_DYNAMIC_LOADER != "" ]] ; then
		lib_opts_2="$lib_opts_1 -Wl,--rpath -Wl,$rpath_dir"
	fi
	# doesn't need -lpthread actually
	typeset lib_opts_3="$LIB_OPTS $LD_LIB_OPTS -Wl,--rpath -Wl,$rpath_dir -Wl,--rpath -Wl,${JACE_LIB} -Wl,--rpath -Wl,${JAVA_LIB}"
	# doesn't need -lpthread actually
	typeset lib_opts_4="$lib_opts_3 -L${INSTALL_ARCH_LIB_DIR} -l${SK_LIB_NAME}"
	typeset lib_opts_5="$lib_opts_1 -l${BOOST_SYSTEM_LIB} -Wl,--rpath -Wl,${JACE_LIB}"
	
	f_startLocalTimer;
	date;
	
	f_cleanOrMakeDirectory $SILVERKING_BUILD_ARCH_DIR
	f_makeWithParents $SILVERKING_INSTALL_DIR
	f_makeWithParents $INSTALL_ARCH_BIN_DIR
	f_makeWithParents $INSTALL_ARCH_LIB_DIR
	[[ -d $DHT_CLIENT_SRC_DIR ]]      || f_abort "src dir $DHT_CLIENT_SRC_DIR does not exist";
	[[ -d $SILVERKING_INSTALL_DIR ]]  || f_abort "install dir $SILVERKING_INSTALL_DIR does not exist";
	
	f_generateProxies;
	f_compileAndLinkProxiesIntoLib "$cc" "$cc_opts" "$inc_opts_with_proxy" "$ld" "$ld_opts" "$lib_opts_1";
	f_buildMainLib "$cc" "$cc_opts" "$inc_opts_with_proxy" "$ld" "$ld_opts" "$lib_opts_2";
	f_installHeaderFiles;
	f_buildTestApp        "$cc" "$cc_opts" "$inc_opts_with_proxy" "$ld" "$ld_opts" "$lib_opts_3" "testdht";
	f_buildGtestFramework "$cc" "$cc_opts" "$inc_opts_with_proxy" "$ld" "$ld_opts" "$lib_opts_3" "testdht" "$GTEST_FOLDER_NAME"
	f_buildKdbQ  "$cc" "$ld_opts" "$lib_opts_4" "$rpath_dir";
	f_buildKdbQ3 "$cc" "$ld_opts" "$lib_opts_4" "$rpath_dir";
	f_buildPerlClient "$swig_cc" "$inc_opts_with_proxy" "$swig_ld" "$lib_opts_4" "$gcc_r_lib";
	f_buildWrapperApps "$cc" "$cc_opts" "$INC_OPTS" "$ld" "$ld_opts" "$lib_opts_5";
	f_copySkfsConfig	# important to copy and not to symlink b/c build/ doesn't exist in dist code, so bin/ would have a dead symlink pointing to build/
						# also in here and not build_skfs b/c we need it to be in silverking dist bin/ and not skfs bin/
	f_printSummary "$output_filename";
	f_printLocalElapsed;
} 2>&1 | tee $output_filename
