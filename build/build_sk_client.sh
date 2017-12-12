#!/bin/ksh

source lib/build_sk_client.lib

f_clearOutEnvVariables
f_checkAndSetBuildTimestamp

function f_checkParams {
	f_printHeader "PARAM CHECK"
	  
	echo "        cc=$CC"
	echo " gcc_r_lib=$GCC_R_LIB"
	echo "sk_version=$SK_VER"
	echo "  cc_flags=$CC_FLAGS"
	echo " rpath_dir=$RPATH_DIR"
	echo "debug_flag=$DEBUG_FLAG"
	  
	if [[ -z $CC || -z $GCC_R_LIB ]] ; then
		echo "Need to pass in a C compiler and lib"
		exit 1
	fi
	  
	if [[ -z $CC_FLAGS ]] ; then
		# By default, debug, not optimized
		CC_FLAGS="-g"
		echo "Set CC_FLAGS=$CC_FLAGS"
	fi

	if [[ -z $RPATH_DIR ]] ; then
		RPATH_DIR=$INSTALL_ARCH_LIB_DIR
		echo "Set RPATH_DIR=$RPATH_DIR"
	fi
}

## params
	     CC=$1
output_filename=$(f_getBuildSkClient_RunOutputFilename "$CC")
{
  GCC_R_LIB=$2     
     SK_VER=$3
   CC_FLAGS=$4
  RPATH_DIR=$5
 DEBUG_FLAG=$6
f_checkParams;

		 LD=$CC
    SWIG_CC=$CC
    SWIG_LD=$CC

	USE_DEBUG_D=""
	if [[ $DEBUG_FLAG == "-d" ]] ; then 
		USE_DEBUG_D="-D_DEBUG -finstrument-functions -finstrument-functions-exclude-file-list=/bits/stl,include/sys"
		LD_OPTS=" ${LD_OPTS} -pg " 
	fi

	USE_JACE_DL_D="";
	if [[ $JACE_DYNAMIC_LOADER != "" ]] ; then
		USE_JACE_DL_D="-DJACE_WANT_DYNAMIC_LOAD";
	fi

	CC_OPTS="$CC_FLAGS -std=c++11 $LD_OPTS -enable-threads=posix -pipe -frecord-gcc-switches -Wall -Wno-unused-local-typedefs $USE_DEBUG_D -DBOOST_NAMESPACE_OVERRIDE=$BOOST_NAMESPACE_OVERRIDE -D_GNU_SOURCE -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -DBOOST_SPIRIT_THREADSAFE -D_REENTRANT -DJACE_EXPORTS -D__STDC_LIMIT_MACROS -D__STDC_FORMAT_MACROS $USE_JACE_DL_D"
	INC_OPTS_WITH_PROXY="$INC_OPTS -I${PROXY_INC}"
	LIB_OPTS_1="$LIB_OPTS $LD_LIB_OPTS"
	LIB_OPTS_2=""
	if [[ $JACE_DYNAMIC_LOADER != "" ]] ; then
		LIB_OPTS_2="$LIB_OPTS_1 -Wl,--rpath -Wl,$RPATH_DIR"
	else
		LIB_OPTS_2=$LIB_OPTS 
	fi
	# doesn't need -lpthread actually
	LIB_OPTS_3="$LIB_OPTS $LD_LIB_OPTS -Wl,--rpath -Wl,$RPATH_DIR -Wl,--rpath -Wl,${JACE_LIB} -Wl,--rpath -Wl,${JAVA_LIB}"
	# doesn't need -lpthread actually
	LIB_OPTS_4="$LIB_OPTS_3 -L${INSTALL_ARCH_LIB_DIR} -l${SK_LIB_NAME}"
	LIB_OPTS_5="$LIB_OPTS $LD_LIB_OPTS -lboost_system -Wl,--rpath -Wl,${JACE_LIB} -Wl,--rpath -Wl,${JAVA_RT_LIB}"
	
	f_startLocalTimer;
	date;
	
	f_cleanOrMakeDirectory $SILVERKING_BUILD_ARCH_DIR
	f_makeWithParents $SILVERKING_INSTALL_DIR
	f_makeWithParents $INSTALL_ARCH_BIN_DIR
	f_makeWithParents $INSTALL_ARCH_LIB_DIR
	[[ -d $DHT_CLIENT_SRC_DIR ]]      || f_abort "src dir $DHT_CLIENT_SRC_DIR does not exist";
	[[ -d $SILVERKING_INSTALL_DIR ]]  || f_abort "install dir $SILVERKING_INSTALL_DIR does not exist";
	
	f_generateProxies;
	f_compileAndLinkProxiesIntoLib "$CC" "$CC_OPTS" "$INC_OPTS_WITH_PROXY" "$LD" "$LD_OPTS" "$LIB_OPTS_1";
	f_buildMainLib "$CC" "$CC_OPTS" "$INC_OPTS_WITH_PROXY" "$LD" "$LD_OPTS" "$LIB_OPTS_2" "$SK_VER";
	f_installHeaderFiles;
	f_buildTestApp        "$CC" "$CC_OPTS" "$INC_OPTS_WITH_PROXY" "$LD" "$LD_OPTS" "$LIB_OPTS_3" "testdht";
	f_buildGtestFramework "$CC" "$CC_OPTS" "$INC_OPTS_WITH_PROXY" "$LD" "$LD_OPTS" "$LIB_OPTS_3" "testdht" "gtest"
	f_buildKdbQ  "$CC" "$LD_OPTS" "$LIB_OPTS_4" "$RPATH_DIR";
	f_buildKdbQ3 "$CC" "$LD_OPTS" "$LIB_OPTS_4" "$RPATH_DIR";
	f_buildPerlClient "$SWIG_CC" "$INC_OPTS_WITH_PROXY" "$SWIG_LD" "$LIB_OPTS_4" "$GCC_R_LIB";
	f_buildWrapperApps "$CC" "$CC_OPTS" "$INC_OPTS" "$LD" "$LD_OPTS" "$LIB_OPTS_5";
	f_printSummary "$output_filename";
	f_printLocalElapsed;
} 2>&1 | tee $output_filename
