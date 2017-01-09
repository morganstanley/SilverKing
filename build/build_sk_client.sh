#! /bin/ksh

source lib_build_sk_client.sh

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
		exit 0
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

	CC_OPTS="$CC_FLAGS $LD_OPTS -enable-threads=posix -pipe -frecord-gcc-switches -Wall -Wno-unused-local-typedefs $USE_DEBUG_D -DBOOST_NAMESPACE_OVERRIDE=$BOOST_NAMESPACE_OVERRIDE -D_GNU_SOURCE -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -DBOOST_SPIRIT_THREADSAFE -D_REENTRANT -DJACE_EXPORTS -D__STDC_LIMIT_MACROS -D__STDC_FORMAT_MACROS $USE_JACE_DL_D"
	
	f_startLocalTimer;
	date;
	echo
	f_cleanAndMakeBuildObjectArea $BUILD_ARCH_DIR
	mkdir -p $SILVERKING_INSTALL_DIR
	mkdir -p $INSTALL_ARCH_BIN_DIR
	mkdir -p $INSTALL_ARCH_LIB_DIR
	[[ -d $DHT_CLIENT_SRC_DIR ]]      || f_abort "src dir $DHT_CLIENT_SRC_DIR does not exist";
	[[ -d $SILVERKING_INSTALL_DIR ]]  || f_abort "install dir $SILVERKING_INSTALL_DIR does not exist";

	f_generateProxies;
	
	typeset include_options="$INC_OPTS -I${PROXY_INC}"
	typeset library_options="$LIB_OPTS $LD_LIB_OPTS"
	f_compileAndLinkProxiesIntoLib "$CC" "$CC_OPTS" "$include_options" "$LD" "$LD_OPTS" "$library_options";
	
	if [[ $JACE_DYNAMIC_LOADER != "" ]] ; then
		library_options="$library_options -Wl,--rpath -Wl,$RPATH_DIR"
	else
		library_options=$LIB_OPTS 
	fi
	
	f_buildMainLib "$CC" "$CC_OPTS" "$include_options" "$LD" "$LD_OPTS" "$library_options" "$SK_VER";
	f_installHeaderFiles;
	
	# doesn't need -lpthread actually
	library_options="$LIB_OPTS $LD_LIB_OPTS -Wl,--rpath -Wl,$RPATH_DIR -Wl,--rpath -Wl,${JACE_LIB} -Wl,--rpath -Wl,${JAVA_LIB}"
	f_buildTestApp "$CC" "$CC_OPTS" "$include_options" "$LD" "$LD_OPTS" "$library_options" "testdht";
	
	# doesn't need -lpthread actually
	library_options="$library_options -L${INSTALL_ARCH_LIB_DIR} -l${SK_LIB_NAME}"
	f_buildKdbQ  "$CC" "$LD_OPTS" "$library_options" "$RPATH_DIR";
	f_buildKdbQ3 "$CC" "$LD_OPTS" "$library_options" "$RPATH_DIR";
	f_buildPerlClient "$SWIG_CC" "$include_options" "$SWIG_LD" "$library_options" "$GCC_R_LIB";
	
	include_options=$INC_OPTS
	library_options="$LIB_OPTS $LD_LIB_OPTS -lboost_system -Wl,--rpath -Wl,${JACE_LIB} -Wl,--rpath -Wl,${JAVA_RT_LIB}"
	f_buildWrapperApps "$CC" "$CC_OPTS" "$include_options" "$LD" "$LD_OPTS" "$library_options";
	
	f_printSummary "$output_filename";
	f_printLocalElapsed;
} 2>&1 | tee $output_filename
