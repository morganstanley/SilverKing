#!/bin/ksh

source lib_common.sh

function f_cleanAndMakeBuildObjectArea {
	f_cleanOrMakeDirectory "$1"
}

function f_runAutoProxy {
	# params
	typeset src=$1
	typeset destHeader=$2
	typeset destSource=$3
	typeset options=$4
	
	echo "java -Xms32m -Xmx256m -classpath $CLASSPATH7 org.jace.proxy.AutoProxy $src $src $destHeader $destSource $CLASSPATH7 -mindep $options"
	$JAVA_7 -Xms32m -Xmx256m -classpath $CLASSPATH7 org.jace.proxy.AutoProxy $src $src $destHeader $destSource $CLASSPATH7 -mindep $options
}

function f_runProxyGenerator {
	# params
	typeset className=$1
	typeset type=$2
	typeset proxyFolder=$3
	typeset ext=$4
	
	$JAVA_7 -Xms32m -Xmx256m -classpath $CLASSPATH7:$OUT_CLASSES_SRC_DIR org.jace.proxy.ProxyGenerator $OUT_CLASSES_SRC_DIR/com/ms/silverking/cloud/dht/$className.class $type -private -exportsymbols >${proxyFolder}/jace/proxy/com/ms/silverking/cloud/dht/$className.$ext
}

function f_runProxyGenerator_Header {
	f_runProxyGenerator "$1" "header" "$PROXY_INC" "h";
}

function f_runProxyGenerator_Source {
	f_runProxyGenerator "$1" "source" "$PROXY_SRC" "cpp";
}

##proxy generation does not support java 8
function f_generateProxies {
	f_printSection "Generating C++ Proxies for Java classes"
	
	rm -rf $PROXY_INC/*
	rm -rf $PROXY_SRC/*
	
	f_runAutoProxy "$DHT_CLIENT_SRC" "$PROXY_INC" "$PROXY_SRC" "-exportsymbols";

	f_runProxyGenerator_Header "client/KeyedOperationException";
	f_runProxyGenerator_Source "client/KeyedOperationException";
	f_runProxyGenerator_Header "client/PutException";
	f_runProxyGenerator_Source "client/PutException";
	f_runProxyGenerator_Header "client/RetrievalException";
	f_runProxyGenerator_Source "client/RetrievalException";
	
	f_runProxyGenerator_Header "client/impl/PutExceptionImpl";
	f_runProxyGenerator_Source "client/impl/PutExceptionImpl";
	f_runProxyGenerator_Header "client/impl/RetrievalExceptionImpl";
	f_runProxyGenerator_Source "client/impl/RetrievalExceptionImpl";
	f_runProxyGenerator_Header "NamespacePerspectiveOptions";
	f_runProxyGenerator_Source "NamespacePerspectiveOptions";
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
	
	typeset buildObjDir=$BUILD_ARCH_DIR/proxyLib
	f_cleanAndMakeBuildObjectArea "$buildObjDir"
	
	f_compileDirectoryTree "$PROXY_SRC_DIR" "$buildObjDir" "$cc" "$cc_opts" "$inc_opts"
	f_createStaticLibrary "$J_SK_LIB_NAME" "$INSTALL_ARCH_LIB_DIR" "$buildObjDir/$ALL_DOT_O_FILES" ""
	f_createSharedLibrary "$J_SK_LIB_NAME" "$INSTALL_ARCH_LIB_DIR" "$buildObjDir/$ALL_DOT_O_FILES" "$ld" "$ld_opts" "$lib_opts"
	
	f_testEquals "$buildObjDir" "$ALL_DOT_O_FILES" "166"
	f_testEquals "$INSTALL_ARCH_LIB_DIR" "$J_SK_LIB_STATIC_NAME" "1"
	f_testEquals "$INSTALL_ARCH_LIB_DIR" "$J_SK_LIB_SHARED_NAME" "1"
}

function f_buildMainLib {
	# params
	typeset cc=$1
	typeset cc_opts=$2
	typeset inc_opts=$3
	typeset ld=$4
	typeset ld_opts=$5
	typeset lib_opts=$6
	typeset sk_ver=$7
	
	f_printSection "Building Main lib"
	
	typeset buildObjDir=$BUILD_ARCH_DIR/mainLib
	f_cleanAndMakeBuildObjectArea "$buildObjDir"
	
	f_compileDirectory "$DHT_CLIENT_SRC_DIR" "$buildObjDir" "$cc" "$cc_opts -DSKVER=$sk_ver" "$inc_opts"
	f_createStaticLibrary "$SK_LIB_NAME" "$INSTALL_ARCH_LIB_DIR" "$buildObjDir/$ALL_DOT_O_FILES" "$J_SK_LIB"
	f_createSharedLibrary "$SK_LIB_NAME" "$INSTALL_ARCH_LIB_DIR" "$buildObjDir/$ALL_DOT_O_FILES" "$ld" "$ld_opts" "$lib_opts -L${INSTALL_ARCH_LIB_DIR} -l${J_SK_LIB_NAME}"
	# fixme was using jace dynamic loader if/else
#	typeset lib_name=$(f_createSharedLibraryName "$SK_LIB_NAME");
#	typeset sk_lib_shared=$INSTALL_ARCH_LIB_DIR/$lib_name
#	f_printSubSection "linking $sk_lib_shared"
#	if [[ $JACE_DYNAMIC_LOADER != "" ]] ; then
#		echo $ld $ld_opts $lib_opts -L${INSTALL_ARCH_LIB_DIR} -l${J_SK_LIB_NAME} -shared $buildObjDir/$ALL_DOT_O_FILES -o $sk_lib_shared 
#		$ld $ld_opts $lib_opts -L${INSTALL_ARCH_LIB_DIR} -l${J_SK_LIB_NAME} -shared $buildObjDir/$ALL_DOT_O_FILES -o $sk_lib_shared 
#	else
#		echo $ld $ld_opts $lib_opts -L${INSTALL_ARCH_LIB_DIR} -shared $buildObjDir/$ALL_DOT_O_FILES $J_SK_LIB -o $sk_lib_shared
#		$ld $ld_opts $lib_opts -L${INSTALL_ARCH_LIB_DIR} -shared $buildObjDir/$ALL_DOT_O_FILES $J_SK_LIB -o $sk_lib_shared 
	#fi
	
	f_testEquals "$buildObjDir" "$ALL_DOT_O_FILES" "68"
	f_testEquals "$INSTALL_ARCH_LIB_DIR" "$SK_LIB_STATIC_NAME" "1"
	f_testEquals "$INSTALL_ARCH_LIB_DIR" "$SK_LIB_SHARED_NAME" "1"
}

function f_installHeaderFiles {
	f_printSection "Installing Header Files"
	mkdir -p $INSTALL_INC_DIR
	cp -f $DHT_CLIENT_SRC_DIR/SK*.h $DHT_CLIENT_SRC_DIR/sk*.h $INSTALL_INC_DIR/
	cp -rf $PROXY_INC/jace  $INSTALL_INC_DIR/
}

function f_buildTestApp {
	# params
	typeset cc=$1
	typeset cc_opts=$2
	typeset inc_opts=$3
	typeset ld=$4
	typeset ld_opts=$5
	typeset lib_opts=$6
	typeset appName=$7
	
	f_printSection "Building Test App"
	f_printSubSection "App: $appName"
	
	typeset buildObjDir=$BUILD_ARCH_DIR/$appName
	f_cleanAndMakeBuildObjectArea "$buildObjDir"
	
	typeset app=$INSTALL_ARCH_BIN_DIR/$appName
	mkdir -p $INSTALL_ARCH_BIN_DIR
	
	f_compileDirectory "$APP_SRC_DIR/$appName" "$buildObjDir" "$cc" "$cc_opts" "$inc_opts -I${INSTALL_INC_DIR}"
	f_link "$app" "$buildObjDir/$ALL_DOT_O_FILES" "$ld" "$ld_opts -Wl,--rpath -Wl,${INSTALL_ARCH_LIB_DIR}" "$lib_opts -L${INSTALL_ARCH_LIB_DIR} -l${SK_LIB_NAME} -l${J_SK_LIB_NAME} -ldl"
	# fixme show glenn
#	f_printSubSection "linking $app"
#	echo "linking $app"
#	if [[ $JACE_DYNAMIC_LOADER != "" ]] ; then
#		echo $ld $ld_opts $lib_opts $buildObjDir/$ALL_DOT_O_FILES -L$INSTALL_ARCH_LIB_DIR -l$SK_LIB_NAME -l$J_SK_LIB_NAME -ldl -Wl,--rpath -Wl,$INSTALL_ARCH_LIB_DIR -o $app
#		$ld $ld_opts $lib_opts $buildObjDir/$ALL_DOT_O_FILES -L$INSTALL_ARCH_LIB_DIR -l$SK_LIB_NAME -l$J_SK_LIB_NAME -ldl -Wl,--rpath -Wl,$INSTALL_ARCH_LIB_DIR -o $app
#	else
#		echo $ld $ld_opts $lib_opts $buildObjDir/$ALL_DOT_O_FILES $SK_LIB $J_SK_LIB -o $app 
#		$ld $ld_opts $lib_opts $buildObjDir/$ALL_DOT_O_FILES $SK_LIB $J_SK_LIB -o $app 
#	fi
	
	f_testEquals "$buildObjDir" "$ALL_DOT_O_FILES" "1"
	f_testEquals "$INSTALL_ARCH_BIN_DIR" "$appName" "1"
}

function f_buildKdbHelper {
	# params
	typeset cc=$1
	typeset ld_opts=$2
	typeset lib_opts=$3
	typeset rpath_dir=$4
	typeset kdb_folder_name=$5
	typeset kdb_dir_name=$6
	typeset d_kx_ver=$7
	typeset prefix=$8
	
	typeset q_lib_install_dir=$INSTALL_ARCH_LIB_DIR/$kdb_folder_name
	typeset q_path_dir=$RPATH_DIR/$kdb_folder_name
	typeset q_app_dir=$APP_SRC_DIR/dhtq

	f_printSection "Building $kdb_folder_name+ Q lib"
	if [[ -d $q_app_dir ]]; then
		f_cleanOrMakeDirectory $q_lib_install_dir

		typeset lib_name=q${SK_LIB_NAME}.so
		typeset q_lib_shared=$q_lib_install_dir/$lib_name
		typeset qdht_name=qdht
		typeset qdht_q_name=qdht.q
		typeset testdht_q_name=testdht.q
		f_printSubSection "Building q lib: $q_lib_shared"
		f_compileFull "$q_app_dir/$qdht_name.c" "$q_lib_shared" "$cc" "$ld_opts -Wno-write-strings -shared -D__STDC_LIMIT_MACROS $d_kx_ver" "-I${BOOST_INC} -I${q_app_dir} -I${INSTALL_INC_DIR} -I${kdb_dir_name}/include" "$lib_opts" 
		## -Wno-write-strings, as krr("error-such-and-such") expects char* , not const, and new versions of gcc give warnings
		#echo $cc -Wno-write-strings -shared $ld_opts -D__STDC_LIMIT_MACROS $d_kx_ver -I$q_app_dir -I$INSTALL_INC_DIR -I$kdb_dir_name/include -I${BOOST_INC} $q_app_dir/$qdht_name.c -o $q_lib_shared $lib_opts -L $INSTALL_ARCH_LIB_DIR 
		#$cc -Wno-write-strings -shared $ld_opts -D__STDC_LIMIT_MACROS $d_kx_ver -I${BOOST_INC} -I$q_app_dir -I$INSTALL_INC_DIR -I$kdb_dir_name/include $q_app_dir/$qdht_name.c -o $q_lib_shared $lib_opts -L $INSTALL_ARCH_LIB_DIR 

		typeset full_testdht_q_name=${prefix}${testdht_q_name}
		$CAT $q_app_dir/$qdht_q_name    | $SED "s#$DHT_LIB_PATH_PLACEHOLDER#$q_path_dir#" > $q_lib_install_dir/$qdht_q_name
		$CAT $q_app_dir/$testdht_q_name | $SED "s#$DHT_LIB_PATH_PLACEHOLDER#$q_path_dir#" > $INSTALL_ARCH_BIN_DIR/$full_testdht_q_name
		
		f_testEquals "$q_lib_install_dir" "$lib_name"    "1"
		f_testEquals "$q_lib_install_dir" "$qdht_q_name" "1"
		f_testEquals "$INSTALL_ARCH_BIN_DIR" "$full_testdht_q_name" "1"
	fi
}

function f_buildKdbQ {
	typeset cc=$1
	typeset ld_opts=$2
	typeset lib_opts=$3
	typeset rpath_dir=$4
	
	f_buildKdbHelper "$cc" "$ld_opts" "$lib_opts" "$rpath_dir" "kdb" "$KDB_DIR" "" ""
}

function f_buildKdbQ3 {
	typeset cc=$1
	typeset ld_opts=$2
	typeset lib_opts=$3
	typeset rpath_dir=$4
	
	f_buildKdbHelper "$cc" "$ld_opts" "$lib_opts" "$rpath_dir" "kdb3" "$KDB3_DIR" "-DKXVER=3" "q3"
}

function f_buildPerlClient {
	typeset cc=$1
	typeset inc_opts=$2
	typeset ld=$3
	typeset lib_opts=$4
	typeset gcc_r_lib=$5
	
	f_printSection "Building Perl Client" 
	
	typeset perl_app_dir=$APP_SRC_DIR/perl
	
	if [[ ! -d $perl_app_dir ]] ; then
		echo "$perl_app_dir is not a directory, can't build perl clients..."
		return
	fi
	
	typeset perl_lib_install_dir=$INSTALL_ARCH_LIB_DIR/perl5
	
	typeset versions="5.10 5.14 5.8"
	for ver in $versions ; do  ## the last "5.8" is re-used at the end of the loop
		typeset buildObjDir=$BUILD_ARCH_DIR/perl$ver
		f_cleanAndMakeBuildObjectArea "$buildObjDir"
		
		typeset actual_perl_version_lib_install=${perl_lib_install_dir//perl5/perl$ver}
		f_cleanOrMakeDirectory "$actual_perl_version_lib_install"
		f_printSubSection "VERSION: $ver"
		
		typeset wrapName=${SK_LIB_NAME}_wrap
		typeset wrap=$buildObjDir/${wrapName}.c
		typeset  obj=$buildObjDir/${wrapName}.o

		f_printSubSection "swigging $SK_LIB_NAME lib"
		##$SWIG -c++ -perl5 -Wall -cpperraswarn -I$INSTALL_INC_DIR -I${SWIG_INC_DIR}/perl5 -o $wrap  $perl_app_dir/dht.i ##-debug-tmsearch
		echo "$SWIG -c++ -perl5 -cpperraswarn -I$INSTALL_INC_DIR -I${SWIG_INC_DIR} -o $wrap $perl_app_dir/dht.i" 
		$SWIG -c++ -perl5 -cpperraswarn -I$INSTALL_INC_DIR -I${SWIG_INC_DIR} -o $wrap $perl_app_dir/dht.i       ##-debug-tmsearch -debug-tmused -Wall

		typeset perl_opts=`${PERL_5_8//5.8/$ver} -MExtUtils::Embed -e ccopts | $SED s/-Wdeclaration-after-statement//`
		f_compile "$wrap" "$obj" "$cc" "-Wno-literal-suffix $perl_opts -fPIC" "$inc_opts -I${INSTALL_INC_DIR}"

		typeset lib_name=SKClientImpl.so
		typeset perl_lib=$actual_perl_version_lib_install/$lib_name
		f_link "$perl_lib" "$obj" "$ld" "-D__STDC_LIMIT_MACROS -shared -Wl,--rpath -Wl,${gcc_r_lib}" "$lib_opts"

		f_printSubSection "copying Perl module(s) to $actual_perl_version_lib_install"
		##$CP $perl_app_dir/*.pm $buildObjDir/*.pm $actual_perl_version_lib_install
		$CP $buildObjDir/*.pm $actual_perl_version_lib_install

		f_testEquals "$buildObjDir" "$wrapName.*" "2"
		f_testEquals "$actual_perl_version_lib_install" "$lib_name" "1"
		f_testEquals "$actual_perl_version_lib_install" "*.pm" "1"
	done
	
	f_symlinkVerbose "$perl_lib_install_dir" "perl$ver"
	
	mkdir -p $INSTALL_ARCH_BIN_DIR
	echo
	f_printSubSection "copying Perl script(s) to $INSTALL_ARCH_BIN_DIR"
	f_copyPerlScript "$perl_app_dir/testdht.pl" "$PERL_5_5"
	f_copyPerlScript "$perl_app_dir/kill_process_and_children.pl" "$PERL_5_8"
}

function f_copyPerlScript {
	f_copyPerlScript_Helper "$1" "$2" "$INSTALL_ARCH_BIN_DIR"
}

function f_copyPerlScript_Helper {
	typeset filename=$1
	typeset replacement_value=$2
	typeset out_directory=$3
	
	typeset pathToOutFile=$out_directory/`$BASENAME $filename`
	
	$CAT $filename | $DOS_2_UNIX | $SED "s#$PERL_PATH_PLACEHOLDER#$replacement_value#" > $pathToOutFile
	$CHMOD 755 $pathToOutFile
}

function f_buildWrapperApps {
	typeset cc=$1
	typeset cc_opts=$2
	typeset inc_opts=$3
	typeset ld=$4
	typeset ld_opts=$5
	typeset lib_opts=$6
	
	f_printSection "Building Wrapper Apps"
	
	# somehow skc won't build right, if we don't use a couple .o's from cltool build, so this can't be inside the loop
	typeset buildObjDir=$BUILD_ARCH_DIR/wrappers
	f_cleanAndMakeBuildObjectArea "$buildObjDir"
		
	typeset appNames="cltool skc"
	for appName in $appNames ; do 
		f_printSubSection "App: $appName"
		
		mkdir -p $INSTALL_ARCH_BIN_DIR
		
		typeset wrapper_app_src_dir=$APP_SRC_DIR/apps/$appName
		typeset app_proxy_inc=$SILVERKING_OUTPUT_BUILD_FOLDER_NAME/$appName/proxies/include
		typeset app_proxy_src=$SILVERKING_OUTPUT_BUILD_FOLDER_NAME/$appName/proxies/src
		typeset app_proxy_src_dir=$BUILD_DIR/$app_proxy_src

		f_cleanOrMakeDirectory $BUILD_DIR/$app_proxy_inc
		f_cleanOrMakeDirectory $BUILD_DIR/$app_proxy_src
		f_runAutoProxy "$wrapper_app_src_dir" "$app_proxy_inc" "$app_proxy_src" "";
		
		inc_opts="$inc_opts -I${app_proxy_inc} -I${DHT_CLIENT_SRC_DIR}"
		f_compileDirectory     "$wrapper_app_src_dir" "$buildObjDir" "$cc" "$cc_opts" "$inc_opts"
		f_compileDirectoryTree "$app_proxy_src_dir"   "$buildObjDir" "$cc" "$cc_opts" "$inc_opts"

		typeset c_appName=${appName}c
		f_link "$INSTALL_ARCH_BIN_DIR/$c_appName" "$buildObjDir/$ALL_DOT_O_FILES" "$ld" "$ld_opts" "$lib_opts"
		
		f_testGreaterThanOrEquals "$buildObjDir" "$ALL_DOT_O_FILES" "27"
		f_testEquals "$INSTALL_ARCH_BIN_DIR" "$c_appName" "1"
	done
}



              BUILD=64
JACE_DYNAMIC_LOADER=1 ## or comment this for static loader

        APP_SRC_DIR=$SRC_LIB_DIR
     DHT_CLIENT_SRC=../$SRC_FOLDER_NAME/$LIB_FOLDER_NAME/dhtclient 
 DHT_CLIENT_SRC_DIR=$SRC_LIB_DIR/dhtclient
	      PROXY_INC=$SILVERKING_OUTPUT_BUILD_FOLDER_NAME/proxies/include
	      PROXY_SRC=$SILVERKING_OUTPUT_BUILD_FOLDER_NAME/proxies/src
      PROXY_SRC_DIR=$BUILD_DIR/$PROXY_SRC

       J_SK_LIB_NAME=jsilverking
J_SK_LIB_STATIC_NAME=$(f_createStaticLibraryName "$J_SK_LIB_NAME");
J_SK_LIB_SHARED_NAME=$(f_createSharedLibraryName "$J_SK_LIB_NAME");
            J_SK_LIB=$INSTALL_ARCH_LIB_DIR/$J_SK_LIB_STATIC_NAME
         SK_LIB_NAME=silverking
  SK_LIB_STATIC_NAME=$(f_createStaticLibraryName "$SK_LIB_NAME");
  SK_LIB_SHARED_NAME=$(f_createSharedLibraryName "$SK_LIB_NAME");
              SK_LIB=$INSTALL_ARCH_LIB_DIR/$SK_LIB_STATIC_NAME

CLASSPATH7=$JACE_JAR_LIB/jace-core-java-1.2.23.jar:\
$JACE_JAR_LIB/jace-core-runtime-1.2.23.jar:\
$JAVA_7_HOME/jre/lib/rt.jar:\
$LIB_DIR/*:\
$SILVERKING_JAR
			  
        BOOST_LIB_THREAD="boost_thread"
BOOST_NAMESPACE_OVERRIDE="boost_1_56_0"

	LD_OPTS=" -rdynamic -fPIC -pthread "  ## rdynamic is for backtracing  ##-std=c++11
	INC_OPTS="-I${BOOST_INC} -I${JACE_INC} -I${JAVA_INC} -I${JAVA_OS_INC}"
	LD_LIB_OPTS="-Wl,--rpath -Wl,${BOOST_LIB}"
	#LIB_OPTS=" -L${BOOST_LIB} -l${BOOST_LIB_THREAD} -Wl,--rpath -Wl,${BOOST_LIB} -L${JACE_LIB} -L${JAVA_LIB} -ljace -lrt -lpthread -ljvm -Wl,--rpath -Wl,${JACE_LIB} -Wl,--rpath -Wl,${JAVA_RT_LIB} "
	LIB_OPTS="-L${BOOST_LIB} -l${BOOST_LIB_THREAD} -L${JACE_LIB} -ljace -L${JAVA_LIB} -ljvm -lrt -lpthread"
