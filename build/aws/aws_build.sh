#!/bin/ksh

function f_yumInstall {
	sudo yum -y install $1
}

function f_downloadTar {
	typeset tar=$1
	typeset url=$2
	
	cd $lib_root
	wget $url
	tar -xvf $tar
	rm $tar
}

function f_downloadJar {
	typeset url=$1
	
	curl -O $url
}

function f_replaceLine {
	typeset oldLine=$1
	typeset newLine=$2
	typeset filename=$3
	typeset admin=$4
	
	$admin sed -i "/${oldLine}/c${newLine}" $filename
}

cd ..
source lib/common.lib
source lib/build_sk_client.lib	# for copying kill_process_and_children.pl
cd -

source lib/common.lib

lib_root=~

echo "BUILD"
# sudo yum install ant - is installing 1.8 and we need 1.9
# download binary file @ http://archive.apache.org/dist/ant/binaries/ or www.apache.org/dist/ant/binaries
echo "installing ant"
ant_version=apache-ant-1.10.0
ant_tar=$ant_version-bin.tar.bz2
f_downloadTar "$ant_tar" "http://archive.apache.org/dist/ant/binaries/$ant_tar"

echo "installing java"
java8=java-1.8.0
java7=java-1.7.0
f_yumInstall "$java8-openjdk-devel.x86_64" # you don't want java-1.8.0-openjdk.x86_64! It really only has the jre's
f_yumInstall "$java7-openjdk-devel.x86_64" 
f_fillInBuildConfigVariable "ANT_9_HOME"  "$lib_root/$ant_version"
f_fillInBuildConfigVariable "JAVA_8_HOME" "/usr/lib/jvm/$java8"
f_fillInBuildConfigVariable "JAVA_7_HOME" "/usr/lib/jvm/$java7"


echo "installing zk"
zk_version=zookeeper-3.4.11
zk_tar=$zk_version.tar.gz
f_downloadTar "$zk_tar" "http://apache.claz.org/zookeeper/$zk_version/$zk_tar"

cd $zk_version/conf
echo "tickTime=2000
dataDir=/var/tmp/zookeeper
clientPort=2181" > zoo.cfg

f_generatePrivateKey

sk_repo_home=~/$REPO_NAME
f_fillInBuildConfigVariable "SK_REPO_HOME" '~/$REPO_NAME' # single quotes, so REPO_HOME isn't interpreted
f_fillInBuildConfigVariable "PERL_5_8"     "/usr/bin/perl"

echo "BUILDING JACE"
f_yumInstall "boost"
boost_version=boost_1_61_0
boost_tar=$boost_version.tar.gz
f_downloadTar "$boost_tar" "https://versaweb.dl.sourceforge.net/project/boost/boost/1.61.0/$boost_tar"

cd $lib_root
boost_lib=libs/boost
mkdir -p $boost_lib
cd $boost_lib
ln -s /usr/lib64/libboost_thread-mt.so.1.53.0    libboost_thread.so
ln -s /usr/lib64/libboost_date_time-mt.so.1.53.0 libboost_date_time.so
ln -s /usr/lib64/libboost_system-mt.so.1.53.0    libboost_system.so

f_overrideBuildConfigVariable "BOOST_INC" "$lib_root/$boost_version"
f_overrideBuildConfigVariable "BOOST_LIB" "$lib_root/$boost_lib"

cd $lib_root
jace_runtime="jace-core-runtime"
   jace_core="jace-core-java"
jace_runtime_jar=$jace_runtime-1.2.22.jar
jace_core_jar=$jace_core-1.2.22.jar
jar_url="search.maven.org/remotecontent?filepath=com/googlecode/jace"
f_downloadJar "$jar_url/$jace_runtime/1.2.22/$jace_runtime_jar"
f_downloadJar "$jar_url/$jace_core/1.2.22/$jace_core_jar"

f_yumInstall "gcc-c++" # for g++
gpp_path=/usr/bin/g++
cd $BUILD_DIR
./$BUILD_JACE_SCRIPT_NAME $gpp_path 

cd $lib_root
jace_lib=libs/jace
mkdir -p $jace_lib
cd $jace_lib
ln -s $sk_repo_home/src/jace/include include
mkdir lib
cd lib
ln -s ../../../$jace_runtime_jar jace-runtime.jar
ln -s ../../../$jace_core_jar    jace-core.jar
mv $INSTALL_ARCH_LIB_DIR/jace/dynamic .
f_fillInBuildConfigVariable "JACE_HOME" "$lib_root/$jace_lib"

echo "BUILD CLIENT"
f_fillInBuildConfigVariable "GPP"         "$gpp_path"
f_fillInBuildConfigVariable "GCC_LIB"     "/usr/lib/gcc/x86_64-amazon-linux/4.8.5"

echo "BUILD SKFS"
f_yumInstall "fuse" #(/bin/fusermount, /etc/fuse.conf, etc.)
f_yumInstall "fuse-devel" #(.h files, .so)
f_fillInBuildConfigVariable "FUSE_INC"  "/usr/include/fuse"
f_fillInBuildConfigVariable "FUSE_LIB"  "/lib64"

f_yumInstall "zlib"
f_yumInstall "zlib-devel"
f_overrideBuildConfigVariable "ZLIB_INC" "/usr/include"
f_overrideBuildConfigVariable "ZLIB_LIB" "/usr/lib64"

f_yumInstall "valgrind"	#(not sure this is necessary)
f_yumInstall "valgrind-devel" #(/usr/include/valgrind/valgrind.h)
f_fillInBuildConfigVariable "VALGRIND_INC" "/usr/include"

source $BUILD_CONFIG_FILE
f_fillInSkfsConfigVariable   "fuseLib" "$FUSE_LIB"
f_fillInSkfsConfigVariable   "fuseBin" "/bin"
f_fillInSkConfig

f_replaceLine "user_allow_other" "user_allow_other" "/etc/fuse.conf" "sudo"

# skc
cd $LIB_DIR
ln -s $SILVERKING_JAR

cd $BUILD_DIR/aws
./aws_zk.sh "start"
cd ..
./$BUILD_SCRIPT_NAME
cd aws
./aws_zk.sh "stop"
