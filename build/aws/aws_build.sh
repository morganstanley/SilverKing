#!/bin/ksh

function f_yumInstall {
	sudo yum -y install $1
}

function f_aws_install_java {
    echo "installing java"
    cd $LIB_ROOT
    typeset java8=java-1.8.0
    typeset java7=java-1.7.0
    f_yumInstall "$java8-openjdk-devel.x86_64" # you don't want java-1.8.0-openjdk.x86_64! It really only has the jre's
    f_yumInstall "$java7-openjdk-devel.x86_64" 
    f_fillInBuildConfigVariable "JAVA_8_HOME" "/usr/lib/jvm/$java8"
    f_fillInBuildConfigVariable "JAVA_7_HOME" "/usr/lib/jvm/$java7"
}

function f_aws_symlink_boost {
    f_yumInstall "boost"
    cd $LIB_ROOT
    typeset boost_lib=libs/boost
    mkdir -p $boost_lib
    cd $boost_lib
    ln -s /usr/lib64/libboost_thread-mt.so.1.53.0    libboost_thread.so
    ln -s /usr/lib64/libboost_date_time-mt.so.1.53.0 libboost_date_time.so
    ln -s /usr/lib64/libboost_system-mt.so.1.53.0    libboost_system.so

    f_overrideBuildConfigVariable "BOOST_LIB" "$LIB_ROOT/$boost_lib"
}

function f_aws_fillin_build_skfs {    
    echo "BUILD SKFS"
    f_yumInstall "fuse" #(/bin/fusermount, /etc/fuse.conf, etc.)
    f_yumInstall "fuse-devel" #(.h files, .so)
    f_fillInBuildConfigVariable "FUSE_INC"  "/usr/include/fuse"
    f_fillInBuildConfigVariable "FUSE_LIB"  "/lib64"

    f_yumInstall "zlib" # not sure why it's necessary
    f_yumInstall "zlib-devel"   # zlib.h and libz.so
    f_overrideBuildConfigVariable "ZLIB_INC" "/usr/include"
    f_overrideBuildConfigVariable "ZLIB_LIB" "/usr/lib64"

    # f_yumInstall "valgrind"	#(not sure this is necessary)
    f_yumInstall "valgrind-devel" #(/usr/include/valgrind/valgrind.h)
    f_fillInBuildConfigVariable "VALGRIND_INC" "/usr/include"
}

cd ..
source lib/common.lib
source lib/build_sk_client.lib	# for copying kill_process_and_children.pl
cd -

source lib/common.lib

typeset output_filename=/tmp/aws_build.out
{
    sudo yum update

    echo "BUILD"
    f_aws_install_ant
    f_aws_install_java
    f_aws_install_zk

    f_generatePrivateKey

    sk_repo_home=$LIB_ROOT/$REPO_NAME
    f_aws_fillin_vars

    echo "BUILDING JACE"
    f_aws_install_boost
    f_aws_symlink_boost
    f_aws_install_jace

    f_yumInstall "gcc-c++" # for g++
    gpp_path=/usr/bin/g++
    cd $BUILD_DIR
    ./$BUILD_JACE_SCRIPT_NAME $gpp_path 

    f_aws_symlink_jace

    echo "BUILD CLIENT"
    f_fillInBuildConfigVariable "GPP"         "$gpp_path"
    f_fillInBuildConfigVariable "GCC_LIB"     "/usr/lib/gcc/x86_64-amazon-linux/4.8.5"
    f_aws_fillin_build_skfs

    source $BUILD_CONFIG_FILE
    f_aws_edit_configs
    f_aws_skc

    cd $BUILD_DIR/aws
    ./aws_zk.sh "start"
    cd ..
    ./$BUILD_SCRIPT_NAME
    cd aws
    ./aws_zk.sh "stop"
} 2>&1 | tee $output_filename


