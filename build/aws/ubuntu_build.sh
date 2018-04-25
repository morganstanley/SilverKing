#!/bin/ksh

function f_aptgetInstall {
	sudo apt-get -qq install $1
}

function f_ubuntu_install_java {
    echo "installing java"
    f_aptgetInstall "default-jdk" 
    typeset java7_tar=jdk-7u80-linux-x64.tar.gz
    f_downloadTar "$java7_tar" "http://ftp.osuosl.org/pub/funtoo/distfiles/oracle-java/$java7_tar"

    f_fillInBuildConfigVariable "JAVA_8_HOME" "/usr/lib/jvm/java-1.8.0-openjdk-amd64"
    f_fillInBuildConfigVariable "JAVA_7_HOME" "$LIB_ROOT/jdk1.7.0_80"
}

function f_ubuntu_symlink_boost {
    f_aptgetInstall "libboost-all-dev" # sudo apt-get install 'libboost-dev' isn't sufficient, doesn't have the .so's
    cd $LIB_ROOT
    typeset boost_lib=libs/boost
    mkdir -p $boost_lib
    cd $boost_lib
    ln -s /usr/lib/x86_64-linux-gnu/libboost_thread.so.1.58.0    libboost_thread.so
    ln -s /usr/lib/x86_64-linux-gnu/libboost_date_time.so.1.58.0 libboost_date_time.so
    ln -s /usr/lib/x86_64-linux-gnu/libboost_system.so.1.58.0    libboost_system.so
    
    f_overrideBuildConfigVariable "BOOST_LIB" "$LIB_ROOT/$boost_lib"
}

function f_ubuntu_fillin_build_skfs {    
    echo "BUILD SKFS"
    f_aptgetInstall "fuse" #(/bin/fusermount, /etc/fuse.conf, etc.)
    f_aptgetInstall "libfuse-dev" #(.h files, .so)
    sudo ln -s /lib/x86_64-linux-gnu/libfuse.so.2 /lib/x86_64-linux-gnu/libfuse.so
    f_fillInBuildConfigVariable "FUSE_INC"  "/usr/include/fuse"
    f_fillInBuildConfigVariable "FUSE_LIB"  "/lib64"

    f_aptgetInstall "zlib1g-dev" # zlib.h and libz.so
    f_overrideBuildConfigVariable "ZLIB_INC" "/usr/include/"
    f_overrideBuildConfigVariable "ZLIB_LIB" "/usr/lib/x86_64-linux-gnu/"

    f_aptgetInstall "valgrind"	#(/usr/include/valgrind/valgrind.h)
    f_fillInBuildConfigVariable "VALGRIND_INC" "/usr/include"
}

cd ..
source lib/common.lib
source lib/build_sk_client.lib	# for copying kill_process_and_children.pl
cd -

source lib/common.lib

typeset output_filename=/tmp/ubuntu_build.out
{
    sudo apt-get update
    f_aptgetInstall "make"
    f_overrideBuildConfigVariable "BASENAME" "/usr/bin/basename"

    echo "BUILD"
    f_aws_install_ant
    f_ubuntu_install_java
    f_aws_install_zk

    f_generatePrivateKey

    sk_repo_home=$LIB_ROOT/$REPO_NAME
    f_aws_fillin_vars

    echo "BUILDING JACE"
    f_aws_install_boost
    f_ubuntu_symlink_boost
    f_aws_install_jace

    f_aptgetInstall "g++"
    gpp_path=/usr/bin/g++
    cd $BUILD_DIR
    ./$BUILD_JACE_SCRIPT_NAME $gpp_path 

    f_aws_symlink_jace

    echo "BUILD CLIENT"
    f_fillInBuildConfigVariable "GPP"         "$gpp_path"
    f_fillInBuildConfigVariable "GCC_LIB"     "/usr/lib/gcc/x86_64-linux-gnu/6.0.0"
    f_ubuntu_fillin_build_skfs

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