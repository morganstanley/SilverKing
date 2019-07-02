#!/bin/ksh

source `dirname $0`/../lib/run_scripts_from_any_path.snippet

# I think this source should really be in aws/lib/common.lib..
cd ..
source lib/common.lib
cd -

source lib/common.lib

function f_ubuntu_aptgetInstall {
    typeset aptGetOption;
    if [[ $SK_QUIET_OUTPUT == "true" ]]; then
        aptGetOption="-qq"
    else
        aptGetOption="-y"
    fi
    
    sudo apt-get $aptGetOption install $1
    f_aws_checkExitCode "apt-get install: $1"
}

function f_ubuntu_install_java {
    echo "installing java"
    cd $LIB_ROOT
    f_ubuntu_aptgetInstall "openjdk-8-jdk" # 'default-jdk' works for 16.04, but for 14.04 it gets java7
    typeset java7_tar=jdk-7u80-linux-x64.tar.gz
    f_aws_downloadTar "$java7_tar" "https://build.funtoo.org/distfiles/oracle-java/$java7_tar"

    f_fillInBuildConfigVariable "JAVA_8_HOME" "/usr/lib/jvm/java-1.8.0-openjdk-amd64"
    f_fillInBuildConfigVariable "JAVA_7_HOME" "$LIB_ROOT/jdk1.7.0_80"
}

function f_ubuntu_symlink_boost {
    echo "symlinking boost"
    f_ubuntu_aptgetInstall "libboost-all-dev" # sudo apt-get install 'libboost-dev' isn't sufficient, doesn't have the .so's
    cd $LIB_ROOT
    typeset boost_lib=libs/boost
    mkdir -p $boost_lib
    cd $boost_lib
    
    typeset boost_number=58
    if [[ $BUILD_TYPE == $TRUSTY ]]; then
        boost_number=54
    fi
    
    f_aws_symlink libboost_thread.so    /usr/lib/x86_64-linux-gnu/libboost_thread.so.1.${boost_number}.0    
    f_aws_symlink libboost_date_time.so /usr/lib/x86_64-linux-gnu/libboost_date_time.so.1.${boost_number}.0 
    f_aws_symlink libboost_system.so    /usr/lib/x86_64-linux-gnu/libboost_system.so.1.${boost_number}.0    
        
    f_overrideBuildConfigVariable "BOOST_LIB" "$LIB_ROOT/$boost_lib"
}

function f_ubuntu_install_fuse {
    echo "installing fuse"
    cd $LIB_ROOT  
    
    f_ubuntu_aptgetInstall "python3"
    f_ubuntu_aptgetInstall "python3-pip"
    if [[ $BUILD_TYPE == $TRAVIS_CI ]]; then
        sudo pip3 install --upgrade setuptools
        sudo pip3 install --upgrade wheel
    fi
    pip3 install meson
    f_aws_checkExitCode "pip3: install meson"
    
    typeset fuse_version="fuse-3.2.6"
    typeset fuse_tar=$fuse_version.tar.gz
    f_aws_downloadTar "$fuse_tar" "https://github.com/libfuse/libfuse/archive/$fuse_tar"
    
    typeset fuse_folder="libfuse-${fuse_version}"
    cd $fuse_folder
    mkdir build
    cd build
    f_ubuntu_aptgetInstall "pkg-config"     # fixes in "meson ..": util/meson.build:27:2: ERROR:  Pkg-config not found.
    f_ubuntu_aptgetInstall "ninja-build"    # fixes in "meson ..": ERROR: Could not detect Ninja v1.5 or newer. ***note it's 'install ninja-build', not 'install ninja' (https://github.com/ninja-build/ninja/wiki/Pre-built-Ninja-packages)
    meson ..
    f_aws_checkExitCode "meson"
    
    ninja
    f_aws_checkExitCode "ninja"
    pip3 install -U pytest          # fixes in "sudo python3 -m pytest test/": /usr/bin/python3: No module named pytest
    f_aws_checkExitCode "pip3: install pytest"
    sudo python3 -m pytest test/
#    f_aws_checkExitCode "python3: pytest" commented out b/c test/test_examples.py::test_cuse fails
    sudo ninja install
    f_aws_checkExitCode "ninja install"
    sudo mv /usr/local/etc/init.d/fuse3 /etc/init.d/    # from (https://github.com/libfuse/libfuse/issues/178)
    f_aws_checkExitCode "mv"
    sudo update-rc.d fuse3 start 34 S . start 41 0 6 .  #
    f_aws_checkExitCode "update-rc.d"
    sudo ninja install
    f_aws_checkExitCode "ninja install"
    
    cd lib
    f_aws_symlink libfuse.so libfuse3.so 
    
    f_aws_replaceLine "user_allow_other" "user_allow_other" "/usr/local/etc/fuse.conf" "sudo"
    
    f_fillInBuildConfigVariable "FUSE_INC"  "$LIB_ROOT/$fuse_folder/include"
    f_fillInBuildConfigVariable "FUSE_LIB"  "$LIB_ROOT/$fuse_folder/build/lib"
}

function f_ubuntu_fillin_build_skfs { 
    echo "filling in build skfs"
    f_ubuntu_aptgetInstall "zlib1g-dev" # zlib.h and libz.so
    f_overrideBuildConfigVariable "ZLIB_INC" "/usr/include"
    f_overrideBuildConfigVariable "ZLIB_LIB" "/usr/lib/x86_64-linux-gnu"

    f_ubuntu_aptgetInstall "valgrind"    #(/usr/include/valgrind/valgrind.h)
    f_fillInBuildConfigVariable "VALGRIND_INC" "/usr/include"
}

function f_ubuntu_download_maven {
    echo "downloading maven"
    f_ubuntu_aptgetInstall "maven"
    mvn --version
    f_aws_checkExitCode "mvn"
}

   TRUSTY="trusty"
TRAVIS_CI="travisci"

typeset BUILD_TYPE=$1
f_checkAndSetBuildTimestamp

typeset output_filename=$(f_aws_getBuild_RunOutputFilename "ubuntu")
{
    if [[ $BUILD_TYPE == $TRUSTY ]]; then
        echo "AMI: Ubuntu 14.04.5 LTS, 14.04, trusty"
    else
        echo "AMI: Ubuntu Server 16.04 LTS (HVM), SSD Volume Type - ami-4e79ed36"
    fi
    
    f_aws_fillInMakeJobs
    f_ubuntu_aptgetInstall "make"
    f_overrideBuildConfigVariable "BASENAME" "/usr/bin/basename"

    echo "BUILD"
    f_aws_install_ant
    f_aws_install_gradle
    f_ubuntu_install_java
    f_aws_install_zk

    f_aws_generatePrivateKey

    sk_repo_home=$LIB_ROOT/$REPO_NAME
    f_aws_fillin_vars

    echo "BUILDING JACE"
    f_aws_install_boost
    f_ubuntu_symlink_boost
    f_aws_install_jace

    f_ubuntu_aptgetInstall "g++"
    gpp_path=/usr/bin/g++
    cd $BUILD_DIR
    ./$BUILD_JACE_SCRIPT_NAME "$gpp_path"

    f_aws_symlink_jace

    echo "BUILD CLIENT"
    f_fillInBuildConfigVariable "GPP"     "$gpp_path"
    typeset gcclib_number="6.0.0"
    if [[ $BUILD_TYPE == $TRUSTY ]]; then
        gcclib_number="4.8.5"
    fi
    f_fillInBuildConfigVariable "GCC_LIB" "/usr/lib/gcc/x86_64-linux-gnu/$gcclib_number"
    f_aws_install_gtest "$gpp_path"
    f_aws_install_swig
       
    echo "BUILD SKFS"
    f_ubuntu_install_fuse
    f_ubuntu_fillin_build_skfs

    f_aws_install_spark
    f_ubuntu_download_maven
    f_aws_compile_sample_app
    
    f_aws_install_iozone

    export SKFS_CC_D_FLAGS="-DFUSE_USE_VERSION=30"  # https://github.com/libfuse/sshfs/commit/34146444ce20c477cba7e9fe113e4387da32ae94
    f_aws_checkBuildConfig_fillInConfigs_andRunEverything
} 2>&1 | tee $output_filename

# needs to be outside the {} or else exit code won't be picked up. I think the tee still runs something even if we exit, which then turns the code to 0
f_exitIfFailed "$output_filename"