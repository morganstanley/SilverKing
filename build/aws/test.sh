#!/bin/ksh

source lib/common.lib

function f_test {
    # sudo yum install ant - is installing 1.8 and we need 1.9
    # download binary file @ http://archive.apache.org/dist/ant/binaries/ or www.apache.org/dist/ant/binaries
    echo "installing ant"
    typeset ant_version="apache-ant-1.10.0"
    typeset ant_tar=$ant_version-bin.tar.bz2
    f_downloadTar "$ant_tar" "http://archive.apache.org/dist/ant/binaries/$ant_tar"
}

f_test