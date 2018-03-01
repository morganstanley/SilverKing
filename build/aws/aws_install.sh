#!/bin/ksh

cd ..
source lib/common.vars
cd -

function f_fillInBuildConfigVariable {
    cd ~/SilverKing/build 	
    f_fillInVariable_Helper "$BUILD_CONFIG_FILE" "$1" "$2"
    cd ~
}

function f_fillInVariable_Helper {
    typeset filename=$1
    typeset variable=$2
    typeset    value=$3

    sed -ri "s#(${variable}=)#\1$value#" $filename
}

cd ~

### BUILD
echo "installing ant"
# sudo yum install ant - is installing 1.8 and we need 1.9
# download binary file @ http://archive.apache.org/dist/ant/binaries/ or www.apache.org/dist/ant/binaries
ant_version=apache-ant-1.10.0
ant_tar=$ant_version-bin.tar.bz2
wget http://archive.apache.org/dist/ant/binaries/$ant_tar
tar -xvf $ant_tar

echo "installing java"
java_version=java-1.8.0-openjdk
sudo yum -y install $java_version-devel.x86_64 # you don't want java-1.8.0-openjdk.x86_64! It really only has the jre's

f_fillInBuildConfigVariable "ANT_9_HOME"  "~/$ant_version"
f_fillInBuildConfigVariable "JAVA_8_HOME" "/usr/lib/jvm/$java_version"

echo "installing zk"
zk_tar=zookeeper-3.4.11.tar.gz
wget http://apache.claz.org/zookeeper/zookeeper-3.4.11/$zk_tar
tar -xvf $zk_tar

cd zookeeper-3.4.11/conf
echo "tickTime=2000
dataDir=/var/tmp/zookeeper
clientPort=2181" > zoo.cfg

cd ~/.ssh
ssh-keygen -f id_rsa -N '' # flags are to bypass prompt
cat id_rsa.pub >> authorized_keys

f_fillInBuildConfigVariable "SK_REPO_HOME"  "~/$REPO_HOME"

cd SilverKing/
cp src/lib/perl/kill_process_and_children.pl bin/
sed -i "s#XXX_PERL_PATH_XXX#/usr/bin/perl#" bin/kill_process_and_children.pl

