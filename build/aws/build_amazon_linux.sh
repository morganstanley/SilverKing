#!/bin/ksh

source `dirname $0`/../lib/run_scripts_from_any_path.snippet

cd ..
source lib/common.lib
source lib/build_sk_client.lib	# for copying kill_process_and_children.pl
cd -

source lib/common.lib

function f_amazon_linux_yumInstall {
	sudo yum -y install $1
}

function f_amazon_linux_install_java {
    echo "installing java"
    cd $LIB_ROOT
    typeset java8=java-1.8.0
    typeset java7=java-1.7.0
    f_amazon_linux_yumInstall "${java8}-openjdk-devel.x86_64" # you don't want java-1.8.0-openjdk.x86_64! It really only has the jre's
    f_amazon_linux_yumInstall "${java7}-openjdk-devel.x86_64" 
    typeset java8home=/usr/lib/jvm/$java8
    f_fillInBuildConfigVariable "JAVA_8_HOME" "$java8home"
    f_fillInBuildConfigVariable "JAVA_7_HOME" "/usr/lib/jvm/$java7"
    
    # make java 8 the default
    # you can see what java's are available with: alternatives --config java
    sudo alternatives --set java  /usr/lib/jvm/jre-1.8.0-openjdk.x86_64/bin/java
    sudo alternatives --set javac ${java8home}-openjdk.x86_64/bin/javac
    
    ### this manually does what 'sudo alternatives --set' would do above
    # sudo rm /etc/alternatives/java
    # sudo ln -s $java8home/bin/java /etc/alternatives/java
    
    # sudo rm /etc/alternatives/javac
    # sudo ln -s $java8home/bin/javac /etc/alternatives/javac
    
    # this is for JAVA_HOME
    # sudo rm /etc/alternatives/jre
    # sudo ln -s /etc/alternatives/jre_1.8.0 /etc/alternatives/jre
}

function f_amazon_linux_symlink_boost {
    f_amazon_linux_yumInstall "boost"
    cd $LIB_ROOT
    typeset boost_lib=libs/boost
    mkdir -p $boost_lib
    cd $boost_lib
    ln -s /usr/lib64/libboost_thread-mt.so.1.53.0    libboost_thread.so
    ln -s /usr/lib64/libboost_date_time-mt.so.1.53.0 libboost_date_time.so
    ln -s /usr/lib64/libboost_system-mt.so.1.53.0    libboost_system.so

    f_overrideBuildConfigVariable "BOOST_LIB" "$LIB_ROOT/$boost_lib"
}

function f_amazon_linux_fillin_build_skfs { 
    f_amazon_linux_yumInstall "fuse" #(/bin/fusermount, /etc/fuse.conf, etc.)
    f_amazon_linux_yumInstall "fuse-devel" #(.h files, .so)
    f_fillInBuildConfigVariable "FUSE_INC"  "/usr/include/fuse"
    f_fillInBuildConfigVariable "FUSE_LIB"  "/lib64"

    f_amazon_linux_yumInstall "zlib" # not sure why it's necessary
    f_amazon_linux_yumInstall "zlib-devel"   # zlib.h and libz.so
    f_overrideBuildConfigVariable "ZLIB_INC" "/usr/include"
    f_overrideBuildConfigVariable "ZLIB_LIB" "/usr/lib64"

    # f_amazon_linux_yumInstall "valgrind"	#(not sure this is necessary)
    f_amazon_linux_yumInstall "valgrind-devel" #(/usr/include/valgrind/valgrind.h)
    f_fillInBuildConfigVariable "VALGRIND_INC" "/usr/include"
}

function f_amazon_linux_download_maven {
    typeset name="epel-apache-maven.repo"
    typeset redirectFile=/etc/yum.repos.d/$name
    sudo wget https://repos.fedorapeople.org/repos/dchen/apache-maven/$name -O $redirectFile
    sudo sed -i s#\$releasever#6#g $redirectFile
    f_amazon_linux_yumInstall "apache-maven"
    mvn --version
}

function f_aws_compile_sample_app {
    echo "
    <project>
      <groupId>edu.berkeley</groupId>
      <artifactId>simple-project</artifactId>
      <modelVersion>4.0.0</modelVersion>
      <name>Simple Project</name>
      <packaging>jar</packaging>
      <version>1.0</version>
      <dependencies>
        <dependency> <!-- Spark dependency -->
          <groupId>org.apache.spark</groupId>
          <artifactId>spark-sql_2.11</artifactId>
          <version>2.3.1</version>
        </dependency>
      </dependencies>
      <properties>
        <maven.compiler.source>1.8</maven.compiler.source>
        <maven.compiler.target>1.8</maven.compiler.target>
      </properties>
    </project>
    " > ~/spark-2.3.1-bin-hadoop2.7/pom.xml
    
    mkdir -p ~/spark-2.3.1-bin-hadoop2.7/src/main/java
    f_aws_sampleJavaFile "SimpleApp"     "$HOME/spark-2.3.1-bin-hadoop2.7/README.md"    # I would use '~' instead of '$HOME', but the java code assumes anything that doesn't start with '/' is a relative path, so it tacks on the cwd to the filename.. which gives us the wrong path
    f_aws_sampleJavaFile "SimpleAppSkfs" "/var/tmp/silverking/skfs/skfs_mnt/skfs/README.md"
    
    mvn package
}

function f_aws_sampleJavaFile {
    typeset className=$1
    typeset  fileName=$2

    echo "
    /* $className.java */
    import org.apache.spark.sql.SparkSession;
    import org.apache.spark.sql.Dataset;

    public class $className {
      public static void main(String[] args) {
        String logFile = \"$fileName\"; // Should be some file on your system
        SparkSession spark = SparkSession.builder().appName(\"Simple Application\").getOrCreate();
        Dataset<String> logData = spark.read().textFile(logFile).cache();

        long numAs = logData.filter(s -> s.contains(\"a\")).count();
        long numBs = logData.filter(s -> s.contains(\"b\")).count();

        System.out.println(\"Lines with a: \" + numAs + \", lines with b: \" + numBs);

        spark.stop();
      }
    }
    " > ~/spark-2.3.1-bin-hadoop2.7/src/main/java/$className.java
}

f_checkAndSetBuildTimestamp

typeset output_filename=$(f_aws_getBuild_RunOutputFilename "amazon-linux")
{
    echo "AMI: Amazon Linux AMI 2018.03.0 (HVM), SSD Volume Type - ami-6b8cef13"

    echo "BUILD"
    f_aws_install_ant
    f_amazon_linux_install_java
    f_aws_install_zk

    f_aws_generatePrivateKey

    sk_repo_home=$LIB_ROOT/$REPO_NAME
    f_aws_fillin_vars

    echo "BUILDING JACE"
    f_aws_install_boost
    f_amazon_linux_symlink_boost
    f_aws_install_jace

    f_amazon_linux_yumInstall "gcc-c++" # for g++
    gpp_path=/usr/bin/g++
    cd $BUILD_DIR
    ./$BUILD_JACE_SCRIPT_NAME "$gpp_path" 

    f_aws_symlink_jace

    echo "BUILD CLIENT"
    f_fillInBuildConfigVariable "GPP"     "$gpp_path"
    f_fillInBuildConfigVariable "GCC_LIB" "/usr/lib/gcc/x86_64-amazon-linux/4.8.5"
    f_aws_install_gtest "$gpp_path"
       
    echo "BUILD SKFS"
    f_amazon_linux_fillin_build_skfs

    f_aws_install_spark
    f_amazon_linux_download_maven
    f_aws_compile_sample_app
    
    f_aws_checkBuildConfig_fillInConfigs_andRunEverything
} 2>&1 | tee $output_filename


