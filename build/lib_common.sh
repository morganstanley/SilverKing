#!/bin/ksh

source build_dependencies.sh

### common/shared
function f_clearOutEnvVariables {
	export   DISPLAY=
	export CLASSPATH=
}

### functions
function f_getExecutablePath {
	((EXECUTABLE_COUNT++))
	typeset path=`which $1`;
	echo "$EXECUTABLE_COUNT. $1 - $path";
	if [[ "$?" != "0" ]] ; then
		echo "please install $1: $path";
		exit 1;
	fi
	
	return path;
}

function f_abort { 
	echo "Aborting: $*"; 
	exit 1; 
}

function f_printSection {
	((SECTION_COUNT++))
	echo
	f_printHeaderHelper "${SECTION_COUNT}." "$1"
}

function f_printHeader {
	f_printHeaderHelper "" "$1"
}

function f_printHeaderHelper {
	echo "##### $1 $2 #####"
}

function f_printSubSection {
	echo
	echo "  ---> $1 "
}

function f_printStep {
	echo
	echo " ($1) $2"
}

function f_cleanOrMakeDirectory {
	echo " clean/make: $1"
	if [[ -d $1 ]] ; then
		echo "cleaning"
		rm -rf $1/*
	else
		echo "making"
		mkdir -p $1
	fi
}

function f_makeAndCopy {
	typeset src=$1;
	typeset dest=$2;
	echo "from: $src"
	echo "  to: $dest" 
	mkdir -p $dest
	cp -rf $src/* $dest
}

function f_removeVerboseList {
	typeset filenames=$1
	typeset directory=$2
	
	for file in $filenames ; do
		f_removeVerbose $directory/$file
	done
}

function f_makeVerbose {
	mkdir -pv $1
}

function f_removeVerbose {
	rm -rv $1
}

function f_remove {
	echo "Removing $1"
	rm -rf $1
}

function f_findAndRemove {
	typeset directory=$1
	typeset filename=$2
	
	for file in `find $directory -type f -name "$filename"`; do
		f_removeVerbose $file
	done
}

function f_copyVerbose {
	cp -rv $1 $2
}

function f_copy {
	echo "Copying $1 => $2"
	cp -r $1 $2
}

function f_rsyncCopy {
	typeset src=$1
	typeset dest=$2
	typeset excludes=$3

	excludeList=""
	for exclude in $excludes ; do
		echo "$item"
		excludeList="$excludeList --exclude $exclude"
	done
	
	f_rsyncHelper "$src" "$dest" "$excludeList"
}

function f_rsyncCut {
	typeset src=$1
	typeset dest=$2
	
	f_rsyncHelper "$src" "$dest" "--remove-source-files"
}

function f_rsyncHelper {
	typeset src=$1
	typeset dest=$2
	typeset options=$3
	
	rsync -avh --progress $options $src $dest
}

function f_move {
	typeset src=$1
	typeset dest=$2
	
	mv -v $src $dest
}

function f_symlinkVerbose {
	typeset link_name=$1
	typeset link_value=$2
	
	ln -sv $link_value $link_name
}

function f_checkAndSetBuildTimestamp {
	typeset extra_info=$1
	
	if [[ -z $BUILD_TIMESTAMP ]]; then
		f_setBuildTimestamp $(f_getBuildTime) "$extra_info"
	fi
}

function f_setBuildTimestamp {
 	typeset time=$1
	typeset extra_info=$2
	
	typeset text=$time
	if [[ -n $extra_info ]]; then
		text="${time}.${extra_info}"
	fi
	
	export BUILD_TIMESTAMP=$text
}

function f_getBuildTime {
	echo `date +"%m_%d_%Y-%H_%M_%S"`
}

function f_printSummaryEntireFlow {
	f_printEntireFlow_Helper $(f_getBuild_RunOutputFilename)
	f_printSummary $(f_getBuildSkClient_RunOutputFilename "$GPP_RHEL6");
	f_printSummary $(f_getBuildSkfs_RunOutputFilename);
}

function f_printEntireFlow_Helper {
	echo -e "\n\n+++++ OVERALL SUMMARY FOR ENTIRE FLOW +++++"
	f_printSummary "$1";
	echo -e "\n-----"
	f_printSummary $(f_getBuildSk_RunOutputFilename);
}

function f_getBuild_RunOutputFilename {
	echo $(f_getRunOutputFilename_Helper "$BUILD_NAME")
}

function f_getBuildSk_RunOutputFilename {
	echo $(f_getRunOutputFilename_Helper "$BUILD_SILVERKING_NAME")
}

function f_getBuildSkClient_RunOutputFilename {
	typeset newName=$(f_getRunOutputFilenameConcat_Helper "$BUILD_SILVERKING_CLIENT_NAME" "$1")
	echo $(f_getRunOutputFilename_Helper "$newName")
}

function f_getBuildSkfs_RunOutputFilename {
	echo $(f_getRunOutputFilename_Helper "$BUILD_SILVERKING_FS_NAME")
}

function f_getRunOutputFilename_Helper {
	typeset filename=$1
	
	echo "${TMP_AREA}${USER}.${BUILD_TIMESTAMP}.${filename}.out"
}

function f_getRunOutputFilenameConcat_Helper {
	typeset filename=$1
	typeset cc=$2
	
	typeset new_cc=`echo $cc | $SED s#/#_#g`
	echo "${filename}${new_cc}"
}

function f_printSummary {
	typeset file=$1
	
	typeset    passCount=$(f_getCount "$file" "$PASS_TEXT")
	typeset    failCount=$(f_getCount "$file" "$FAIL_TEXT")
	typeset keyWordCount=$(f_getKeyWordCount "$file")
	typeset       result=$(f_getResult "$failCount" "$keyWordCount")
	
	echo
	echo "SUMMARY for $file"
	echo "PASS: $passCount"
	echo "FAIL: $failCount"
	echo "KEYWORD: $keyWordCount" 
	echo "RESULT: $result"
	if [[ $failCount -gt 0 ]] ; then
		typeset failList=$(f_getList "$file" "$FAIL_TEXT")
		typeset subbedList=`echo "$failList" | sed -e "s/$FAIL_TEXT/-${FAIL_TEXT}/"`
		echo "$subbedList"
	fi
}

function f_getCount {
	typeset file=$1
	typeset text=$2
	
	typeset count=`grep "\s${text}" -w $file -c`	# -c, rather than "... | wc -l", because wc -l still gives you count of 1, with empty hits
	echo $count
}

function f_getList {
	typeset file=$1
	typeset text=$2
	
	typeset count=`grep "\s${text}" -w $file`
	echo "$count"
}

function f_getKeyWordCount {
	typeset file=$1
	
	typeset count=`egrep '(not found|error|undefined|no such file or directory|exit status)' -i -w $1 | egrep '[javadoc|jar]' -v | wc -l`
	echo $count
}

function f_getResult {
	typeset failCount=$1;
	typeset keyWordCount=$2;
	
	if [[ $failCount -eq 0 && $keyWordCount -eq 0 ]]; then
		echo "${PASS_TEXT}ED"
	else
		echo "${FAIL_TEXT}ED"
	fi
}

function f_printFileOutputLine {
	echo "Full run output is in: $1"
}

function f_testEquals {
	typeset directory=$1
	typeset name=$2
	typeset expectedSize=$3
	
	typeset folderName=`basename $directory`
	typeset matchingFileCount=$(f_getMatchingFilesCount "$directory" "$name")
	
	f_printChecking "$folderName";
	typeset result=$(f_getEqualsResult "$matchingFileCount" "$expectedSize")
	f_printResult "$result" "files matching $name == $expectedSize" "$matchingFileCount";
}

function f_testGreaterThanOrEquals {
	typeset directory=$1
	typeset name=$2
	typeset expectedSize=$3
	
	typeset folderName=`basename $directory`
	typeset matchingFileCount=$(f_getMatchingFilesCount "$directory" "$name")
	
	f_printChecking "$folderName";
	typeset result=$(f_getGreaterThanOrEqualsResult "$matchingFileCount" "$expectedSize")
	f_printResult "$result" "size is >= $expectedSize" "$matchingFileCount";
}

function f_getMatchingFilesCount {
	typeset directory=$1
	typeset filename=$2

	typeset count=`find $directory -type f -name "$filename" | wc -l`
	
	echo $count
}

function f_printChecking {
	echo
	echo "Checking /$1"
}

function f_testExists {
	typeset pathToFile=$1
	
	typeset result=$(f_getTestExistsResult "$pathToFile")
	typeset file=`basename $pathToFile`
	f_printResult "$result" "is a file and size > 0" "$file";
}

function f_printResult {
	typeset result=$1
	typeset comment=$2
	typeset actual=$3

	echo "   $result - ($comment) actual=$actual" 
}

function f_getEqualsResult {
	typeset actualSize=$1;
	typeset expectedSize=$2;
	
	if [[ $actualSize -eq $expectedSize ]]; then
		echo $PASS_TEXT
	else
		echo $FAIL_TEXT
	fi
}

function f_getGreaterThanOrEqualsResult {
	typeset actualSize=$1;
	typeset expectedSize=$2;
	
	if [[ $actualSize -ge $expectedSize ]]; then
		echo $PASS_TEXT
	else
		echo $FAIL_TEXT
	fi
}

function f_getTestExistsResult {
	typeset pathToFile=$1
	
	if [[ -f $pathToFile && -s $pathToFile ]]; then
		echo $PASS_TEXT
	else
		echo $FAIL_TEXT
	fi	
}

function f_startGlobalTimer {
	START_GLOBAL=$(f_getSeconds)
}

function f_getSeconds {
	echo `date +%s`
}

function f_printGlobalElapsed {
	typeset elapsedTime=$(f_getGlobalElapsed)
	f_printElapsed "$elapsedTime"
}

function f_getGlobalElapsed {
	echo $(f_getElapsed "$START_GLOBAL")
}

function f_startLocalTimer {
	START_LOCAL=$(f_getSeconds)
}

function f_printLocalElapsed {
	typeset elapsedTime=$(f_getLocalElapsed)
	f_printElapsed "$elapsedTime"
}

function f_printElapsed {
	typeset seconds=$1
	typeset time=`date -u -d @${seconds} +"%T"`
	echo 
	echo "Total Elapsed Time: $time ($seconds s)" 
}

function f_getLocalElapsed {
	echo $(f_getElapsed "$START_LOCAL")
}

function f_getElapsed {
	typeset startTime=$1
	typeset endTime=$(f_getSeconds)
	typeset elapsed=$((endTime-startTime))
	echo $elapsed
}

##### compiling and linking

function f_compileC {
	# params
	typeset filename=$1
	typeset outputDirectory=$2
	typeset cc=$3
	typeset cc_opts=$4
	typeset inc_opts=$5
	
	f_compileHelper "c" "$filename" "$outputDirectory" "$cc" "$cc_opts" "$inc_opts"
}

function f_compileCpp {
	# params
	typeset filename=$1
	typeset outputDirectory=$2
	typeset cc=$3
	typeset cc_opts=$4
	typeset inc_opts=$5

	f_compileHelper "cpp" "$filename" "$outputDirectory" "$cc" "$cc_opts" "$inc_opts"
}

function f_compileHelper {
	# params
	typeset fileExt=$1
	typeset filename=$2
	typeset outputDirectory=$3
	typeset cc=$4
	typeset cc_opts=$5
	typeset inc_opts=$6

	typeset out_filename=`$BASENAME $filename`
	out_filename=$outputDirectory/`echo -n $out_filename | $SED "s/.${fileExt}$/.o/"` # with pounds signs has to be like this for some reason... "s#.${fileExt}\\$#.o#"`
	f_compile "$filename" "$out_filename" "$cc" "$cc_opts" "$inc_opts"
}

function f_compile {
	# params
	typeset filename=$1
	typeset out_filename=$2
	typeset cc=$3
	typeset cc_opts=$4
	typeset inc_opts=$5
	
	echo	
	echo "compiling $filename => $out_filename"
	echo $cc $cc_opts $inc_opts -c -o $out_filename $filename
	$cc $cc_opts $inc_opts -c -o $out_filename $filename
}

function f_compileFull {
	# params
	typeset filename=$1
	typeset out_filename=$2
	typeset cc=$3
	typeset ld_opts=$4
	typeset inc_opts=$5
	typeset lib_opts=$6

	echo	
	echo "compiling $filename => $out_filename"
	echo $cc $ld_opts $inc_opts $lib_opts -o $out_filename $filename
	$cc $ld_opts $inc_opts $lib_opts -o $out_filename $filename
}

function f_compileDirectoryTree {
	# params
	typeset directoryToCompile=$1
	typeset outputDirectory=$2
	typeset cc=$3
	typeset cc_opts=$4
	typeset inc_opts=$5
	
	f_printSubSection "Compiling"
	for filename in `find ${directoryToCompile} -type f -name "*.cpp"` ; do
		f_compileCpp "$filename" "$outputDirectory" "$cc" "$cc_opts" "$inc_opts"
	done
}

function f_compileDirectory {
	# params
	typeset directoryToCompile=$1
	typeset outputDirectory=$2
	typeset cc=$3
	typeset cc_opts=$4
	typeset inc_opts=$5
	
	f_printSubSection "Compiling"
	for filename in $directoryToCompile/*.cpp ; do
		f_compileCpp "$filename" "$outputDirectory" "$cc" "$cc_opts" "$inc_opts"
	done
}

function f_createStaticLibraryName {
	echo $(f_createLibraryName "$1" "a");
}

function f_createSharedLibraryName {
	echo $(f_createLibraryName "$1" "so");
}

function f_createLibraryName {
	typeset lib_name=$1
	typeset ext=$2
	
	echo lib${lib_name}.${ext}
}

function f_createStaticLibrary {
	typeset lib_name=$1
	typeset out_dir=$2
	typeset obj_files=$3
	typeset lib_files=$4
	
	typeset static_lib_name=$(f_createStaticLibraryName "$lib_name");
	typeset static_lib=$out_dir/$static_lib_name
	f_printSubSection "creating static lib: $static_lib"
	echo $AR $static_lib $obj_files $lib_files
	$AR $static_lib $obj_files $lib_files
}

function f_createSharedLibrary {
	# params
	typeset lib_name=$1
	typeset out_dir=$2
	typeset obj_files=$3
	typeset ld=$4
	typeset ld_opts=$5
	typeset lib_opts=$6

	typeset shared_lib_name=$(f_createSharedLibraryName "$lib_name");
	typeset shared_lib=$out_dir/$shared_lib_name
	f_link "$shared_lib" "$obj_files" "$ld" "$ld_opts -shared" "$lib_opts"
}

function f_link {
	# params
	typeset out_filename=$1
	typeset obj_files=$2
	typeset ld=$3
	typeset ld_opts=$4
	typeset lib_opts=$5
	
	f_printSubSection "linking $obj_files => $out_filename"
	echo $ld $ld_opts $lib_opts -o $out_filename $obj_files
	$ld $ld_opts $lib_opts -o $out_filename $obj_files 
}

function f_checkoutOss {
	f_checkoutRepo $OSS_REPO_NAME
}

function f_checkoutRepo {
	$GIT clone ${OSS_REPO_URL}${1}.git
}

# surrounding double "" is important
function f_getAllFilesIn {
	echo "`find $1`"
}

function f_getAllFilesCount {
	typeset fileList=$(f_getAllFilesIn $1)
	typeset count=$(f_countHelper "$fileList")
	count=$((count-1))	# -1, filter out "." (the root, which is $1)
	echo $count
}

function f_getSymLinksIn {
	typeset list=$(f_parseTypeOutOfList 'l')
	echo "$list"
}

function f_getSymLinksCount {
	typeset list=$(f_getSymLinksIn)
	typeset count=$(f_countHelper "$list")
	echo $count
}

function f_getDirectoriesIn {
	typeset list=$(f_parseTypeOutOfList 'd')
	typeset dirs=`echo "$list" | awk '{print $9}'`
	echo "$dirs"
}

function f_getDirectoriesCount {
	typeset dirs=$(f_getDirectoriesIn)
	typeset count=$(f_countHelper "$dirs")
	echo $count
}

#./src:		= dirLine
#total 20
#drwxrwsr-x 3 holstben cc3Y65  4096 Nov  9 11:04 com
#drwxrwsr-x 6 holstben cc3Y65  4096 Nov  9 11:04 lib
#drwxrwsr-x 2 holstben cc3Y65 12288 Nov  9 11:04 skfs                                                                                                                 
function f_getFilesIn {
	typeset list=$(f_getLongRecursiveList)
	typeset             dirsList=`echo "$list" | grep '^[ld]' -v | grep '^total' -v`
	typeset	  dirsRemovedDirLine=`echo "$dirsList" | egrep '^\.(/*.+)*:$' -v`
	typeset dirsRemoveEmptyLines=`echo "$dirsRemovedDirLine" | egrep '^\s*$' -v`
	echo "$dirsRemoveEmptyLines"
}

function f_getFilesCount {
	typeset files=$(f_getFilesIn)
	typeset count=$(f_countHelper "$files")
	echo $count
}

function f_parseTypeOutOfList {
	typeset list=$(f_getLongRecursiveList)
	typeset parsed=`echo "$list" | grep "^$1"`
	echo "$parsed"
}

function f_getLongRecursiveList {
	echo "`ls -lR`"
}

# quotes around $1 is important!
function f_countHelper {
	echo "`echo \"$1\" | wc -l`"
}

function f_getIncrementedFolderIn {
	typeset latest=$(f_getLatestFolder "$1")
	typeset incremented=`echo $latest | gawk 'BEGIN{FS=OFS="."}{$NF++;print $0}'`
	echo $incremented
}

function f_getLatestFolder {
	typeset latest=`ls -1t $1 | egrep -v "prod|qa|git" | head -n 1`
	echo $latest
}


### variables
EXECUTABLE_COUNT=0
SECTION_COUNT=0

PASS_TEXT="PASS"
FAIL_TEXT="FAIL"

START_GLOBAL=""
 START_LOCAL=""

 ALL_DOT_O_FILES="*.o"

  BUILD_FOLDER_NAME="build"
    BIN_FOLDER_NAME="bin"
    LIB_FOLDER_NAME="lib"
    DOC_FOLDER_NAME="doc"
	SRC_FOLDER_NAME="src"
   TEST_FOLDER_NAME="test"
INCLUDE_FOLDER_NAME="include"
 COMMON_FOLDER_NAME="common"
COMMON_INCLUDE_FOLDER_NAME=$COMMON_FOLDER_NAME/$INCLUDE_FOLDER_NAME
 JAVADOC_FOLDER_NAME="javadocs"
    JAR_FOLDER_NAME="jar"
CLASSES_FOLDER_NAME="classes"
SILVERKING_JAR_NAME="silverking.jar"

SILVERKING_OUTPUT_BUILD_FOLDER_NAME="silverking_build"
     SILVERKING_INSTALL_FOLDER_NAME="silverking_install"
      SKFS_OUTPUT_BUILD_FOLDER_NAME="skfs_build"
           SKFS_INSTALL_FOLDER_NAME="skfs_install"
		             SKFS_EXEC_NAME="skfsd"
				   ARCH_OUTPUT_NAME="arch_output_area"
			   BUILD_ARCH_AREA_NAME=$ARCH_OUTPUT_NAME
			 INSTALL_ARCH_AREA_NAME=$ARCH_OUTPUT_NAME
						  

             PATH_TO_SCRIPT=`pwd`/`$DIRNAME $0`
                   ROOT_DIR=$PATH_TO_SCRIPT/..
		          BUILD_DIR=$ROOT_DIR/$BUILD_FOLDER_NAME
                    BIN_DIR=$ROOT_DIR/$BIN_FOLDER_NAME
                    LIB_DIR=$ROOT_DIR/$LIB_FOLDER_NAME
		            DOC_DIR=$ROOT_DIR/$DOC_FOLDER_NAME
					SRC_DIR=$ROOT_DIR/$SRC_FOLDER_NAME
			    INCLUDE_DIR=$ROOT_DIR/$INCLUDE_FOLDER_NAME	
SILVERKING_OUTPUT_BUILD_DIR=$BUILD_DIR/$SILVERKING_OUTPUT_BUILD_FOLDER_NAME
     SILVERKING_INSTALL_DIR=$SILVERKING_OUTPUT_BUILD_DIR/$SILVERKING_INSTALL_FOLDER_NAME
	            OUT_JAR_DIR=$SILVERKING_OUTPUT_BUILD_DIR/$JAR_FOLDER_NAME
            OUT_JAVADOC_DIR=$SILVERKING_OUTPUT_BUILD_DIR/$JAVADOC_FOLDER_NAME
        OUT_CLASSES_SRC_DIR=$SILVERKING_OUTPUT_BUILD_DIR/$CLASSES_FOLDER_NAME/$SRC_FOLDER_NAME
       OUT_CLASSES_TEST_DIR=$SILVERKING_OUTPUT_BUILD_DIR/$CLASSES_FOLDER_NAME/$TEST_FOLDER_NAME
             SILVERKING_JAR=$OUT_JAR_DIR/$SILVERKING_JAR_NAME
    
      BUILD_ARCH_DIR=$SILVERKING_OUTPUT_BUILD_DIR/$BUILD_ARCH_AREA_NAME
	  
	     SRC_LIB_DIR=$SRC_DIR/$LIB_FOLDER_NAME
	  
  	 INSTALL_INC_DIR=$SILVERKING_INSTALL_DIR/$COMMON_INCLUDE_FOLDER_NAME
    INSTALL_ARCH_DIR=$SILVERKING_INSTALL_DIR/$INSTALL_ARCH_AREA_NAME
INSTALL_ARCH_LIB_DIR=$INSTALL_ARCH_DIR/$LIB_FOLDER_NAME
INSTALL_ARCH_BIN_DIR=$INSTALL_ARCH_DIR/$BIN_FOLDER_NAME
	
	SKFS_OUTPUT_BUILD_DIR=$BUILD_DIR/$SKFS_OUTPUT_BUILD_FOLDER_NAME
	     SKFS_INSTALL_DIR=$SKFS_OUTPUT_BUILD_DIR/$SKFS_INSTALL_FOLDER_NAME
	               SKFS_D=$SKFS_INSTALL_DIR/$INSTALL_ARCH_AREA_NAME/$SKFS_EXEC_NAME
	
	    		  BUILD_NAME="build"
		     SILVERKING_NAME="silverking"
		     SILVERKING_ABBR="sk"
       BUILD_SILVERKING_NAME="${BUILD_NAME}_${SILVERKING_ABBR}"	
BUILD_SILVERKING_CLIENT_NAME="${BUILD_SILVERKING_NAME}_client"
    BUILD_SILVERKING_FS_NAME="${BUILD_SILVERKING_NAME}_fs"	 
		      ANT_BUILD_NAME="${BUILD_NAME}_sk"
	
              ANT_BUILD_SCRIPT_NAME="${ANT_BUILD_NAME}.xml"
                  BUILD_SCRIPT_NAME="${BUILD_NAME}.sh"
       BUILD_SILVERKING_SCRIPT_NAME="${BUILD_SILVERKING_NAME}.sh"
BUILD_SILVERKING_CLIENT_SCRIPT_NAME="${BUILD_SILVERKING_CLIENT_NAME}.sh"
    BUILD_SILVERKING_FS_SCRIPT_NAME="${BUILD_SILVERKING_FS_NAME}.sh"
	
						 TMP_AREA="/tmp/"
	
  	   				 INSTALL_NAME="install"
   INSTALL_SILVERKING_SCRIPT_NAME="${INSTALL_NAME}_${SILVERKING_ABBR}.sh"
INSTALL_SILVERKING_FS_SCRIPT_NAME="${INSTALL_NAME}_${SILVERKING_ABBR}_fs.sh"
  
	               UNINSTALL_PREFIX="un"
   UNINSTALL_SILVERKING_SCRIPT_NAME="${UNINSTALL_PREFIX}${INSTALL_SILVERKING_SCRIPT_NAME}"
UNINSTALL_SILVERKING_FS_SCRIPT_NAME="${UNINSTALL_PREFIX}${INSTALL_SILVERKING_FS_SCRIPT_NAME}"
					
				RELEASE_NAME="release"
		 RELEASE_SCRIPT_NAME="${RELEASE_NAME}.sh"
		
		
<<COMMENT
echo +++++++++
echo pwd=`pwd`
echo zero=$0 
echo dirname=`/usr/bin/dirname $0`
echo bd=$BUILD_DIR	
echo +++++++++
COMMENT



       
	