#!/bin/ksh

source lib_common.sh

f_clearOutEnvVariables
f_checkAndSetBuildTimestamp

function f_checkParams {
	f_printHeader "PARAM CHECK"
	  
	echo "                       cc=$CC"
	echo "                  sk_root=$SK_ROOT"
	echo "new_install_ach_area_name=$NEW_INSTALL_ARCH_AREA_NAME"
	  
	if [[ -z $CC ]] ; then
		echo "Need to pass in a C compiler"
		exit 0
	fi
	
	if [[ -z $SK_ROOT ]] ; then
		SK_ROOT=$SILVERKING_INSTALL_DIR
		echo "Set SK_ROOT=$SK_ROOT"
	fi

	if [[ -n $NEW_INSTALL_ARCH_AREA_NAME ]] ; then
		echo "Changing INSTALL_ARCH_AREA_NAME=$INSTALL_ARCH_AREA_NAME => $NEW_INSTALL_ARCH_AREA_NAME"
		INSTALL_ARCH_AREA_NAME=$NEW_INSTALL_ARCH_AREA_NAME
	fi
}

function f_compileAndLink {	
	echo "compile source files"
	cFiles="hashtable.c hashtable_utility.c hashtable_itr.c Util.c ArrayBlockingQueue.c QueueProcessor.c Cache.c FileBlockCache.c AttrCache.c AttrReader.c DirEntryIndex.c FileBlockID.c FileID.c FileIDToPathMap.c ActiveOp.c ActiveOpRef.c AttrReadRequest.c FileBlockReadRequest.c FileBlockReader.c PartialBlockReader.c PartialBlockReadRequest.c NSKeySplit.c AttrWriter.c AttrWriteRequest.c FileBlockWriter.c FileBlockWriteRequest.c SRFSDHT.c ResponseTimeStats.c ReaderStats.c PathGroup.c G2TaskOutputReader.c G2OutputDir.c PathListEntry.c FileAttr.c WritableFile.c WritableFileBlock.c WritableFileTable.c ArrayBlockList.c DirEntry.c DirData.c DirDataReader.c DirDataReadRequest.c OpenDir.c OpenDirCache.c OpenDirTable.c OpenDirUpdate.c OpenDirWriter.c OpenDirWriteRequest.c ReconciliationSet.c FileStatus.c WritableFileReference.c skfs.c"
	for filename in $cFiles ; do
		f_compileC "$SKFS_SRC_DIR/$filename" "$SKFS_BUILD_OBJ_DIR" "$CC" "$CC_OPTS -W -Wall -Wno-unused -Wno-strict-aliasing" "$INC_OPTS -I${SKFS_SRC_DIR}"
	done
	
	SKFS_EXEC=$SKFS_INSTALL_ARCH_DIR/$SKFS_EXEC_NAME
	f_link "$SKFS_EXEC" "$SKFS_BUILD_OBJ_DIR/$ALL_DOT_O_FILES" "$CC" "$CC_OPTS -W -Wall -Wno-unused" "$LIB_OPTS"
	
	f_runChecks
	#echo "compile UnitTest"
	#SKFS_OBJ="$SKFS_BUILD_OBJ_DIR/UnitTest.o"
	#SKFS_EXEC="${SKFS_INSTALL_ARCH_DIR}/unitTest"
	#cFiles="skfs UnitTest"
	#for i in $cFiles ; do
	#  echo $CC -I$SKFS_SRC_DIR -W -Wno-unused -DUNIT_TESTS $CC_OPTS $INC_OPTS  -c $SKFS_SRC_DIR/$i.c -o $SKFS_BUILD_OBJ_DIR/$i.o
	#  $CC -I$SKFS_SRC_DIR -W -Wno-unused -DUNIT_TESTS $CC_OPTS  $INC_OPTS  -c $SKFS_SRC_DIR/$i.c -o $SKFS_BUILD_OBJ_DIR/$i.o 
	#done

	#echo
	#echo "link $SKFS_EXEC"
	#echo "$CC -DUNIT_TESTS $CC_OPTS -W -Wall -Wno-unused -o $SKFS_EXEC $SKFS_BUILD_OBJ_DIR/$ALL_DOT_O_FILES $LIB_OPTS" 
	#$CC -DUNIT_TESTS $CC_OPTS -W -Wall -Wno-unused -o $SKFS_EXEC $SKFS_BUILD_OBJ_DIR/$ALL_DOT_O_FILES $LIB_OPTS 
}

function f_runChecks {
	f_printSection "SUMMARY of Silverking FS Build"

	f_testEquals "$SKFS_BUILD_OBJ_DIR" "$ALL_DOT_O_FILES" "52" 
	echo "Checking INSTALL /$SKFS_EXEC_NAME"
	f_testExists "$SKFS_EXEC"
}

output_filename=$(f_getBuildSkfs_RunOutputFilename)
{
	# params
							CC=$1
					   SK_ROOT=$2
	NEW_INSTALL_ARCH_AREA_NAME=$3
	f_checkParams

	   SKFS_BUILD_OBJ_DIR=$SKFS_OUTPUT_BUILD_DIR
	SKFS_INSTALL_ARCH_DIR=$SKFS_INSTALL_DIR/$INSTALL_ARCH_AREA_NAME
			 SKFS_SRC_DIR=$ROOT_DIR/$SRC_FOLDER_NAME/skfs
			   SK_INC_DIR=$SK_ROOT/$COMMON_INCLUDE_FOLDER_NAME
			   SK_LIB_DIR=$SK_ROOT/$INSTALL_ARCH_AREA_NAME/$LIB_FOLDER_NAME

	if [[ $CC == $GPP_RHEL6 ]] ; then
		CC_D_FLAGS='-DFUSE_USE_VERSION=28 -DUSE_QSORT_R'
	fi

	C_FLAGS="-O2 -g"
	#C_FLAGS="-g"
	LD_OPTS=" -fPIC -pthread -rdynamic "
	CC_OPTS="$C_FLAGS $LD_OPTS -pipe -Wno-write-strings -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -D_REENTRANT $CC_D_FLAGS -DJACE_WANT_DYNAMIC_LOAD"
	INC_OPTS="-I${FUSE_INC_DIR} -I${SK_INC_DIR} -I${ZLIB_INC} -I${VALGRIND_INC} -I${BOOST_INC} "
	##LIB_OPTS="-L${FUSE_LIB_DIR} -L${SK_LIB_DIR} -lpthread -L${BOOST_LIB} -lboost_system -lrt -lfuse -lsilverking -L${ZLIB_LIB} -lz -Wl,-rpath,'$ORIGIN/../../lib'  -Wl,--rpath -Wl,${SK_LIB_DIR} -Wl,--rpath -Wl,${FUSE_LIB_DIR} -Wl,--rpath -Wl,${BOOST_LIB}"
	LIB_OPTS="-L${FUSE_LIB_DIR} -lfuse -L${SK_LIB_DIR} -lsilverking -L${JACE_LIB} -ljace -L${BOOST_LIB} -lboost_system -L${JAVA_LIB} -ljvm -lrt -lpthread -L${ZLIB_LIB} -lz -Wl,--rpath -Wl,${FUSE_LIB_DIR} -Wl,--rpath -Wl,${SK_LIB_DIR} -Wl,--rpath -Wl,${JACE_LIB} -Wl,--rpath -Wl,${BOOST_LIB} -Wl,--rpath -Wl,${JAVA_LIB}"


	f_startLocalTimer;
	date;
	echo
	echo "using            SK_VERSION: $SK_ROOT"
	echo "using    SKFS_BUILD_OBJ_DIR: $SKFS_BUILD_OBJ_DIR"
	echo "using SKFS_INSTALL_ARCH_DIR: $SKFS_INSTALL_ARCH_DIR"
	echo "using          SKFS_SRC_DIR: $SKFS_SRC_DIR"
	echo "using            SK_INC_DIR: $SK_INC_DIR"
	echo "using            SK_LIB_DIR: $SK_LIB_DIR"
	echo

	f_cleanOrMakeDirectory $SKFS_BUILD_OBJ_DIR
	f_cleanOrMakeDirectory $SKFS_INSTALL_ARCH_DIR
	f_compileAndLink
	f_printSummary "$output_filename"
	f_printLocalElapsed;
} 2>&1 | tee $output_filename


