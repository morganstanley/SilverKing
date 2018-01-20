#!/bin/ksh

source lib/common.lib

f_clearOutEnvVariables
f_checkAndSetBuildTimestamp

function f_checkParams {
	f_printHeader "PARAM CHECK"
	  
	echo "                            cc=$CC"
	echo "                  fuse_inc_dir=$FUSE_INC_DIR"
	echo "                  fuse_lib_dir=$FUSE_LIB_DIR"
	echo "                       sk_root=$SK_ROOT"
	echo " install_arch_area_folder_name=$INSTALL_ARCH_AREA_FOLDER_NAME"
	  
	if [[ -z $CC ]] ; then
		echo "Need to pass in a C compiler"
		exit 1
	fi
	  
	if [[ -z $FUSE_INC_DIR ]] ; then
		echo "Need to pass in a fuse_inc_dir"
		exit 1
	fi
	  
	if [[ -z $FUSE_LIB_DIR ]] ; then
		echo "Need to pass in a fuse_lib_dir"
		exit 1
	fi
	
	if [[ -z $SK_ROOT ]] ; then
		SK_ROOT=$SILVERKING_INSTALL_DIR
		echo "Set SK_ROOT=$SK_ROOT"
	fi
	
	if [[ -z $INSTALL_ARCH_AREA_FOLDER_NAME ]] ; then
		INSTALL_ARCH_AREA_FOLDER_NAME=$INSTALL_ARCH_AREA_NAME
		echo "Set INSTALL_ARCH_AREA_FOLDER_NAME=$INSTALL_ARCH_AREA_FOLDER_NAME"
	fi
}

function f_compileAndLink {	
	echo "compile source files"
	typeset cFilenames="hashtable.c hashtable_utility.c hashtable_itr.c Util.c ArrayBlockingQueue.c QueueProcessor.c Cache.c FileBlockCache.c AttrCache.c AttrReader.c DirEntryIndex.c FileBlockID.c FileID.c FileIDToPathMap.c ActiveOp.c ActiveOpRef.c AttrReadRequest.c FileBlockReadRequest.c FileBlockReader.c PartialBlockReader.c PartialBlockReadRequest.c NSKeySplit.c AttrWriter.c AttrWriteRequest.c FileBlockWriter.c FileBlockWriteRequest.c SRFSDHT.c ResponseTimeStats.c ReaderStats.c PathGroup.c G2TaskOutputReader.c G2OutputDir.c PathListEntry.c FileAttr.c WritableFile.c WritableFileBlock.c WritableFileTable.c ArrayBlockList.c DirEntry.c DirData.c DirDataReader.c DirDataReadRequest.c OpenDir.c OpenDirCache.c OpenDirTable.c OpenDirUpdate.c OpenDirWriter.c OpenDirWriteRequest.c ReconciliationSet.c FileStatus.c WritableFileReference.c skfs.c"
	typeset fileCount=0;
	typeset resolvedAbsFilenames;
	for filename in $cFilenames ; do
		resolvedAbsFilenames+="$SKFS_SRC_DIR/$filename "
		((fileCount++))
	done
	
	f_printSubSection "Compiling and Assembling ($fileCount in $SKFS_SRC_DIR)"
	f_compileAssembleFilesUsingMake "c" "$resolvedAbsFilenames" "$fileCount" "$SKFS_BUILD_ARCH_DIR" "$CC" "$CC_OPTS -Wno-strict-aliasing" "$INC_OPTS -I${SKFS_SRC_DIR}"
	
	SKFS_EXEC=$SKFS_INSTALL_ARCH_DIR/$SKFS_EXEC_NAME
	f_link "$SKFS_EXEC" "$SKFS_BUILD_ARCH_DIR/$ALL_DOT_O_FILES" "$CC" "$CC_OPTS" "$LIB_OPTS"
	
	f_runBuildChecks
}

function f_runBuildChecks {
	f_printSection "SUMMARY of Silverking FS Build"

	f_testEquals "$SKFS_BUILD_ARCH_DIR" "$ALL_DOT_O_FILES" "52" 
	echo "Checking INSTALL /$SKFS_EXEC_NAME"
	f_testExists "$SKFS_EXEC"
}

function f_printVariables {
	echo
	echo " sk_inc_dir=$SK_INC_DIR"
	echo " sk_lib_dir=$SK_LIB_DIR"
	echo
}

							CC=$1
output_filename=$(f_getBuildSkfs_RunOutputFilename "$CC")
{
	# params
				  FUSE_INC_DIR=$2
				  FUSE_LIB_DIR=$3
					   SK_ROOT=$4
 INSTALL_ARCH_AREA_FOLDER_NAME=$5
					   
	f_checkParams

			   SK_INC_DIR=$SK_ROOT/${COMMON_INCLUDE_FOLDER_NAME}_2
			   SK_LIB_DIR=$SK_ROOT/$INSTALL_ARCH_AREA_FOLDER_NAME/$LIB_FOLDER_NAME

	if [[ $CC == $GPP ]] ; then
		CC_D_FLAGS='-DFUSE_USE_VERSION=28 -DUSE_QSORT_R'
	fi

	C_FLAGS="-g -O2 -std=gnu++11"
	LD_OPTS="-fPIC -pthread -rdynamic"
	CC_OPTS="$C_FLAGS $LD_OPTS -pipe -Wno-write-strings -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -D_REENTRANT $CC_D_FLAGS -W -Wall -Wno-unused "
	INC_OPTS="-I${FUSE_INC_DIR} -I${SK_INC_DIR} -I${SK_INC_DIR}/gen -I${ZLIB_INC} -I${VALGRIND_INC} -I${BOOST_INC} -I${JAVA_8_HOME}/.exec/x86_64.linux.2.6.glibc.2.5/include/ -I${JAVA_8_HOME}/.exec/x86_64.linux.2.6.glibc.2.5/include/linux/"
	LIB_OPTS="-L${FUSE_LIB_DIR} -lfuse -L${SK_LIB_DIR} -lclickMe -L${BOOST_LIB} -lboost_system -L${JAVA_LIB} -ljvm -lrt -lpthread -L${ZLIB_LIB} -lz      -Wl,--rpath=${FUSE_LIB_DIR} -Wl,--rpath=${SK_LIB_DIR} -Wl,--rpath=${BOOST_LIB} -Wl,--rpath=${JAVA_LIB} "

	f_startLocalTimer;	
	date;
	f_printVariables
	f_cleanOrMakeDirectory $SKFS_BUILD_ARCH_DIR
	f_cleanOrMakeDirectory $SKFS_INSTALL_ARCH_DIR
	f_compileAndLink
	f_printSummary "$output_filename"
	f_printLocalElapsed;
} 2>&1 | tee $output_filename


