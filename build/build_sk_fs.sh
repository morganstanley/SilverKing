#!/bin/ksh

source lib/build_sk_client.vars	# for SK_LIB_NAME and JACE_LIB_NAME

f_clearOutEnvVariables
f_checkAndSetBuildTimestamp

function f_checkParams {
	f_printHeader "PARAM CHECK"
	  
	echo "                            cc=$cc"
	echo "                  fuse_inc_dir=$fuse_inc_dir"
	echo "                  fuse_lib_dir=$fuse_lib_dir"
	echo "                       sk_root=$sk_root"
	echo " install_arch_area_folder_name=$install_arch_area_folder_name"
	  
	if [[ -z $cc ]] ; then
		echo "Need to pass in a C compiler"
		exit 1
	fi
	  
	if [[ -z $fuse_inc_dir ]] ; then
		echo "Need to pass in a fuse_inc_dir"
		exit 1
	fi
	  
	if [[ -z $fuse_lib_dir ]] ; then
		echo "Need to pass in a fuse_lib_dir"
		exit 1
	fi
	
	if [[ -z $sk_root ]] ; then
		sk_root=$SILVERKING_INSTALL_DIR
		echo "Set sk_root=$sk_root"
	fi
	
	if [[ -z $install_arch_area_folder_name ]] ; then
		install_arch_area_folder_name=$INSTALL_ARCH_AREA_NAME
		echo "Set install_arch_area_folder_name=$install_arch_area_folder_name"
	fi
}

function f_compileAndLink {	
	echo "compile source files"
	typeset cFilenames="hashtable.c hashtable_utility.c hashtable_itr.c Util.c ArrayBlockingQueue.c QueueProcessor.c Cache.c FileBlockCache.c AttrCache.c AttrReader.c DirEntryIndex.c FileBlockID.c FileID.c FileIDToPathMap.c ActiveOp.c ActiveOpRef.c AttrReadRequest.c FileBlockReadRequest.c FileBlockReader.c PartialBlockReader.c PartialBlockReadRequest.c NSKeySplit.c AttrWriter.c AttrWriteRequest.c FileBlockWriter.c FileBlockWriteRequest.c SRFSDHT.c ResponseTimeStats.c ReaderStats.c PathGroup.c G2TaskOutputReader.c G2OutputDir.c PathListEntry.c FileAttr.c WritableFile.c WritableFileBlock.c WritableFileTable.c ArrayBlockList.c DirEntry.c DirData.c DirDataReader.c DirDataReadRequest.c OpenDir.c OpenDirCache.c OpenDirTable.c OpenDirUpdate.c OpenDirWriter.c OpenDirWriteRequest.c ReconciliationSet.c FileStatus.c WritableFileReference.c NativeFile.c NativeFileReference.c NativeFileTable.c skfs.c SKFSOpenFile.c BlockReader.c"
	typeset fileCount=0;
	typeset resolvedAbsFilenames;
	for filename in $cFilenames ; do
		resolvedAbsFilenames+="$SKFS_SRC_DIR/$filename "
		((fileCount++))
	done
	
	f_printSubSection "Compiling and Assembling ($fileCount in $SKFS_SRC_DIR)"
	f_compileAssembleFilesUsingMake "c" "$resolvedAbsFilenames" "$fileCount" "$SKFS_BUILD_ARCH_DIR" "$cc" "$cc_opts -W -Wall -Wno-unused -Wno-strict-aliasing" "$inc_opts -I${SKFS_SRC_DIR}"
	
	SKFS_EXEC=$SKFS_INSTALL_ARCH_DIR/$SKFS_EXEC_NAME
	f_link "$SKFS_EXEC" "$SKFS_BUILD_ARCH_DIR/$ALL_DOT_O_FILES" "$cc" "$cc_opts -W -Wall -Wno-unused" "$lib_opts"
	
	f_runBuildChecks
	#echo "compile UnitTest"
	#SKFS_OBJ="$SKFS_BUILD_ARCH_DIR/UnitTest.o"
	#SKFS_EXEC="${SKFS_INSTALL_ARCH_DIR}/unitTest"
	#cFiles="skfs UnitTest"
	#for i in $cFiles ; do
	#  echo $cc -I$SKFS_SRC_DIR -W -Wno-unused -DUNIT_TESTS $cc_opts $inc_opts  -c $SKFS_SRC_DIR/$i.c -o $SKFS_BUILD_ARCH_DIR/$i.o
	#  $cc -I$SKFS_SRC_DIR -W -Wno-unused -DUNIT_TESTS $cc_opts  $inc_opts  -c $SKFS_SRC_DIR/$i.c -o $SKFS_BUILD_ARCH_DIR/$i.o 
	#done

	#echo
	#echo "link $SKFS_EXEC"
	#echo "$cc -DUNIT_TESTS $cc_opts -W -Wall -Wno-unused -o $SKFS_EXEC $SKFS_BUILD_ARCH_DIR/$ALL_DOT_O_FILES $lib_opts" 
	#$cc -DUNIT_TESTS $cc_opts -W -Wall -Wno-unused -o $SKFS_EXEC $SKFS_BUILD_ARCH_DIR/$ALL_DOT_O_FILES $lib_opts 
}

function f_runBuildChecks {
	f_printSection "SUMMARY of Silverking FS Build"

	f_testEquals "$SKFS_BUILD_ARCH_DIR" "$ALL_DOT_O_FILES" "57" 
	echo "Checking INSTALL /$SKFS_EXEC_NAME"
	f_testExists "$SKFS_EXEC"
}

function f_printVariables {
	echo
	echo " sk_inc_dir=$sk_inc_dir"
	echo " sk_lib_dir=$sk_lib_dir"
	echo
}

# params
typeset 							   cc=$1
typeset output_filename=$(f_getBuildSkfs_RunOutputFilename "$cc")
{
	typeset 				 fuse_inc_dir=$2
	typeset 				 fuse_lib_dir=$3	
	typeset                    cc_d_flags=$4
	typeset 					  sk_root=$5
	typeset install_arch_area_folder_name=$6
	f_checkParams

	typeset sk_inc_dir=$sk_root/$COMMON_INCLUDE_FOLDER_NAME
	typeset sk_lib_dir=$sk_root/$install_arch_area_folder_name/$LIB_FOLDER_NAME

	typeset c_flags="-g -O2"
	typeset cc_opts="$c_flags $LD_OPTS -pipe -Wno-write-strings -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -D_REENTRANT $cc_d_flags -DJACE_WANT_DYNAMIC_LOAD"
	typeset inc_opts="-I${BOOST_INC} -I${fuse_inc_dir} -I${sk_inc_dir} -I${ZLIB_INC} -I${VALGRIND_INC}"
	typeset lib_opts="-L${BOOST_LIB} -l${BOOST_SYSTEM_LIB} -L${JAVA_LIB} -ljvm -lrt -lpthread -L${JACE_LIB} -l${JACE_LIB_NAME} -L${fuse_lib_dir} -lfuse -L${sk_lib_dir} -l${SK_LIB_NAME} -L${ZLIB_LIB} -lz $LD_LIB_OPTS -Wl,--rpath -Wl,${JACE_LIB} -Wl,--rpath -Wl,${fuse_lib_dir} -Wl,--rpath -Wl,${sk_lib_dir} -Wl,--rpath -Wl,${JAVA_LIB}"

	f_startLocalTimer;
	date;
	f_printVariables
	f_cleanOrMakeDirectory $SKFS_BUILD_ARCH_DIR
	f_cleanOrMakeDirectory $SKFS_INSTALL_ARCH_DIR
	f_compileAndLink
	f_printSummary "$output_filename"
	f_printLocalElapsed;
} 2>&1 | tee $output_filename


