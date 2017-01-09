#!/bin/ksh

fuseVersion=2.6.3

export PATH=$PATH:/usr/bin

old_dir=`pwd`
cd `dirname $0`
curDir=`pwd`

hostname

usage() 
{
  echo "usage      : $0 -c <command> -z <zkEnsemble> -g <GCName> [ -C <Compression> -s <skGlobalCodebase> -l <logLevel> -n <fsNativeOnlyFile> -L <coreLimit>] -f <forceSKFSDirCreation> -E <skfsEntryTimeoutSecs> -A <skfsAttrTimeoutSecs> -N <skfsNegativeTimeoutSecs> -T <transientCacheSizeKB>"
  echo "    command       : Command to apply {CheckSKFS, StopSKFS}"
  echo "    zkEnsemble    : ZooKeeper ensemble definition"
  echo "    GCName        : GridConfig Name"
  echo "    Compression   : skfs compression {LZ4, SNAPPY, ZIP, BZIP2, NOCOMPRESSION}"
  echo "    skGlobalCodebase: skGlobalCodebase override"
  echo "    logLevel      : logLevel {FINE,OPS} default: OPS"
  echo "    nativeFSOnlyFile: fsNativeOnlyFile file  with csv filelist [/rel/path:y,...]"
  echo "    coreLimit     : ulimit -c <coreLimit>"
  exit 1
}

# Initialize values from the environment.
# Any command line parameters will override these.
Compression="${GC_SK_COMRESSION}"
if [[ "${Compression}" == "" ]] ; then
	Compression="LZ4"
fi
if [[ "${GC_SK_LOG_LEVEL}" != "" ]] ; then 
	logLevel="${GC_SK_LOG_LEVEL}"
fi
if [[ "${GC_SK_NATIVE_ONLY_FILE}" != "" ]] ; then 
	nativeFSOnlyFile="${GC_SK_NATIVE_ONLY_FILE}"
fi

coreLimit="unlimited"
forceSKFSDirCreation="false"

while getopts "c:z:g:C:s:l:n:L:f:E:A:N:T:" opt; do
    case $opt in
    c) nodeControlCommand="$OPTARG";;
    z) zkEnsemble="$OPTARG" ;;
	g) GCName="$OPTARG" ;;
	C) Compression="$OPTARG" ;;
	s) skGlobalCodebase="$OPTARG" ;;
	l) logLevel="$OPTARG" ;;
	n) nativeFSOnlyFile="$OPTARG" ;;
	L) coreLimit="$OPTARG" ;;
	f) forceSKFSDirCreation="$OPTARG" ;;
	E) _skfsEntryTimeoutSecs="$OPTARG" ;;
	A) _skfsAttrTimeoutSecs="$OPTARG" ;;
	N) _skfsNegativeTimeoutSecs="$OPTARG" ;;
	T) _transientCacheSizeKB="$OPTARG" ;;
    *) usage
    esac
done
shift $(($OPTIND - 1))

echo "Basic arguments parsed"
echo
echo "GCName $GCName"
echo "nodeControlCommand $nodeControlCommand"
echo "zkEnsemble $zkEnsemble"
echo "forceSKFSDirCreation $forceSKFSDirCreation"

if [[ ! -n "${GCName}"  || ! -n "${nodeControlCommand}" || ! -n "${zkEnsemble}" ]] ; then
  echo "Missing required argument"
  usage
fi
if [ $nodeControlCommand == "CheckSKFS" ]; then
    echo
elif [ $nodeControlCommand == "StopSKFS" ]; then
    echo
else
    echo "Unknown command $nodeControlCommand"
    usage
fi

## logging level option
if [[ "${logLevel}" != "OPS" && "${logLevel}" != "FINE" ]] ; then
	if [[ "${logLevel}" != "" ]] ; then
		echo "Unsupported log level : ${logLevel}. Re-setting logLevel to default: OPS."
	fi
	logLevel="OPS";
fi
if [[ "${logLevel}" == "FINE" ]] ; then
	## set verbosity to true ( it sets fuse -d option)
	verbosity="true"; 
else
	verbosity="false";
fi

id=`pgrep skfsd`
if [ "$id" != "" ]; then
    echo "Found skfsd"
	if [ "${nodeControlCommand}" != "StopSKFS" ] ; then 
		exit
	else
	    echo "continuing a"
	fi
else
    echo "No skfsd found"
fi


echo "curDir = ${curDir}"
jaceLibs=${SK_JACE_HOME}/lib/jace-core.jar:${SK_JACE_HOME}/common/lib/jace-runtime.jar
if [[ ! -n "${skGlobalCodebase}" ]]; then      
        cd ../../lib
        cpBase=`pwd`
        cp=""
        for f in `ls $cpBase/*.jar`; do
                cp=$cp:$f
        done
        cd "${bin_dir}"

	export CLASSPATH=$cp:${jaceLibs}:${SK_JAVA_HOME}/jre/lib/rt.jar:${UtilClassPath};

	#export CLASSPATH=$SK_CLASSPATH:${jaceLibs}:${SK_JAVA_HOME}/jre/lib/rt.jar:${UtilClassPath};
	export SK_CLASSPATH=${CLASSPATH}
else
	export CLASSPATH=${skGlobalCodebase}:${jaceLibs}
	export SK_CLASSPATH=${CLASSPATH}
fi

echo $CLASSPATH

UTIL_CLASS="com.ms.silverking.cloud.skfs.management.MetaUtil"
tmpfile=/tmp/skfs.${USER}.$$
utilCmd="${SK_JAVA_HOME}/bin/java ${UTIL_CLASS} -c GetFromZK -d ${GCName} -z ${zkEnsemble} -t ${tmpfile}"
echo ${utilCmd}
${utilCmd}
if [[ $? != 0 ]] ; then
	echo "MetaUtil failed to get ${GCName} configuration from ${zkEnsemble} into ${tmpfile}, exiting" ;
	exit 1 ;
fi
grep -v "^null$" /tmp/skfs.${USER}.$$ > ${tmpfile}.conf
chmod 755 ${tmpfile}.conf
. ${tmpfile}.conf
echo tmpfile ${tmpfile}
rm ${tmpfile}

# Must be set in conf:
#useBigWrites="";
#fusePath
#fuseLib
#fuseBin
#fuseLibKO

echo "fuseBin: ${fuseBin}"
export PATH=${PATH}:${fuseBin}

${fuseBin}/fusermount -u $skfsMount 
sleep 1
${fuseBin}/fusermount -z -u $skfsMount 

killall -9 skfsd
pkill skfsd

if [[ "${forceSKFSDirCreation}" != "false" ]] ; then
    if [ -e $skfsBase ] ; then
	"Creating new SKFS Dir"
	echo mv ${skfsBase} ${skfsBase}.$$
	mv ${skfsBase} ${skfsBase}.$$
	echo $?
	echo mkdir ${skfsBase}
	mkdir ${skfsBase}
	echo $?
    fi
fi
# below is needed until skfs.c picks up a log dir from config
mkdir -p /var/tmp/silverking/skfs/logs


if [ ${nodeControlCommand} == "StopSKFS" ] ; then 
	exit
else
    echo "continuing b"
fi

if [ ! -e $skfsLogs ] ; then
	mkdir -m 777 -p $skfsLogs
fi

echo "Removing old $skfsMount"
rmdir $skfsMount
echo "Creating $skfsMount"

if [ ! -e $skfsMount ] ; then
	mkdir -m 777 -p $skfsMount
fi

echo zkEnsemble $zkEnsemble
echo GCName $GCName
echo Compression $Compression
echo checksum $checksum
echo "skGlobalCodebase $skGlobalCodebase"

##nativeFSOnlyFile - file with csv files/dirs list that will be accessed only from native FS
if [[  "${nativeFSOnlyFile}" != "" ]] ; then
	if [[ ! -f ${nativeFSOnlyFile} ]] ; then
		touch ${nativeFSOnlyFile}
	fi
else
	## path is set to default file name
	nativeFSOnlyFile=${fsNativeOnlyFile}
	touch ${nativeFSOnlyFile}
fi

boost=""
export LD_LIBRARY_PATH=${gccPath}:${boost}:$curDir/fuse/libs:${fuseLibKO}:${SK_JACE_HOME}/lib/dynamic:${JAVA_LIB_HOME}/jre/lib/amd64/server:${fuseLib}

echo "LD_LIBRARY_PATH=$LD_LIBRARY_PATH"

echo "Core limit: ${coreLimit}"
ulimit -c ${coreLimit}

FS_EXEC="$curDir/skfsd"

SKFS_DIR=$skfsBase
SKFS_MOUNT=$skfsMount
SKFS_LOG_DIR=$skfsLogs
mkdir -p $SKFS_LOG_DIR > /dev/null 2>&1

fbwQOption="--fbwReliableQueue=TRUE"
noFBWPaths=""

# override any class vars with cmd line params
if [[ "${_skfsEntryTimeoutSecs}" != "" ]] ; then
    skfsEntryTimeoutSecs=${_skfsEntryTimeoutSecs}
fi
if [[ "${_skfsAttrTimeoutSecs}" != "" ]] ; then
    skfsAttrTimeoutSecs=${_skfsAttrTimeoutSecs}
fi
if [[ "${_skfsNegativeTimeoutSecs}" != "" ]] ; then
    skfsNegativeTimeoutSecs=${_skfsNegativeTimeoutSecs}
fi
if [[ "${_transientCacheSizeKB}" != "" ]] ; then
    transientCacheSizeKB=${_transientCacheSizeKB}
fi

# now set the options
if [[ "${skfsEntryTimeoutSecs}" != "" ]] ; then
    entryTimeoutOption="--entryTimeoutSecs=${skfsEntryTimeoutSecs}"
fi

if [[ "${skfsAttrTimeoutSecs}" != "" ]] ; then
    attrTimeoutOption="--attrTimeoutSecs=${skfsAttrTimeoutSecs}"
fi

if [[ "${skfsNegativeTimeoutSecs}" != "" ]] ; then
    negativeTimeoutOption="--negativeTimeoutSecs=${skfsNegativeTimeoutSecs}"
fi

echo "entryTimeoutOption $entryTimeoutOption"
echo "attrTimeoutOption $attrTimeoutOption"
echo "negativeTimeoutOption $negativeTimeoutOption"

if [[ "${jvmOptions}" != "" ]] ; then
	skfsJvmOpt="--jvmOptions=${jvmOptions}";
fi

if [[ "${transientCacheSizeKB}" == "" ]] ; then
    memKB=`cat /proc/meminfo | grep MemTotal | gawk '{print $2}' `
    transientCacheSizeLimitKB=12000000
    transientCacheSizeKB=$((${memKB} / 48))
    if [ ${transientCacheSizeKB} -gt ${transientCacheSizeLimitKB} ]; then
        transientCacheSizeKB=${transientCacheSizeLimitKB}
    fi 
fi
echo "transientCacheSizeKB ${transientCacheSizeKB}"

mkdir_mnt="mkdir -p $SKFS_MOUNT"
load_module="${fuseBin}/fusectl start > ${SKFS_LOG_DIR}/load.log 2>&1"
# note -d option is currently in skfs.c
export start_fuse="nohup $FS_EXEC --mount=${SKFS_MOUNT} --verbose=${verbosity} --host=localhost --gcname=${GCName} --zkLoc=${zkEnsemble} --compression=${Compression} --nfsMapping=${nfsMapping} "--permanentSuffixes=${permanentSuffixes}" --noErrorCachePaths=${noErrorCachePaths} --noLinkCachePaths=${noLinkCachePaths} --snapshotOnlyPaths=${snapshotOnlyPaths} --taskOutputPaths=${taskOutputPaths} --compressedPaths=${compressedPaths} --noFBWPaths=${noFBWPaths} ${fbwQOption} --fsNativeOnlyFile=${nativeFSOnlyFile} --transientCacheSizeKB=${transientCacheSizeKB} --logLevel=${logLevel} ${useBigWrites} ${entryTimeoutOption} ${attrTimeoutOption} ${negativeTimeoutOption} ${skfsJvmOpt} > ${SKFS_LOG_DIR}/fuse.log.$$ 2>&1"

echo $start_fuse

#for key in "${!mountMap[@]}"; do
#    ls ${mountMap[$key]}
#done

run_cmd="$mkdir_mnt && $load_module"

echo "SK_JAVA_HOME $SK_JAVA_HOME"
echo "SK_JACE_HOME $SK_JACE_HOME"
echo "SK_CLASSPATH $SK_CLASSPATH"

eval $run_cmd
sleep 6 

tmpFile=/tmp/$$.skfs.fuse.tmp
rm $tmpFile 
echo "export PATH=${SK_JAVA_HOME}/bin:${PATH}:${fuseBin}:" >> $tmpFile
#echo "export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}" >> $tmpFile  
#echo "export MALLOC_ARENA_MAX=4" >> $tmpFile
#echo "export CLASSPATH=${CLASSPATH}" >> $tmpFile
echo "$start_fuse" >> $tmpFile
chmod +x $tmpFile

skfsdpid=`pgrep skfsd`
if [ "$skfsdpid" != "" ]; then
    echo "found skfsd pid $skfsdpid, exiting";
	exit
fi

$tmpFile &
sleep 1

id=`pgrep skfsd`
if [ "$id" != "" ]; then
    echo found skfsd
else
    echo no skfsd found: noskfsd
fi

cd $old_dir

exit 0
