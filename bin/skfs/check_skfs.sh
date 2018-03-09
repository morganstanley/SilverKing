#!/bin/ksh

function f_printSkfsCheckWithResult {
	typeset id=$(f_getSkfsPid)
	if [[ -n "$id" ]]; then
		f_printSkfsFound
		f_printPass
		
		# leave this script running until skfs exits; currently being used by treadmill
		if [[ -n $waitForSkfsdBeforeExiting ]]; then
			typeset count=0
			typeset secondsToSleep=10
			typeset twoMinuteIntervals=$((120 / $secondsToSleep))
			while [[ -e /proc/$id ]]; do
				((count++))
				if [[ $count -eq $twoMinuteIntervals ]]; then
					f_printSkfsdStatus "$id" "alive"
					count=0
				fi
				sleep $secondsToSleep
			done
			
			f_printSkfsdStatus "$id" "dead"
		fi
		
		exit
	else
		f_printSkfsNotFound
		f_printFail
		exit -1
	fi
}

function f_printSkfsCheckWithResultOnlyIfFound {
	typeset id=$(f_getSkfsPid)
	if [[ -n "$id" ]]; then
		f_printSkfsFound
		f_printPass
		exit
	fi
}

function f_printSkfsCheck {
	typeset extraComment=$1
	
	typeset id=$(f_getSkfsPid)
	if [[ -n "$id" ]]; then
		f_printSkfsFound "$extraComment"
	else
		f_printSkfsNotFound
	fi
}

function f_printSkfsStopWithResult {
	typeset id=$(f_getSkfsPid)
	if [[ -n "$id" ]]; then
		f_printSkfsFound
		f_printFail
		exit -1
	else
		f_printSkfsNotFound
		f_printPass
		exit
	fi
}

function f_getSkfsPid {
	echo `pgrep -f $SKFSD_PATTERN`
}

function f_printSkfsdStatus {
	echo `date +"%Y-%m-%d %H:%M:%S"`": skfsd $1 $2"
}

function f_printSkfsFound {
	typeset id=$(f_getSkfsPid)
	echo "FOUND - skfsd '$GCName' ${id}${1}";
}

function f_printSkfsNotFound {
	echo "NOT FOUND - skfsd '$GCName'"
}

function f_printPass {
	f_printHelper "PASS"
}

function f_printFail {
	f_printHelper "FAIL"
}

function f_printHelper {
	echo 
	echo "RESULT: $1"
}

usage() 
{
  echo "usage      : $0 -c <command> -z <zkEnsemble> -g <GCName> [ -C <Compression> -s <skGlobalCodebase> -l <logLevel> -n <fsNativeOnlyFile> -L <coreLimit>] -f <forceSKFSDirCreation> -E <skfsEntryTimeoutSecs> -A <skfsAttrTimeoutSecs> -N <skfsNegativeTimeoutSecs> -T <transientCacheSizeKB>"
  echo "    command         : Command to apply {$CHECK_SKFS_COMMAND, $STOP_SKFS_COMMAND}"
  echo "    zkEnsemble      : ZooKeeper ensemble definition"
  echo "    GCName          : GridConfig Name"
  echo "    Compression     : skfs compression {LZ4, SNAPPY, ZIP, BZIP2, NOCOMPRESSION}"
  echo "    skGlobalCodebase: skGlobalCodebase override"
  echo "    logLevel        : logLevel {FINE,OPS} default: OPS"
  echo "    nativeFSOnlyFile: fsNativeOnlyFile file with csv filelist [/rel/path:y,...]"
  echo "    coreLimit       : ulimit -c <coreLimit>"
  echo "    -w              : waits for skfsd to exit"
  exit 1
}

export PATH=$PATH:/usr/bin

old_dir=`pwd`
cd `dirname $0`
curDir=`pwd`

hostname

# Initialize values from the environment.
# Any command line parameters will override these.
Compression="${GC_SK_COMPRESSION}"
if [[ -z "${Compression}" ]] ; then
	Compression="LZ4"
fi
if [[ -n "${GC_SK_LOG_LEVEL}" ]] ; then 
	logLevel="${GC_SK_LOG_LEVEL}"
fi
if [[ -n "${GC_SK_NATIVE_ONLY_FILE}" ]] ; then 
	nativeFSOnlyFile="${GC_SK_NATIVE_ONLY_FILE}"
fi

coreLimit="unlimited"
forceSKFSDirCreation="false"
waitForSkfsdBeforeExiting=""

while getopts "c:z:g:C:s:l:n:L:f:E:A:N:T:w" opt; do
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
	w) waitForSkfsdBeforeExiting="true" ;;
    *) usage
    esac
done
shift $(($OPTIND - 1))

source ../lib/common.lib
f_printSection "BASIC ARGUMENTS PARSED"
echo "GCName:                    $GCName"
echo "nodeControlCommand:        $nodeControlCommand"
echo "zkEnsemble:                $zkEnsemble"
echo "forceSKFSDirCreation:      $forceSKFSDirCreation"
echo "waitForSkfsdBeforeExiting: $waitForSkfsdBeforeExiting"

SKFSD_PATTERN="skfsd.*${GCName}"
   
  CHECK_SKFS_COMMAND="CheckSKFS"
   STOP_SKFS_COMMAND="StopSKFS"

if [[ -z "${GCName}"  || -z "${nodeControlCommand}" || -z "${zkEnsemble}" ]] ; then
  echo "Missing required argument"
  usage
fi
if [[ $nodeControlCommand == $CHECK_SKFS_COMMAND || $nodeControlCommand == $STOP_SKFS_COMMAND ]]; then
    echo
else
    echo "Unknown command: '$nodeControlCommand'"
    usage
fi

## logging level option
if [[ "${logLevel}" != "OPS" && "${logLevel}" != "FINE" && "${logLevel}" != "INFO" ]] ; then
	if [[ -n "${logLevel}" ]] ; then
		echo "Unsupported log level: ${logLevel}. Re-setting logLevel to default: OPS."
	fi
	logLevel="OPS";
fi
if [[ "${logLevel}" == "FINE" ]] ; then
	## set verbosity to true ( it sets fuse -d option)
	verbosity="true"; 
else
	verbosity="false";
fi

f_printSection "PRE-EXISTING CHECKS"
f_printSubSection "Checking for skfs"
id=$(f_getSkfsPid)
if [[ -n "$id" ]]; then
    f_printSkfsFound
	if [[ $nodeControlCommand == $CHECK_SKFS_COMMAND ]] ; then
		f_printPass
		exit
	fi
else
	f_printSkfsNotFound
fi

f_printSubSection "Checking GC File"
fullGcFilePath=$GC_DEFAULT_BASE/$GCName.env
if [[ ! -e $fullGcFilePath ]] ; then
    echo "Can't find configuration file $fullGcFilePath"
	f_printFail
    exit
fi
source $fullGcFilePath
if [[ -z "${GC_SK_NAME}" ]] ; then
    echo "Error in $fullGcFilePath - can't find 'GC_SK_NAME'"
	f_printFail
    exit
fi

SK_PATTERN="DHTNode.*${GC_SK_NAME}"
f_printSubSection "Checking for sk"
id=`pgrep -f $SK_PATTERN`
if [[ -n "$id" ]]; then
    echo "FOUND - '$GC_SK_NAME' $id"
else
    echo "NOT FOUND - '$GC_SK_NAME'"
    if [[ $nodeControlCommand == $CHECK_SKFS_COMMAND ]] ; then 
		echo "Sk daemon needs to exist in order to execute: $nodeControlCommand"
		f_printFail
        exit -1
    fi
fi

f_printSection "DOING SKFS inits"
f_printSubSection "Configuring CLASSPATH and SK VARS"
echo "curDir: $curDir"
jaceLibs=${SK_JACE_HOME}/lib/jace-core.jar:${SK_JACE_HOME}/lib/jace-runtime.jar
if [[ -z "${skGlobalCodebase}" ]]; then 
	typeset cp=$(f_getClasspath "../../lib" "$curDir")
	export CLASSPATH=$cp:${jaceLibs}:${SK_JAVA_HOME}/jre/lib/rt.jar;
	export SK_CLASSPATH=${CLASSPATH}
else
	export CLASSPATH=${skGlobalCodebase}:${jaceLibs}
	export SK_CLASSPATH=${CLASSPATH}
fi

echo "CLASSPATH:    $CLASSPATH"
echo "SK_JAVA_HOME: $SK_JAVA_HOME"
echo "SK_JACE_HOME: $SK_JACE_HOME"
echo "SK_CLASSPATH: $SK_CLASSPATH"

tmpfile=/tmp/skfs.${USER}.$$
tmpfileConf=${tmpfile}.conf
f_printSubSection "Retrieving '$GCName' config from '$zkEnsemble' into '$tmpfile'"
UTIL_CLASS="com.ms.silverking.cloud.skfs.management.MetaUtil"
utilCmd="${SK_JAVA_HOME}/bin/java ${UTIL_CLASS} -c GetFromZK -d ${GCName} -z ${zkEnsemble} -t ${tmpfile}"
echo ${utilCmd}
${utilCmd}
if [[ $? != 0 ]] ; then
	echo "MetaUtil failed to get '$GCName' configuration from '$zkEnsemble' into '$tmpfile', exiting" ;
	f_printFail
	exit 1 ;
fi

f_printSubSection "Renaming '$tmpfile' -> '$tmpfileConf'"
grep -v "^null$" $tmpfile > $tmpfileConf
chmod 755 $tmpfileConf
source $tmpfileConf
rm $tmpfile

#source from config environment
if [[ -n "${SKFS_DHT_OP_MIN_TIMEOUT_MS}" ]] ; then 
	dhtOpMinTimeoutMS="${SKFS_DHT_OP_MIN_TIMEOUT_MS}"
fi
if [[ -n "${SKFS_DHT_OP_MAX_TIMEOUT_MS}" ]] ; then 
	dhtOpMaxTimeoutMS="${SKFS_DHT_OP_MAX_TIMEOUT_MS}"
fi
if [[ -n "${SKFS_NATIVE_FILE_MODE}" ]] ; then 
	nativeFileMode="${SKFS_NATIVE_FILE_MODE}"
fi
if [[ -n "${SKFS_BR_REMOTE_ADDRESS_FILE}" ]] ; then 
	brRemoteAddressFile="${SKFS_BR_REMOTE_ADDRESS_FILE}"
fi
if [[ -n "${SKFS_BR_PORT}" ]] ; then 
	brPort="${SKFS_BR_PORT}"
fi
if [[ -n "${SKFS_RECONCILIATION_SLEEP}" ]] ; then 
	reconciliationSleep="${SKFS_RECONCILIATION_SLEEP}"
fi
if [[ -n "${SKFS_ODW_MIN_WRITE_INTERVAL_MILLIS}" ]] ; then 
	odwMinWriteIntervalMillis="${SKFS_ODW_MIN_WRITE_INTERVAL_MILLIS}"
fi
if [[ -n "${SKFS_SYNC_DIR_UPDATES}" ]] ; then 
	syncDirUpdates="${SKFS_SYNC_DIR_UPDATES}"
fi

f_printSection "TEARING DOWN OLD SKFS"
f_printSubSection "Unmounting FUSE"
# Must be set in conf:
#useBigWrites="";
#fuseLib
#fuseBin
#fuseLibKO

echo "fuseBin: $fuseBin"
export PATH=${PATH}:${fuseBin}

${fuseBin}/fusermount -u $skfsMount 
sleep 1
${fuseBin}/fusermount -z -u $skfsMount 
sleep 1

echo "Removing old mount: $skfsMount"
rmdir $skfsMount

f_printSubSection "Trying last resort pkill skfsd '$SKFSD_PATTERN'"
f_printSkfsCheck ", so pkill should find this"
echo "running pkill"
pkill -9 -f $SKFSD_PATTERN
if [[ $? -eq 0 ]]; then
    echo "  Matched"
else
    echo "  No Matches"
fi
f_printSkfsCheck

if [[ $nodeControlCommand == $STOP_SKFS_COMMAND ]] ; then 
	f_printSkfsStopWithResult
fi

f_printSection "SETTING UP NEW SKFS"
if [[ "${forceSKFSDirCreation}" != "false" ]] ; then
    if [[ -e $skfsBase ]] ; then
		echo "Creating new SKFS Dir"
		echo mv ${skfsBase} ${skfsBase}.$$
		mv ${skfsBase} ${skfsBase}.$$
		echo $?
		echo mkdir ${skfsBase}
		mkdir ${skfsBase}
		echo $?
    fi
fi

if [[ ! -e $skfsLogs ]] ; then
	echo "Creating log:     $skfsLogs"
	mkdir -m 777 -p $skfsLogs
fi

if [[ ! -e $skfsMount ]] ; then
	echo "Creating mount:   $skfsMount"
	mkdir -m 777 -p $skfsMount
fi

echo
echo "zkEnsemble:       $zkEnsemble"
echo "GCName:           $GCName"
echo "Compression:      $Compression"
echo "checksum:         $checksum"
echo "skGlobalCodebase: $skGlobalCodebase"

##nativeFSOnlyFile - file with csv files/dirs list that will be accessed only from native FS
if [[ -n "${nativeFSOnlyFile}" ]] ; then
	if [[ ! -f ${nativeFSOnlyFile} ]] ; then
		touch ${nativeFSOnlyFile}
	fi
else
	## path is set to default file name
	nativeFSOnlyFile=${fsNativeOnlyFile}
	touch ${nativeFSOnlyFile}
fi

export LD_LIBRARY_PATH=${gccPath}:${fuseLibKO}:${fuseLib}:${SK_JACE_HOME}/lib/dynamic:${SK_JAVA_HOME}/jre/lib/amd64/server
echo "LD_LIBRARY_PATH: $LD_LIBRARY_PATH"

echo "Core limit:      $coreLimit"
ulimit -c ${coreLimit}

f_printSubSection "Configuring path to skfsd"
# Determine path to binary
# skLocalSys variable may come from host group vars; if so, that overrides all
if [[ -z "${skLocalSys}" ]]; then
    # Otherwise, check to see if we're running in a dev env
    # If not running in dev env, just use path without any system component
    if [[ -n "${skGlobalCodebase}" ]]; then
        # In a dev environment, use 'uname -r'
        skLocalSys=`uname -r`
		
		if [[ ! -e $skLocalSys ]]; then
			echo "Trying to use '$skLocalSys', but no folder exists."
			typeset rhVersion=`echo $skLocalSys | grep -P -o "el\d"`
			if [[ -n $rhVersion ]]; then
				echo "So trying to find a similar rh${rhVersion} version."
				skLocalSys=`ls | grep $rhVersion`
				if [[ -n $skLocalSys ]]; then
					echo "Found '$skLocalSys', will try that"
				else 
					echo "None found, will use default path to skfsd"
				fi
			else
				echo "Using default path to skfsd"
				skLocalSys=""
			fi
		fi
    fi
fi

echo "skLocalSys: $skLocalSys"
    
# Add '/' if needed
if [[ -n "${skLocalSys}" ]]; then
    skLocalSysPath="/${skLocalSys}"
else
    skLocalSysPath=""
fi

# Full path to binary
FS_EXEC="${curDir}${skLocalSysPath}/skfsd"
echo "skfsd path: $FS_EXEC"

if [[ ! -e $FS_EXEC ]]; then
	echo "'$FS_EXEC' doesn't exist. How am I supposed to start skfs w/o a valid binary? Quitting..."
	f_printFail
	exit
fi

SKFS_MOUNT=$skfsMount
SKFS_LOG_DIR=$skfsLogs

fbwQOption="--fbwReliableQueue=TRUE"
noFBWPaths=""

f_printSubSection "Configuring options"
# override any class vars with cmd line params
if [[ -n "${_skfsEntryTimeoutSecs}" ]] ; then
    skfsEntryTimeoutSecs=${_skfsEntryTimeoutSecs}
fi
if [[ -n "${_skfsAttrTimeoutSecs}" ]] ; then
    skfsAttrTimeoutSecs=${_skfsAttrTimeoutSecs}
fi
if [[ -n "${_skfsNegativeTimeoutSecs}" ]] ; then
    skfsNegativeTimeoutSecs=${_skfsNegativeTimeoutSecs}
fi
if [[ -n "${_transientCacheSizeKB}" ]] ; then
    transientCacheSizeKB=${_transientCacheSizeKB}
fi

# now set the options
if [[ -n "${skfsEntryTimeoutSecs}" ]] ; then
    entryTimeoutOption="--entryTimeoutSecs=${skfsEntryTimeoutSecs}"
fi
if [[ -n "${skfsAttrTimeoutSecs}" ]] ; then
    attrTimeoutOption="--attrTimeoutSecs=${skfsAttrTimeoutSecs}"
fi
if [[ -n "${skfsNegativeTimeoutSecs}" ]] ; then
    negativeTimeoutOption="--negativeTimeoutSecs=${skfsNegativeTimeoutSecs}"
fi
if [[ -n "${dhtOpMinTimeoutMS}" ]] ; then
    dhtOpMinTimeoutMSOption="--dhtOpMinTimeoutMS=${dhtOpMinTimeoutMS}"
fi
if [[ -n "${dhtOpMaxTimeoutMS}" ]] ; then
    dhtOpMaxTimeoutMSOption="--dhtOpMaxTimeoutMS=${dhtOpMaxTimeoutMS}"
fi
if [[ -n "${nativeFileMode}" ]] ; then
    nativeFileModeOption="--nativeFileMode=${nativeFileMode}"
fi
if [[ -n "${brRemoteAddressFile}" ]] ; then
    brRemoteAddressFileOption="--brRemoteAddressFile=${brRemoteAddressFile}"
fi
if [[ -n "${brPort}" ]] ; then
    brPortOption="--brPort=${brPort}"
fi
if [[ -n "${reconciliationSleep}" ]] ; then
    reconciliationSleepOption="--reconciliationSleep=${reconciliationSleep}"
fi
if [[ -n "${odwMinWriteIntervalMillis}" ]] ; then
    odwMinWriteIntervalMillisOption="--odwMinWriteIntervalMillis=${odwMinWriteIntervalMillis}"
fi
if [[ -n "${syncDirUpdates}" ]] ; then
    syncDirUpdatesOption="--syncDirUpdates=${syncDirUpdates}"
fi

echo "entryTimeoutOption:    $entryTimeoutOption"
echo "attrTimeoutOption:     $attrTimeoutOption"
echo "negativeTimeoutOption: $negativeTimeoutOption"

if [[ -n "${jvmOptions}" ]] ; then
	skfsJvmOpt="--jvmOptions=${jvmOptions}";
fi

if [[ -z "${transientCacheSizeKB}" ]] ; then
    memKB=`cat /proc/meminfo | grep MemTotal | gawk '{print $2}' `
    transientCacheSizeLimitKB=12000000
    transientCacheSizeKB=$((${memKB} / 48))
    if [[ ${transientCacheSizeKB} -gt ${transientCacheSizeLimitKB} ]]; then
        transientCacheSizeKB=${transientCacheSizeLimitKB}
    fi 
fi
echo "transientCacheSizeKB:  ${transientCacheSizeKB}"

#for key in "${!mountMap[@]}"; do
#    ls ${mountMap[$key]}
#done

f_printSubSection "Making mount and starting fusectl"
load_module="${fuseBin}/fusectl start > ${SKFS_LOG_DIR}/load.log 2>&1"
run_cmd="$load_module"
echo "$run_cmd"
eval $run_cmd
sleep 6 

f_printSubSection "Writing to tmpFile"
tmpFile=/tmp/$$.skfs.fuse.tmp
rm -v $tmpFile 
echo "writing to tmpFile: $tmpFile"
echo "export PATH=${SK_JAVA_HOME}/bin:${PATH}:${fuseBin}:" >> $tmpFile
# note -d option is currently in skfs.c
export start_fuse="nohup $FS_EXEC --mount=${SKFS_MOUNT} --verbose=${verbosity} --host=localhost --gcname=${GCName} --zkLoc=${zkEnsemble} --compression=${Compression} --nfsMapping=${nfsMapping} --permanentSuffixes=${permanentSuffixes} --noErrorCachePaths=${noErrorCachePaths} --noLinkCachePaths=${noLinkCachePaths} --snapshotOnlyPaths=${snapshotOnlyPaths} --taskOutputPaths=${taskOutputPaths} --compressedPaths=${compressedPaths} --noFBWPaths=${noFBWPaths} ${fbwQOption} --fsNativeOnlyFile=${nativeFSOnlyFile} --transientCacheSizeKB=${transientCacheSizeKB} --logLevel=${logLevel} ${useBigWrites} ${entryTimeoutOption} ${attrTimeoutOption} ${negativeTimeoutOption} ${dhtOpMinTimeoutMSOption} ${dhtOpMaxTimeoutMSOption} ${nativeFileModeOption} ${brRemoteAddressFileOption}  ${brPortOption} ${reconciliationSleepOption} ${odwMinWriteIntervalMillisOption} ${syncDirUpdatesOption} ${skfsJvmOpt} > ${SKFS_LOG_DIR}/fuse.log.$$ 2>&1"
#echo "export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}" >> $tmpFile  
#echo "export MALLOC_ARENA_MAX=4" >> $tmpFile
#echo "export CLASSPATH=${CLASSPATH}" >> $tmpFile
echo "$start_fuse" >> $tmpFile
chmod +x $tmpFile

# possible sanity check against concurrent CheckSKFS processes
f_printSubSection "Checking for skfs"
f_printSkfsCheckWithResultOnlyIfFound

f_printSubSection "Starting fuse"
cat $tmpFile
$tmpFile &
sleep 1

cd $old_dir

f_printSubSection "Checking for skfs again"
f_printSkfsCheckWithResult
