// skfs.c

#ifndef FUSE_USE_VERSION
#define FUSE_USE_VERSION 26
#endif

/////////////
// includes

////#include <config.h>//use only in conjunction with configure/makefile

#ifdef linux
/* For pread()/pwrite() */
#define _XOPEN_SOURCE 500
#endif

#include <argp.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <fuse.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#ifdef HAVE_SETXATTR
#include <sys/xattr.h>
#endif
#include <valgrind/valgrind.h>

#include <string>
#include <set>
#include <vector>

#include "AttrReader.h"
#include "AttrWriter.h"
#include "BlockReader.h"
#include "FileBlockReader.h"
#include "FileBlockWriter.h"
#include "FileIDToPathMap.h"
//#include "NSKeySplit.h"
#include "OpenDirTable.h"
#include "PartialBlockReader.h"
#include "PathGroup.h"
#include "ReconciliationSet.h"
#include "ResponseTimeStats.h"
#include "skfs.h"
#include "SKFSOpenFile.h"
#include "SRFSConstants.h"
#include "SRFSDHT.h"
#include "Util.h"
#include "WritableFile.h"
#include "WritableFileTable.h"

#include <signal.h>
#include <sys/inotify.h>

////////////
// defines

// inotify-related defs
#define EVENT_SIZE  ( sizeof (struct inotify_event) )
#define INOTIFY_BUF_LEN     ( 1024 * ( EVENT_SIZE + 16 ) )

#define SO_VERBOSE 'v'
#define SO_HOST 'h'
#define SO_ZK_LOC 'z'
#define SO_GC_NAME 'G'
#define SO_MOUNT 'm'
#define SO_NFS_MAPPING 'n'
#define SO_PERMANENT_SUFFIXES 's'
#define SO_NO_ERROR_CACHE_PATHS 'e'
#define SO_NO_LINK_CACHE_PATHS 'o'
#define SO_FS_NATIVE_ONLY_FILE 'f'
#define SO_SNAPSHOT_ONLY_PATHS 'a'
#define SO_TASK_OUTPUT_PATHS 'g'
#define SO_COMPRESSED_PATHS 'C'
#define SO_NO_FBW_PATHS 'w'
#define SO_FBW_RELIABLE_QUEUE 'q'
#define SO_COMPRESSION 'c'
#define SO_CHECKSUM 'S'
#define SO_TRANSIENT_CACHE_SIZE_KB 'T'
#define SO_CACHE_CONCURRENCY 'y'
#define SO_LOG_LEVEL 'l'
#define SO_JVM_OPTIONS 'J'
#define SO_BIGWRITES 'B'
#define SO_ENTRY_TIMEOUT_SECS 'E'
#define SO_ATTR_TIMEOUT_SECS 'A'
#define SO_NEGATIVE_TIMEOUT_SECS 'N'
#define SO_DHT_OP_MIN_TIMEOUT_MS 'x'
#define SO_DHT_OP_MAX_TIMEOUT_MS 'X'
#define SO_NATIVE_FILE_MODE 'F'
#define SO_REMOTE_ADDRESS_FILE 'R'
#define SO_BLOCK_READER_PORT 'P'
#define SO_RECONCILIATION_SLEEP 'L'
#define SO_ODW_MIN_WRITE_INTERVAL_MILLIS 'I'
#define SO_SYNC_DIR_UPDATES 'U'

#define LO_VERBOSE "verbose"
#define LO_HOST "host"
//#define LO_PORT "port"
#define LO_ZK_LOC "zkLoc"
//#define LO_DHT_NAME "dhtName"
#define LO_GC_NAME "gcname"
#define LO_MOUNT "mount" 
#define LO_NFS_MAPPING "nfsMapping" 
#define LO_PERMANENT_SUFFIXES "permanentSuffixes" 
#define LO_NO_ERROR_CACHE_PATHS "noErrorCachePaths"
#define LO_NO_LINK_CACHE_PATHS "noLinkCachePaths"
#define LO_FS_NATIVE_ONLY_FILE "fsNativeOnlyFile"
#define LO_SNAPSHOT_ONLY_PATHS "snapshotOnlyPaths"
#define LO_TASK_OUTPUT_PATHS "taskOutputPaths"
#define LO_COMPRESSED_PATHS "compressedPaths"
#define LO_NO_FBW_PATHS "noFBWPaths"
#define LO_FBW_RELIABLE_QUEUE "fbwReliableQueue"
#define LO_COMPRESSION "compression"
#define LO_CHECKSUM "checksum"
#define LO_TRANSIENT_CACHE_SIZE_KB "transientCacheSizeKB"
#define LO_CACHE_CONCURRENCY "cacheConcurrency"
#define LO_LOG_LEVEL "logLevel"
#define LO_JVM_OPTIONS "jvmOptions"
#define LO_BIGWRITES "bigwrites"
#define LO_ENTRY_TIMEOUT_SECS "entryTimeoutSecs"
#define LO_ATTR_TIMEOUT_SECS "attrTimeoutSecs"
#define LO_NEGATIVE_TIMEOUT_SECS "negativeTimeoutSecs"
#define LO_DHT_OP_MIN_TIMEOUT_MS "dhtOpMinTimeoutMS"
#define LO_DHT_OP_MAX_TIMEOUT_MS "dhtOpMaxTimeoutMS"
#define LO_NATIVE_FILE_MODE "nativeFileMode"
#define LO_REMOTE_ADDRESS_FILE "brRemoteAddressFile"
#define LO_BLOCK_READER_PORT "brPort"
#define LO_RECONCILIATION_SLEEP "reconciliationSleep"
#define LO_ODW_MIN_WRITE_INTERVAL_MILLIS "odwMinWriteIntervalMillis"
#define LO_SYNC_DIR_UPDATES "syncDirUpdates"


#define OPEN_MODE_FLAG_MASK 0x3

#define ODT_NAME "OpenDirTable"

#define SKFS_FUSE_OPTION_STRING_LENGTH	128
#define _SKFS_RENAME_RETRY_DELAY_MS    5
#define _SKFS_RENAME_MAX_ATTEMPTS   20
#define _SKFS_TIMEOUT_NEVER 0x0fffffff
#define _FBC_NAME "FileBlockCache"
#define _MAX_REASONABLE_DISK_STAT_LENGTH 32

#define _rn_retry_limit 20
#define _rn_retry_interval_ms 10

#define _NUM_NATIVE_FILE_MODES 3
#define _DEFAULT_NATIVE_FILE_MODE nf_readRelay_localPreread
#define _PREREAD_SIZE 1


///////////////////////
// private prototypes

static error_t parse_opt(int key, char *arg, struct argp_state *state);
static void *stats_thread(void *);
static void * nativefile_watcher_thread(void * unused);
static void initPaths();
static void initReaders();
static void initDirs();
static void destroyReaders();
static void destroyPaths();
static int _skfs_unlink(const char *path, int deleteBlocks = TRUE);
static void add_fuse_option(char *option);
static int ensure_not_writable(char *path1, char *path2 = NULL);
static int modify_file_attr(const char *path, char *fnName, mode_t *mode, uid_t *uid, gid_t *gid);
static int _modify_file_attr(const char *path, char *fnName, mode_t *mode, uid_t *uid, gid_t *gid, 
                            const struct timespec *last_access_tp, const struct timespec *last_modification_tp, const struct timespec *last_change_tp);
static void skfs_destroy(void* private_data);
static void init_util_sk();
static uint64_t _get_sk_system_uint64(const char *stat);
static NativeFileMode parseNativeFileMode(char *s);

static int skfs_rename_directory(char *oldPath, char *newPath, time_t curEpochTimeSeconds, long curTimeNanos, std::vector<char *> *unlinkList);
static int skfs_rename_file(char *oldPath, char *newPath, time_t curEpochTimeSeconds, long curTimeNanos, std::vector<char *> *unlinkList);
static int skfs_unlink_list(std::vector<char *> *unlinkList);

////////////
// globals

static struct argp_option options[] = {
       {LO_VERBOSE,  SO_VERBOSE, LO_VERBOSE,      0,  "Mode/test to run", 0 },
       {LO_HOST,  SO_HOST, LO_HOST,      0,  "Host name", 0 },
//       {LO_PORT,  SO_PORT, LO_PORT,      0,  "Port" , 0},
       {LO_ZK_LOC,  SO_ZK_LOC, LO_ZK_LOC,      0,  "Zookeeper Locations" , 0},
//       {LO_DHT_NAME,  SO_DHT_NAME, LO_DHT_NAME,      0,  "DHT Name", 0 },
       {LO_GC_NAME,  SO_GC_NAME, LO_GC_NAME,      0,  "GridConfig Name", 0 },
       {LO_MOUNT,  SO_MOUNT, LO_MOUNT,      0,  "Mount", 0 },
       {LO_NFS_MAPPING,  SO_NFS_MAPPING, LO_NFS_MAPPING,      0,  "NFS mapping", 0 },
       {LO_PERMANENT_SUFFIXES,   SO_PERMANENT_SUFFIXES,  LO_PERMANENT_SUFFIXES,       0,  "Permanent suffixes", 0 },
       {LO_NO_ERROR_CACHE_PATHS, SO_NO_ERROR_CACHE_PATHS, LO_NO_ERROR_CACHE_PATHS,    0,  "No error cache paths", 0 },
       {LO_NO_LINK_CACHE_PATHS,  SO_NO_LINK_CACHE_PATHS, LO_NO_LINK_CACHE_PATHS,      0,  "No link cache paths", 0 },
       {LO_FS_NATIVE_ONLY_FILE,  SO_FS_NATIVE_ONLY_FILE, LO_FS_NATIVE_ONLY_FILE,      0,  "fs native only file", 0 },
       {LO_SNAPSHOT_ONLY_PATHS,  SO_SNAPSHOT_ONLY_PATHS, LO_SNAPSHOT_ONLY_PATHS,      0,  "Snapshot only paths", 0 },
       {LO_COMPRESSED_PATHS,     SO_COMPRESSED_PATHS,    LO_COMPRESSED_PATHS,    OPTION_ARG_OPTIONAL,  "Compressed paths", 0 },
       {LO_NO_FBW_PATHS,         SO_NO_FBW_PATHS,        LO_NO_FBW_PATHS,        OPTION_ARG_OPTIONAL,  "no fbw paths", 0 },
       {LO_FBW_RELIABLE_QUEUE,   SO_FBW_RELIABLE_QUEUE,  LO_FBW_RELIABLE_QUEUE,  OPTION_ARG_OPTIONAL,  "fbwReliableQueue", 0 },
       {LO_TASK_OUTPUT_PATHS,    SO_TASK_OUTPUT_PATHS,   LO_TASK_OUTPUT_PATHS,        0,  "Task output path mappings", 0 },
       {LO_COMPRESSION,          SO_COMPRESSION,         LO_COMPRESSION,              0,  "Compression", 0 },
       {LO_CHECKSUM,             SO_CHECKSUM,            LO_CHECKSUM,                 0,  "Checksum", 0 },
       {LO_TRANSIENT_CACHE_SIZE_KB,  SO_TRANSIENT_CACHE_SIZE_KB, LO_TRANSIENT_CACHE_SIZE_KB,      0,  "transientCacheSizeKB", 0 },
       {LO_CACHE_CONCURRENCY,   SO_CACHE_CONCURRENCY, LO_CACHE_CONCURRENCY,           0,  "cacheConcurrency", 0 },
	   {LO_LOG_LEVEL,           SO_LOG_LEVEL,            LO_LOG_LEVEL,                0, "logLevel", 0},
	   {LO_JVM_OPTIONS,         SO_JVM_OPTIONS,          LO_JVM_OPTIONS,              0, "comma-separated jvmOptions", 0},
       {LO_BIGWRITES,           SO_BIGWRITES,            LO_BIGWRITES,           OPTION_ARG_OPTIONAL,  "enable big_writes", 0 },
       {LO_ENTRY_TIMEOUT_SECS, SO_ENTRY_TIMEOUT_SECS,    LO_ENTRY_TIMEOUT_SECS,  0,  "entry timeout seconds", 0 },
       {LO_ATTR_TIMEOUT_SECS, SO_ATTR_TIMEOUT_SECS,    LO_ATTR_TIMEOUT_SECS,  0,  "attr timeout seconds", 0 },
       {LO_NEGATIVE_TIMEOUT_SECS, SO_NEGATIVE_TIMEOUT_SECS,    LO_NEGATIVE_TIMEOUT_SECS,  0,  "negative timeout seconds", 0 },
       {LO_DHT_OP_MIN_TIMEOUT_MS, SO_DHT_OP_MIN_TIMEOUT_MS,    LO_DHT_OP_MIN_TIMEOUT_MS,  0,  "min dht timeout ms", 0 },
       {LO_DHT_OP_MAX_TIMEOUT_MS, SO_DHT_OP_MAX_TIMEOUT_MS,    LO_DHT_OP_MAX_TIMEOUT_MS,  0,  "max dht timeout ms", 0 },
       {LO_NATIVE_FILE_MODE, SO_NATIVE_FILE_MODE, LO_NATIVE_FILE_MODE,  0, "nativeFileMode", 0 },
       {LO_REMOTE_ADDRESS_FILE, SO_REMOTE_ADDRESS_FILE, LO_REMOTE_ADDRESS_FILE,  0, "brRemoteAddressFile", 0 },
       {LO_BLOCK_READER_PORT, SO_BLOCK_READER_PORT, LO_BLOCK_READER_PORT, 0, "brPort", 0 },
       {LO_RECONCILIATION_SLEEP, SO_RECONCILIATION_SLEEP, LO_RECONCILIATION_SLEEP, 0, "reconciliationSleep", 0 },
       {LO_ODW_MIN_WRITE_INTERVAL_MILLIS, SO_ODW_MIN_WRITE_INTERVAL_MILLIS, LO_ODW_MIN_WRITE_INTERVAL_MILLIS, 0, "odwMinWriteIntervalMillis", 0 },
       {LO_SYNC_DIR_UPDATES, SO_SYNC_DIR_UPDATES, LO_SYNC_DIR_UPDATES, 0, "syncDirUpdates", 0 },
       { 0, 0, 0, 0, 0, 0 }
};
static char *nativeFileModes[] = {"nf_blockReadOnly", "nf_readRelay_localPreread", "nf_readRelay_distributedPreread"};

static char doc[] = "skfs";
static char args_doc[] = "";
static struct argp argp = {options, parse_opt, args_doc, doc, 0, 0, 0 };

static CmdArgs _args;
CmdArgs *args = &_args;

static struct fuse_args fuseArgs = FUSE_ARGS_INIT(0, NULL);

static PartialBlockReader *pbr;
static FileBlockReader *fbr;
static AttrReader	*ar;
//static DirReader	*dr;
static OpenDirTable	*odt;
static FileIDToPathMap	*f2p;
static WritableFileTable	*wft;
static AttrWriter	*aw;
static AttrWriter	*awSKFS;
static FileBlockWriter	*fbwCompress;
static FileBlockWriter	*fbwRaw;
static FileBlockWriter	*fbwSKFS;
static SRFSDHT	*sd;
static ResponseTimeStats	*rtsAR_DHT;
static ResponseTimeStats	*rtsAR_NFS;
static ResponseTimeStats	*rtsFBR_DHT;
static ResponseTimeStats	*rtsFBR_NFS;
static ResponseTimeStats	*rtsODT;
static PathGroup volatile	*fsNativeOnlyPaths;
static char			*fsNativeOnlyFile;
static BlockReader  *br;

static int			statsIntervalSeconds = 20;
static int			statsDetailIntervalSeconds = 300;
static pthread_t	statsThread;
static pthread_t	nativeFileWatcherThread;

static char	logFileName[SRFS_MAX_PATH_LENGTH];
static char	logFileNameDht[SRFS_MAX_PATH_LENGTH];
static FILE * dhtCliLog = NULL; 

static char	fuseEntryOption[SKFS_FUSE_OPTION_STRING_LENGTH];
static char	fuseAttrOption[SKFS_FUSE_OPTION_STRING_LENGTH];
static char	fuseACAttrOption[SKFS_FUSE_OPTION_STRING_LENGTH];
static char	fuseNegativeOption[SKFS_FUSE_OPTION_STRING_LENGTH];

SKSessionOptions	*sessOption;

static int  destroyCalled;
static pthread_spinlock_t	destroyLockInstance;
static pthread_spinlock_t	*destroyLock = &destroyLockInstance;

static SKSession    *pUtilSession;
static SKSyncNSPerspective *systemNSP;


///////////////////
// implementation

// argument parsing

static int parseTimeout(char *arg) {
    int val;

    val = -1;
    if (arg != NULL) {
        val = atoi(arg);
        if (val < 0) {
            val = _SKFS_TIMEOUT_NEVER;
        }
    } else {
        fatalError("Unexpected NULL arg", __FILE__, __LINE__);
    }
    return val;
}

static NativeFileMode parseNativeFileMode(char *s) {
    int i;
    
    for (i = 0; i < _NUM_NATIVE_FILE_MODES; i++) {
        if (!strcmp(s, nativeFileModes[i])) {
            return (NativeFileMode)i;
        }
    }
    return _DEFAULT_NATIVE_FILE_MODE;
}

static int parseBoolean(char *arg) {
    if (!strcmp(arg, "NO") || !strcmp(arg, "FALSE") || !strcmp(arg, "no") || !strcmp(arg, "false") || !strcmp(arg, "0")) { 
        return 0;
    } else { 
        return 1;
    }
}

static error_t parse_opt(int key, char *arg, struct argp_state *state) {
        /* Get the input argument from argp_parse, which we
           know is a pointer to our CmdArgs structure. */
		int cacheConcur = 0;
        CmdArgs *arguments = (struct CmdArgs *)state->input;
        switch (key) {
                case SO_HOST:
                        arguments->host = arg;
                        break;
                case SO_ZK_LOC:
                        arguments->zkLoc = arg;
                        break;
                case SO_GC_NAME:
                        arguments->gcname = arg;
                        break;
                case SO_NFS_MAPPING:
                        arguments->nfsMapping = arg;
                        break;
                case SO_PERMANENT_SUFFIXES:
                        arguments->permanentSuffixes = arg;
                        break;
                case SO_NO_ERROR_CACHE_PATHS:
                        arguments->noErrorCachePaths = arg;
                        break;
                case SO_NO_LINK_CACHE_PATHS:
                        arguments->noLinkCachePaths = arg;
                        break;
                case SO_FS_NATIVE_ONLY_FILE:
                        arguments->fsNativeOnlyFile = arg;
                        break;
                case SO_SNAPSHOT_ONLY_PATHS:
                        arguments->snapshotOnlyPaths = arg;
                        break;
                case SO_COMPRESSED_PATHS:
                        arguments->compressedPaths = arg;
                        break;
                case SO_NO_FBW_PATHS:
                        arguments->noFBWPaths = arg;
                        break;
                case SO_FBW_RELIABLE_QUEUE:
                        arguments->fbwReliableQueue = parseBoolean(arg);
                        break;
                case SO_TASK_OUTPUT_PATHS:
                        arguments->taskOutputPaths = arg;
                        break;
                case SO_COMPRESSION:
						if (!strcmp(arg, "NONE")) { 
							arguments->compression = SKCompression::NONE;
						} else if (!strcmp(arg, "ZIP")) { 
							arguments->compression = SKCompression::ZIP;
						} else if (!strcmp(arg, "BZIP2")) { 
							arguments->compression = SKCompression::BZIP2;
						} else if (!strcmp(arg, "SNAPPY")) { 
							arguments->compression = SKCompression::SNAPPY;
                        } else if (!strcmp(arg, "LZ4")) { 
                            arguments->compression = SKCompression::LZ4;
						} else {
							arguments->compression = SKCompression::NONE;
						}
                        break;
                case SO_CHECKSUM:
                    {
						if (!strcmp(arg, "NONE")) { 
                            arguments->checksum = SKChecksumType::NONE;
						} else if (!strcmp(arg, "MD5")) { 
							arguments->checksum = SKChecksumType::MD5;
						} else if (!strcmp(arg, "SHA_1")) { 
							arguments->checksum = SKChecksumType::SHA_1;
						} else if (!strcmp(arg, "MURMUR3_32")) { 
							arguments->checksum = SKChecksumType::MURMUR3_32;
						} else if (!strcmp(arg, "MURMUR3_128")) { 
							arguments->checksum = SKChecksumType::MURMUR3_128;
						} else {
							arguments->checksum = SKChecksumType::NONE;
						}
                        break;
                    }
				case SO_TRANSIENT_CACHE_SIZE_KB:
						arguments->transientCacheSizeKB = atoi(arg);
						break;
				case SO_CACHE_CONCURRENCY:
						cacheConcur = atoi(arg);
						if(cacheConcur > 0) arguments->cacheConcurrency = cacheConcur ;
						if(arguments->cacheConcurrency == 0) arguments->cacheConcurrency = 1;
						break;
                case SO_VERBOSE:
                        arguments->verbose = parseBoolean(arg);
                        break;
                case ARGP_KEY_ARG:
                        // if (state->arg_num >= 2) {
                        /* Too many arguments. */
                        //argp_usage (state);
                        //}
                        //arguments->args[state->arg_num] = arg;
			srfsLog(LOG_WARNING, "would add %d %s", state->arg_num, state->argv[state->arg_num]); fflush(stdout);
                        break;
                case ARGP_KEY_END:
                        //if (state->arg_num < 2) {
                        /* Not enough arguments. */
                        //argp_usage (state);
                        //}
                        break;
                case SO_MOUNT:
						srfsLog(LOG_WARNING, "mount: %s", arg);
                        add_fuse_option(arg);
                        arguments->mountPath = arg;
                        break;
				case SO_LOG_LEVEL:
						arguments->logLevel = arg;
						break;
				case SO_JVM_OPTIONS:
						arguments->jvmOptions = arg;
						break;
                case SO_BIGWRITES:
                        arguments->enableBigWrites = parseBoolean(arg);
						break;
				case SO_ENTRY_TIMEOUT_SECS:
						arguments->entryTimeoutSecs = parseTimeout(arg);
						break;
				case SO_ATTR_TIMEOUT_SECS:
						arguments->attrTimeoutSecs = parseTimeout(arg);
						break;
				case SO_NEGATIVE_TIMEOUT_SECS:
						arguments->negativeTimeoutSecs = parseTimeout(arg);
						break;
				case SO_DHT_OP_MIN_TIMEOUT_MS:
						arguments->dhtOpMinTimeoutMS = sd_parse_timeout(arg, SRFS_DHT_OP_MIN_TIMEOUT_MS);
						break;
				case SO_DHT_OP_MAX_TIMEOUT_MS:
						arguments->dhtOpMaxTimeoutMS = sd_parse_timeout(arg, SRFS_DHT_OP_MAX_TIMEOUT_MS);
						break;
				case SO_NATIVE_FILE_MODE:
						arguments->nativeFileMode = parseNativeFileMode(arg);
						break;
				case SO_REMOTE_ADDRESS_FILE:
						arguments->brRemoteAddressFile = arg;
						break;
				case SO_BLOCK_READER_PORT:
						arguments->brPort = atoi(arg);
						break;
				case SO_RECONCILIATION_SLEEP:
						arguments->reconciliationSleep = arg;
						break;
				case SO_ODW_MIN_WRITE_INTERVAL_MILLIS:
						arguments->odwMinWriteIntervalMillis = atoi(arg);
						break;
				case SO_SYNC_DIR_UPDATES:
						arguments->syncDirUpdates = parseBoolean(arg);
						break;
                default:
			//printf("Adding %d %s\n", state->arg_num, state->argv[state->arg_num]); fflush(stdout);
			//fuse_opt_add_arg(&fuseArgs, state->argv[state->arg_num]);
                        return ARGP_ERR_UNKNOWN;
			break;
        }
        return 0;
}

static void checkArguments(CmdArgs *arguments) {
    if (arguments->zkLoc == NULL) {
		fatalError(LO_ZK_LOC " not set", __FILE__, __LINE__);
    }
    if (arguments->gcname == NULL) {
		fatalError(LO_GC_NAME " not set", __FILE__, __LINE__);
    }
    if (arguments->host == NULL) {
		fatalError(LO_HOST " not set", __FILE__, __LINE__);
    }
}

void initDefaults(CmdArgs *arguments) {
    arguments->verbose = FALSE;
    arguments->host = "localhost";
    arguments->gcname = NULL;
    arguments->zkLoc = NULL;
    arguments->nfsMapping = NULL;
    arguments->permanentSuffixes = NULL;
	arguments->noErrorCachePaths = NULL;
	arguments->compression = SKCompression::NONE;
	arguments->checksum = SKChecksumType::NONE;
	arguments->transientCacheSizeKB = 0;
	arguments->cacheConcurrency = sysconf(_SC_NPROCESSORS_ONLN);
    arguments->jvmOptions = NULL;
	arguments->enableBigWrites = TRUE;
    arguments->entryTimeoutSecs = -1;
    arguments->attrTimeoutSecs = -1;
    arguments->negativeTimeoutSecs = -1;
    arguments->dhtOpMinTimeoutMS = SRFS_DHT_OP_MIN_TIMEOUT_MS;
    arguments->dhtOpMaxTimeoutMS = SRFS_DHT_OP_MAX_TIMEOUT_MS;
    arguments->nativeFileMode = _DEFAULT_NATIVE_FILE_MODE;
    arguments->brRemoteAddressFile = NULL;
    arguments->brPort = -1;
    arguments->reconciliationSleep = NULL;
    arguments->odwMinWriteIntervalMillis = ODW_DEF_MIN_WRITE_INTERVAL_MILLIS;
    arguments->syncDirUpdates = DEF_SYNC_DIR_UPDATES;
}

static void displayArguments(CmdArgs *arguments) {
    printf("verbose %d\n", (int)arguments->verbose);
    printf("host %s\n", arguments->host);
    printf("gcname %s\n", arguments->gcname);
    printf("zkLoc %s\n", arguments->zkLoc);
    printf("nfsMapping %s\n", arguments->nfsMapping);
    printf("permanentSuffixes %s\n", arguments->permanentSuffixes);
    printf("noErrorCachePaths %s\n", arguments->noErrorCachePaths);
    printf("compression %d\n", arguments->compression);
    printf("checksum %d\n", arguments->checksum);
    printf("transientCacheSizeKB %d\n", arguments->transientCacheSizeKB);
    printf("cacheConcurrency %d\n", arguments->cacheConcurrency);
    printf("jvmOptions %s\n", arguments->jvmOptions);
	printf("enableBigWrites %d\n", arguments->enableBigWrites);
	printf("entryTimeoutSecs %d\n", arguments->entryTimeoutSecs);
	printf("attrTimeoutSecs %d\n", arguments->attrTimeoutSecs);
	printf("negativeTimeoutSecs %d\n", arguments->negativeTimeoutSecs);
	printf("dhtOpMinTimeoutMS %ld\n", arguments->dhtOpMinTimeoutMS);
	printf("dhtOpMaxTimeoutMS %ld\n", arguments->dhtOpMaxTimeoutMS);
	printf("nativeFileMode %s\n", nativeFileModes[arguments->nativeFileMode]);
	printf("brRemoteAddressFile %s\n", arguments->brRemoteAddressFile);
	printf("brPort %d\n", arguments->brPort);
	printf("reconciliationSleep %d\n", arguments->reconciliationSleep);
	printf("odwMinWriteIntervalMillis %d\n", arguments->odwMinWriteIntervalMillis);
	printf("syncDirUpdates %d\n", arguments->syncDirUpdates);
}

// FUSE interface

static int skfs_getattr(const char *path, struct stat *stbuf) {
	srfsLogAsync(LOG_OPS, "_ga %x %s", get_caller_pid(), path);
	if (fsNativeOnlyPaths != NULL && pg_matches(fsNativeOnlyPaths, path)) {
		char nativePath[SRFS_MAX_PATH_LENGTH];

		srfsLogAsync(LOG_OPS, "native lstat");
		ar_translate_path(ar, nativePath, path);
		srfsLog(LOG_FINE, "%s -> %s", path, nativePath);
		return lstat(nativePath, stbuf);
	} else {
		int	result;

        result = ENOENT;
		if (is_writable_path(path)) {
            WritableFileReference    *wf_ref;
            
			odt_record_get_attr(odt, (char *)path);
            
			wf_ref = wft_get(wft, path);
			srfsLog(LOG_FINE, "wf_ref %llx", wf_ref);
			if (wf_ref != NULL) {
                WritableFile	    *wf;
			
                wf = wfr_get_wf(wf_ref);
                srfsLog(LOG_FINE, "wf %llx", wf);
				memcpy(stbuf, &wf->fa.stat, sizeof(struct stat));
				result = 0;
                wfr_delete(&wf_ref, aw, fbwSKFS, ar->attrCache);
			} else {
				srfsLog(LOG_FINE, "fa couldn't find writable file in wft %s", path);
                // We will look in the kv store for this case
			}
		}
        if (result != 0) {
            result = ar_get_attr_stat(ar, (char *)path, stbuf);
            srfsLog(LOG_FINE, "result %d %s %d", result, __FILE__, __LINE__);
        }
		srfsLog(LOG_FINE, "stbuf->st_mode %o stbuf->st_nlink %d result %d", stbuf->st_mode, stbuf->st_nlink, result);
        //stat_display(stbuf, stderr);
		return -result;
	}
}

static int ensure_not_writable(char *path1, char *path2) {
    int fileOpen;
    int attemptIndex;
    
    attemptIndex = 0;    
    fileOpen = TRUE;
    do {
        if (wft_contains(wft, path1) || (path2 != NULL && wft_contains(wft, path2))) {
            if (attemptIndex + 1 < _SKFS_RENAME_MAX_ATTEMPTS) {
                srfsLog(LOG_INFO, "ensure_not_writable failed. Sleeping for retry.");
                usleep(_SKFS_RENAME_RETRY_DELAY_MS * 1000);
                ++attemptIndex;
            } else {
                srfsLog(LOG_ERROR, "ensure_not_writable failed/timed out.");
                return FALSE;
            }
        } else {
            fileOpen = FALSE;
        }
    } while (fileOpen);
    return TRUE;
}

static int modify_file_attr(const char *path, char *fnName, mode_t *mode, uid_t *uid, gid_t *gid) {
    struct timespec last_access_tp;
    struct timespec *p_last_modification_tp;
    struct timespec last_change_tp;
    time_t  curEpochTimeSeconds;
    long    curTimeNanos;
    
    // get time outside of the critical section
    if (clock_gettime(CLOCK_REALTIME, &last_access_tp)) {
        fatalError("clock_gettime failed", __FILE__, __LINE__);
    }
    // modification time not updated
    p_last_modification_tp = NULL;
    last_change_tp = last_access_tp;
    return _modify_file_attr(path, fnName, mode, uid, gid, &last_access_tp, p_last_modification_tp, &last_change_tp);
}

static int _modify_file_attr(const char *path, char *fnName, mode_t *mode, uid_t *uid, gid_t *gid, 
                            const struct timespec *last_access_tp, const struct timespec *last_modification_tp, const struct timespec *last_change_tp) {
	if (!is_writable_path(path)) {
		return -EIO;
    } else {    
        WritableFileReference    *wf_ref;
        
        // FUTURE - best effort consistency; future, make rigorous
        //if (!ensure_not_writable((char *)path)) {
        //    srfsLog(LOG_ERROR, "Can't %s writable file", fnName);
        //    return -EIO;
        if (wft_contains(wft, path)) {
            // delay to make sure that there is a chance to release - update: removed this delay
            //usleep(5 * 1000);
            wf_ref = wft_get(wft, path);
         } else {
            wf_ref = NULL;
         }
        
        if (wf_ref != NULL) { // file is currently open; modify writable
            WritableFile    *wf;
            int rc;
            
            wf = wfr_get_wf(wf_ref);
            rc = wf_modify_attr(wf, mode, uid, gid, last_access_tp, last_modification_tp, last_change_tp);
            wfr_delete(&wf_ref, aw, fbwSKFS, ar->attrCache);
            return rc;
        } else {
            FileAttr	fa;
            int			result;
        
            memset(&fa, 0, sizeof(FileAttr));
            result = ar_get_attr(ar, (char *)path, &fa);
            if (result != 0) {
                return -ENOENT;
            } else {
                SKOperationState::SKOperationState  writeResult;
            
                if (last_access_tp != NULL) {
                    fa.stat.st_atime = last_access_tp->tv_sec;
                    fa.stat.st_atim.tv_nsec = last_access_tp->tv_nsec;
                }
                if (last_modification_tp != NULL) {
                    fa.stat.st_mtime = last_modification_tp->tv_sec;
                    fa.stat.st_mtim.tv_nsec = last_modification_tp->tv_nsec;
                }
                if (last_change_tp != NULL) {
                    fa.stat.st_ctime = last_change_tp->tv_sec;
                    fa.stat.st_ctim.tv_nsec = last_change_tp->tv_nsec;
                }
                if (mode != NULL) {
                    fa.stat.st_mode = *mode;
                }
                if (uid != NULL && *uid != (uid_t)-1) {
                    fa.stat.st_uid = *uid;
                }
                if (gid != NULL && *gid != (gid_t)-1) {
                    fa.stat.st_gid = *gid;
                }
                writeResult = aw_write_attr_direct(aw, path, &fa, ar->attrCache);
                if (writeResult != SKOperationState::SUCCEEDED) {
                    srfsLog(LOG_ERROR, "aw_write_attr_direct failed in %s %s", fnName, path);
                    return -EIO;
                } else {
                    return 0;
                }
            }
        }
    }
}

static int skfs_chmod(const char *path, mode_t mode) {
	srfsLogAsync(LOG_OPS, "_cm %s %x", path, mode);
    return modify_file_attr(path, "chmod", &mode, NULL, NULL);
}

static int skfs_chown(const char *path, uid_t uid, gid_t gid) {
	srfsLogAsync(LOG_OPS, "_co %s %d %d", path, uid, gid);
    return modify_file_attr(path, "chown", NULL, &uid, &gid);
}

static int skfs_truncate(const char *path, off_t size) {
    WritableFile            *wf;
    WritableFileReference   *wf_ref;
    int openedForTruncation;
    int rc;
    
	srfsLogAsync(LOG_OPS, "_t %s %lu", path, size);
    wf_ref = wft_get(wft, path);
	if (wf_ref != NULL) { // file is currently open; we're good
        openedForTruncation = FALSE;
    } else {              // file is not open; open it
        FileAttr	fa;
        int         statResult;
        
        openedForTruncation = TRUE;
        // Check if file exists
        memset(&fa, 0, sizeof(FileAttr));
        statResult = ar_get_attr(ar, (char *)path, &fa);
        if (statResult != 0) {
            return -statResult;
        } else {
            wf_ref = wft_create_new_file(wft, path, S_IFREG | 0666, &fa, pbr);
            if (wf_ref == NULL) {
                return -EIO;
            }
        }
    }    
    wf = wfr_get_wf(wf_ref);
    rc = wf_truncate(wf, size, fbwSKFS, pbr);
    wfr_delete(&wf_ref, aw, fbwSKFS, ar->attrCache);
    
    /*
    This is not needed with current approach as the reference deletion
    will remove the file from the table
    if (openedForTruncation) {
        WritableFileReference    *wf_tableRef;
        int frc;
    
        // If the file wasn't originally open, we need to close it
        // as we just opened it above
        wf_tableRef = wft_remove(wft, path);
        frc = -wf_flush(wf, aw, fbwSKFS, ar->attrCache);
		rc = -wf_close(wf, aw, fbwSKFS, ar->attrCache);
        if (rc == 0 && frc != 0) {
            rc = frc;
        }
        wfr_delete(&wf_tableRef);
    }
    */
    
    return rc;
}

static int skfs_utimens(const char *path, const struct timespec ts[2]) {
    struct timespec last_change_tp;
    
	srfsLogAsync(LOG_OPS, "_ut %s", path);
    // get time outside of the critical section
    if (clock_gettime(CLOCK_REALTIME, &last_change_tp)) {
        fatalError("clock_gettime failed", __FILE__, __LINE__);
    }
    return _modify_file_attr(path, "utimens", NULL, NULL, NULL, &ts[0], &ts[1], &last_change_tp);
}

static int skfs_access(const char *path, int mask) {
	int	result;
	struct stat stbuf;

	srfsLogAsync(LOG_OPS, "_a %s", path);
	memset(&stbuf, 0, sizeof(struct stat));
	result = ar_get_attr_stat(ar, (char *)path, &stbuf);
    if (result == 0 && mask != F_OK) {
        // for now we ignore the mask and just return success
        // if we could stat it
        // FUTURE - handle mask values
    }
	return -result;
}

static int skfs_readlink(const char *path, char *buf, size_t size) {
	char	linkPath[SRFS_MAX_PATH_LENGTH];
	int		numRead;
	int		fallbackToNFS;
	int		returnSize;

	srfsLogAsync(LOG_OPS, "_rl %s %d", path, size);

	fallbackToNFS = FALSE;
	srfsLog(LOG_FINE, "skfs_readlink %s %u", path, size);
	memset(linkPath, 0, SRFS_MAX_PATH_LENGTH);
	
	if (fsNativeOnlyPaths != NULL && pg_matches(fsNativeOnlyPaths, path)) {
		srfsLogAsync(LOG_OPS, "native readlink");
		fallbackToNFS = TRUE;
	} else {
		if (!ar_is_no_link_cache_path(ar, (char *)path)) {
			numRead = pbr_read(pbr, path, linkPath, SRFS_MAX_PATH_LENGTH, 0, NULL);

			if (numRead > 0 && numRead < SRFS_MAX_PATH_LENGTH) {
				char	appendBuf[SRFS_MAX_PATH_LENGTH + SRFS_MAX_PATH_LENGTH + 1];
				char	resultBuf[SRFS_MAX_PATH_LENGTH + SRFS_MAX_PATH_LENGTH + 1];
				char	*fullLinkPath;
				int		pathSimplifyResult;

				memset(appendBuf, 0, SRFS_MAX_PATH_LENGTH + SRFS_MAX_PATH_LENGTH + 1);
				memset(resultBuf, 0, SRFS_MAX_PATH_LENGTH + SRFS_MAX_PATH_LENGTH + 1);
				srfsLog(LOG_FINE, "link %s. linkPath %s", path, linkPath);
				if (linkPath[0] == '/') {
					srfsLog(LOG_FINE, "absolute symlink");
					fullLinkPath = linkPath;
				} else {
					char	pathBase[SRFS_MAX_PATH_LENGTH];
					char	*lastSlash;
					
					strcpy(pathBase, path);
					lastSlash = strrchr(pathBase, '/');
					if (lastSlash != NULL) {
						*lastSlash = '\0';
					}
					srfsLog(LOG_FINE, "relative symlink");
					sprintf(appendBuf, "%s/%s/%s", args->mountPath, pathBase, linkPath);
					fullLinkPath = appendBuf;
				}
				srfsLog(LOG_FINE, "fullLinkPath %s", fullLinkPath);
				pathSimplifyResult = path_simplify(fullLinkPath, resultBuf, SRFS_MAX_PATH_LENGTH + SRFS_MAX_PATH_LENGTH + 1);
				srfsLog(LOG_FINE, "pathSimplifyResult %d", pathSimplifyResult);
				if (pathSimplifyResult == FALSE) {
					fallbackToNFS = TRUE;
				} else {
					srfsLog(LOG_FINE, "simplified path %s", resultBuf);
					returnSize = strlen(resultBuf) + 1;
					if ((size_t)returnSize >= size) {
						returnSize = size - 1;
					}
					memcpy(buf, resultBuf, returnSize);
					buf[returnSize] = '\0';
					returnSize = 0;
				}
			} else {
				srfsLog(LOG_WARNING, "error reading link %s. falling back to nfs", path);
				fallbackToNFS = TRUE;
			}
		} else {
			fallbackToNFS = TRUE;
			srfsLog(LOG_WARNING, "no link cache path %s", path);
		}
	}

	if (fallbackToNFS) {
		char nfsPath[SRFS_MAX_PATH_LENGTH];
		//char readLinkPath[SRFS_MAX_PATH_LENGTH];

		memset(nfsPath, 0, SRFS_MAX_PATH_LENGTH);
		ar_translate_path(ar, nfsPath, path);
		srfsLog(LOG_WARNING, "skfs_readlink fallback nfsPath %s %u", nfsPath);
		returnSize = readlink(nfsPath, buf, size - 1);
		buf[returnSize] = '\0';

		//res = readlink(nfsPath, readLinkPath, size - 1);
		if (returnSize == -1) {
			return -errno;
		}
		//readLinkPath[res] = '\0';

		//translateReversePath(buf, readLinkPath);
	} else {
		srfsLog(LOG_FINE, "skfs_readlink no fallback");
	}
	//return returnSize;
	srfsLog(LOG_FINE, "skfs_readlink complete %s", path);
	return 0;
}


static int nfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                       off_t offset, struct fuse_file_info *fi) {
    DIR *dp;
    struct dirent *de;
    (void) offset;
    (void) fi;
    char nfsPath[SRFS_MAX_PATH_LENGTH];

	ar_translate_path(ar, nfsPath, path);
	srfsLog(LOG_FINE, "opendir %s\t%s", nfsPath, path);
    dp = opendir(nfsPath);
	if (dp == NULL) {
        return -errno;
	}

    while ((de = readdir(dp)) != NULL) {
        struct stat st;
        memset(&st, 0, sizeof(st));
        st.st_ino = de->d_ino;
        st.st_mode = de->d_type << 12;
		if (filler(buf, de->d_name, &st, 0)) {
            break;
		}
    }

    closedir(dp);
    return 0;
}

static int skfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                       off_t offset, struct fuse_file_info *fi) {
	srfsLogAsync(LOG_OPS, "_rd %s %ld", path, offset);
	if (!is_writable_path(path) && !is_base_path(path)) {
		if (path[1] != '\0') {
			return nfs_readdir(path, buf, filler, offset, fi);
		} else {
			return -ENOENT;
		}
	} else {
		return odt_readdir(odt, path, buf, filler, offset, fi);
	}
}

static int skfs_opendir(const char* path, struct fuse_file_info* fi) {
	srfsLogAsync(LOG_OPS, "_od %s", path);
	if (!is_writable_path(path) && !is_base_path(path)) {
		return 0;
	} else {
		return odt_opendir(odt, path, fi);
	}
}

static int skfs_releasedir(const char* path, struct fuse_file_info *fi) {
	srfsLogAsync(LOG_OPS, "_red %s", path);
	return odt_releasedir(odt, path, fi);
}

static int skfs_mkdir(const char *path, mode_t mode) {
	srfsLogAsync(LOG_OPS, "_mkd %s", path);
	return odt_mkdir(odt, (char *)path, mode);
}

static int skfs_rmdir(const char *path) {
	srfsLogAsync(LOG_OPS, "_rmd %s", path);
	return odt_rmdir(odt, (char *)path);
}

// FUTURE - Need to check/enforce atomicity of operations,
//          particularly mixed metadata/data operations.
//          For now, we perform best-effort rejection of
//          overlapped operations.

static int skfs_rename(const char *oldpath, const char *newpath) {
	srfsLogAsync(LOG_OPS, "_rn %s %s", oldpath, newpath);
	if (!is_writable_path(oldpath) || !is_writable_path(newpath)) {
		return -EIO;
    } else {    
        struct timespec tp;
        FileAttr	fa;
        int			result;
        time_t  curEpochTimeSeconds;
        long    curTimeNanos;

        // get time outside of the critical section
        if (clock_gettime(CLOCK_REALTIME, &tp)) {
            fatalError("clock_gettime failed", __FILE__, __LINE__);
        }    
        curEpochTimeSeconds = tp.tv_sec;
        curTimeNanos = tp.tv_nsec;
        memset(&fa, 0, sizeof(FileAttr));
        result = ar_get_attr(ar, (char *)oldpath, &fa);
        if (result != 0) {
            return -ENOENT;
        } else {
            int rnRetries;
            
            /*
            // FUTURE - The below segment is a temporary workaround for the case where a zero-size attribute is somehow read
            // Unclear if this is an application error or a bug, but we work around either case.
            rnRetries = 0;
            while (fa.stat.st_size == 0 && rnRetries < _rn_retry_limit) {
                int         _result;
                FileAttr    _fa;
                
                usleep(_rn_retry_interval_ms * 1000);
                memset(&_fa, 0, sizeof(FileAttr));
                _result = ar_get_attr(ar, (char *)oldpath, &_fa, stat_mtime_micros(&fa.stat) + 1);
                if (_result == 0) {
                    memcpy(&fa, &_fa, sizeof(FileAttr));
                } else {
                    // We presume the zero-size is legit
                }
                srfsLog(LOG_WARNING, "Detected zero-size attr in _rn for %s. Retry %s. fa.stat.st_size %d", oldpath, _result == 0 ? "succeeded" : "failed", fa.stat.st_size);
                ++rnRetries;
            }
            */
            
            if (S_ISDIR(fa.stat.st_mode)) {
                std::vector<char *>  unlinkList;
                int             dirRenameResult;
                
                dirRenameResult = skfs_rename_directory((char *)oldpath, (char *)newpath, curEpochTimeSeconds, curTimeNanos, &unlinkList);
                if (dirRenameResult == 0) {
                    dirRenameResult = skfs_unlink_list(&unlinkList);
                }
                return dirRenameResult;
            } else {
                // FUTURE - below is best-effort. enforce
                if (!ensure_not_writable((char *)oldpath, (char *)newpath)) {
                    WritableFileReference   *wfr;

                    wfr = wft_get(wft, oldpath);
                    if (wfr != NULL) {
                        WritableFile    *wf;
                        
                        wf = wfr_get_wf(wfr);
                        wf_set_pending_rename(wf, newpath);
                        wfr_delete(&wfr, aw, fbwSKFS, ar->attrCache);
                        return 0;
                        //srfsLog(LOG_ERROR, "Can't rename writable file");
                        //return -EIO;
                    } else {
                        srfsLog(LOG_ERROR, "Unexpected NULL wfr");
                        return -EIO;
                    }
                } else {
                    SKOperationState::SKOperationState  writeResult;
                
                    fa.stat.st_ctime = curEpochTimeSeconds;
                    fa.stat.st_ctim.tv_nsec = curTimeNanos;
                    // Link the new file to the old file blocks
                    writeResult = aw_write_attr_direct(aw, newpath, &fa, ar->attrCache);
                    if (writeResult != SKOperationState::SUCCEEDED) {
                        srfsLog(LOG_ERROR, "aw_write_attr_direct failed in skfs_rename %s", newpath);
                        return -EIO;
                    } else {
                        // Create a directory entry for the new file
                        if (odt_add_entry_to_parent_dir(odt, (char *)newpath)) {
                            srfsLog(LOG_WARNING, "Couldn't create new entry in parent for %s", newpath);
                            return -EIO;
                        } else {
                            // Delete the old file
                            return _skfs_unlink(oldpath, FALSE);
                        }
                    }
                }
            }
        }
    }
}


/////////////////////////
// begin directory rename

// Directory renaming is accomplished by copying metadata, but leaving file blocks alone.
// This works in two phases: 1 - Copy file and directory metadata to the new locations; 2 - Unlink the old files and directories.

static int skfs_rename_directory(char *oldPath, char *newPath, time_t curEpochTimeSeconds, long curTimeNanos, std::vector<char *> *unlinkList) {
    DirData *dd;
    int     result;
    
    srfsLog(LOG_INFO, "skfs_rename_directory %s %s", oldPath, newPath);
    result = 0;
    dd = odt_get_DirData(odt, oldPath);
    if (dd == NULL) {
        srfsLog(LOG_WARNING, "odt_get_DirData failed %s %s %d", oldPath, __FILE__, __LINE__);
        result = -EIO;
    } else {
        int         rc;
        struct stat oldDirStat;
        
        rc = 0;        
        memset(&oldDirStat, 0, sizeof(struct stat));
        rc = ar_get_attr_stat(ar, oldPath, &oldDirStat);
        if (rc == 0) {
            // Create the new directory
            result = odt_mkdir(odt, (char *)newPath, oldDirStat.st_mode);
            if (result == 0) {
                uint32_t    index;
                
                for (index = 0; index < dd->numEntries && result == 0; index++) {
                    DirEntry    *de;
                    
                    de = dd_get_entry(dd, index);
                    if (de != NULL && !de_is_deleted(de)) {
                        struct stat st;
                        char    oldChildPath[SRFS_MAX_PATH_LENGTH];
                        char    newChildPath[SRFS_MAX_PATH_LENGTH];
                    
                        memset(oldChildPath, 0, SRFS_MAX_PATH_LENGTH);
                        memset(newChildPath, 0, SRFS_MAX_PATH_LENGTH);
                        sprintf(oldChildPath, "%s/%s", oldPath, de_get_name(de));
                        sprintf(newChildPath, "%s/%s", newPath, de_get_name(de));
                        memset(&st, 0, sizeof(struct stat));
                        rc = ar_get_attr_stat(ar, oldChildPath, &st);
                        if (rc != 0) {
                            srfsLog(LOG_WARNING, "ar_get_attr_stat failed %s %d %s %d", oldChildPath, rc, __FILE__, __LINE__);
                            result = -rc;
                        } else {
                            if (S_ISDIR(st.st_mode)) {
                                result = skfs_rename_directory(oldChildPath, newChildPath, curEpochTimeSeconds, curTimeNanos, unlinkList);
                            } else {
                                result = skfs_rename_file(oldChildPath, newChildPath, curEpochTimeSeconds, curTimeNanos, unlinkList);
                            }
                        }
                    }
                }
                dd_delete(&dd);
                
                if (result == 0) {
                    unlinkList->push_back(strdup(oldPath));
                }
            }
        } else {
            srfsLog(LOG_WARNING, "ar_get_attr_stat failed %s %d %s %d", oldPath, rc, __FILE__, __LINE__);
            result = -EIO;
        }
    }
    srfsLog(LOG_INFO, "out skfs_rename_directory %s %s %d", oldPath, newPath, result);
     return result;
}

static int skfs_unlink_list(std::vector<char *> *unlinkList) {
    int result;
    
    result = 0;
    for (unsigned i = 0; i < unlinkList->size(); i++) {
        char    *oldPath;
        
        oldPath = unlinkList->at(i);
        if (oldPath != NULL) {
            int _result;
            
            srfsLog(LOG_INFO, "unlinking for rename %s", oldPath);
            _result = _skfs_unlink(oldPath, FALSE);
            if (_result != 0) {
                srfsLog(LOG_WARNING, "_skfs_unlink failed %s %d %s %d", oldPath, result, __FILE__, __LINE__);
                result = _result;
            }
            mem_free((void**)&oldPath);
        }
    }
    return result;
}

static int skfs_rename_file(char *oldPath, char *newPath, time_t curEpochTimeSeconds, long curTimeNanos, std::vector<char *> *unlinkList) {
    int result;
    
    srfsLog(LOG_INFO, "skfs_rename_file %s %s", oldPath, newPath);
    // FUTURE - below is best-effort. enforce
    if (!ensure_not_writable((char *)oldPath, (char *)newPath)) {
        WritableFileReference   *wfr;

        wfr = wft_get(wft, oldPath);
        if (wfr != NULL) {
            WritableFile    *wf;
            
            wf = wfr_get_wf(wfr);
            wf_set_pending_rename(wf, newPath);
            wfr_delete(&wfr, aw, fbwSKFS, ar->attrCache);
            result = 0;
            //srfsLog(LOG_ERROR, "Can't rename writable file");
            //return -EIO;
        } else {
            srfsLog(LOG_ERROR, "Unexpected NULL wfr %s %d", __FILE__, __LINE__);
            result = -EIO;
        }
    } else {
        SKOperationState::SKOperationState  writeResult;
        FileAttr	fa;

        memset(&fa, 0, sizeof(FileAttr));
        result = ar_get_attr(ar, (char *)oldPath, &fa);
        if (result == 0) {
            fa.stat.st_ctime = curEpochTimeSeconds;
            fa.stat.st_ctim.tv_nsec = curTimeNanos;
            // Link the new file to the old file blocks
            writeResult = aw_write_attr_direct(aw, newPath, &fa, ar->attrCache);
            if (writeResult != SKOperationState::SUCCEEDED) {
                srfsLog(LOG_ERROR, "aw_write_attr_direct failed in skfs_rename %s %s %d", newPath, __FILE__, __LINE__);
                result = -EIO;
            } else {
                // Create a directory entry for the new file
                if (odt_add_entry_to_parent_dir(odt, (char *)newPath)) {
                    srfsLog(LOG_WARNING, "Couldn't create new entry in parent for %s %s %d", newPath, __FILE__, __LINE__);
                    result = -EIO;
                } else {
                    unlinkList->push_back(strdup(oldPath));
                }
            }
        } else {
            srfsLog(LOG_WARNING, "ar_get_attr failed %s %d %s %d", oldPath, result, __FILE__, __LINE__);
        }
    }
    srfsLog(LOG_INFO, "out skfs_rename_file %s %s %d", oldPath, newPath, result);
    return result;
}

// end directory rename
///////////////////////



static int skfs_associate_new_file(const char *path, struct fuse_file_info *fi, WritableFileReference *wf_ref) {
	if (wf_ref == NULL) {
        srfsLog(LOG_FINE, "skfs_associate_new_file %s NULL ref", path);
        return -ENOENT;
	} else {
        SKFSOpenFile    *sof;
        
        sof = (SKFSOpenFile*)fi->fh;
        if (sof->type != OFT_WritableFile_Write) {
            fatalError("Unexpected sof->type != OFT_WritableFile_Write", __FILE__, __LINE__);
        }
		sof->wf_ref = wf_ref;
        if (srfsLogLevelMet(LOG_FINE)) {
            srfsLog(LOG_FINE, "skfs_associate_new_file %s %llx %llx", path, wf_ref, wfr_get_wf(wf_ref));
        }
		return 0;
	}
}

static int skfs_open(const char *path, struct fuse_file_info *fi) {
    SKFSOpenFile    *sof;
    
    // Below line is LOG_INFO as we don't want to log this for r/o files
	srfsLogAsync(LOG_INFO, "_O %s %x %s%s%s", path, fi->flags,
        ((fi->flags & OPEN_MODE_FLAG_MASK) == O_RDONLY ? "R" : ""),
        ((fi->flags & OPEN_MODE_FLAG_MASK) == O_WRONLY ? "W" : ""),
        ((fi->flags & OPEN_MODE_FLAG_MASK) == O_RDWR ? "RW" : ""),
        ((fi->flags & O_CREAT) ? "c" : ""),
        ((fi->flags & O_EXCL) ? "x" : ""),
        ((fi->flags & O_TRUNC) ? "t" : "")
        );
    
    sof = sof_new();
    fi->fh = (uint64_t)sof;
    
	if (!is_writable_path(path)) {        
        int nativeRelay;
        
        nativeRelay = FALSE;
        if (fsNativeOnlyPaths != NULL && pg_matches(fsNativeOnlyPaths, path)) {
            nativeRelay = TRUE;
        } else {
            int         result;
            FileAttr    *fa;
            
            fa = (FileAttr*)mem_alloc(1,sizeof(FileAttr));
            result = ar_get_attr(ar, (char *)path, fa);
            if (result != 0) {
                if (!is_writable_path(path)) {
                    srfsLog(LOG_WARNING, "Error reading %s result %d", path, result);
                    fatalError("readFromFile() failed", __FILE__, __LINE__);
                } else {
                    sof_delete(&sof);
                    return -ENOENT;
                }
            }
            nativeRelay = fa->stat.st_nlink == FA_NATIVE_LINK_MAGIC;
            if (!nativeRelay) {
                sof->attr = fa;
            } else {
                sof->attr = fa;
                //fa_delete(&fa);
            }
        }
        if (nativeRelay) {
            char nativePath[SRFS_MAX_PATH_LENGTH];
            
            sof->type = OFT_NativeRelay;
            ar_translate_path(ar, nativePath, path);
            srfsLog(LOG_FINE, "%s -> %s", path, nativePath);
            sof->fd = open(nativePath, O_RDONLY);
            if (sof->fd < 0) {
                sof_delete(&sof);
                return -errno;
            }
            sof->nativePath = strdup(nativePath);
        } else {
            sof->type = OFT_NativeStandard;
        }
        
        /*
        // eventually use this check, but stricter than prev code, so need to test
        if ((fi->flags & OPEN_MODE_FLAG_MASK) == O_RDONLY) {
            return 0;
        } else {
            return -EACCES;
        }
        */
        return 0;
    } else {
        if ((fi->flags & OPEN_MODE_FLAG_MASK) == O_RDONLY) {
            FileAttr	*fa;
            int         statResult;
            
            sof->wf_ref = (uint64_t)NULL;
            sof->type = OFT_WritableFile_Read;
            // for already open writable files, we ignore the ongoing write
            // and use what exists in the key-value store
            
            // Read attribute which should have already been created
            fa = (FileAttr*)mem_alloc(1, sizeof(FileAttr));
            statResult = ar_get_attr(ar, (char *)path, fa);
            if (!statResult) {
                sof->attr = fa;
                return 0;
            } else {
                // Can't read attribute which should exist => fail
                fa_delete(&fa);
                sof_delete(&sof);
                return -EIO;
            }
        } else if ((fi->flags & OPEN_MODE_FLAG_MASK) == O_WRONLY
                    || (fi->flags & OPEN_MODE_FLAG_MASK) == O_RDWR) {
            int	result;
            WritableFileReference    *existing_wf_ref;
            OpenDir *parentDir;
            uint64_t    parentDirUpdateTimeMillis;
            
            srfsLogAsync(LOG_OPS, "_o %s %x", path, fi->flags);
            sof->type = OFT_WritableFile_Write;
            // FIXME - do we need this in mknod also, or can we remove it there?
            parentDir = NULL;
            parentDirUpdateTimeMillis = curTimeMillis();
            if (odt_add_entry_to_parent_dir(odt, (char *)path, &parentDir)) {
                srfsLog(LOG_WARNING, "Couldn't create new entry in parent for %s", path);
            }
            existing_wf_ref = wft_get(wft, path);
            if (existing_wf_ref == NULL) {
                // File does not exist in the wft
                if ((fi->flags & O_TRUNC) != 0) {
                    WritableFileReference    *wf_ref;
                    
                    // O_TRUNC specified; use a new wf since we don't care about the old data blocks

                    if (((fi->flags & O_CREAT) != 0) && ((fi->flags & O_EXCL) != 0)) {
                        FileAttr	fa;
                        int statResult;

                        // Check if file exists...
                        memset(&fa, 0, sizeof(FileAttr));
                        statResult = ar_get_attr(ar, (char *)path, &fa);
                        if (!statResult) {
                            // Exclusive creation requested, but file already exists => fail
                            sof_delete(&sof);
                            return -EEXIST;
                        }
                    }
                    wf_ref = wft_create_new_file(wft, path, S_IFREG | 0666);
                    if (wf_ref != NULL) {
                        wf_set_parent_dir(wfr_get_wf(wf_ref), parentDir, parentDirUpdateTimeMillis);
                        result = skfs_associate_new_file(path, fi, wf_ref);
                    } else {
                        srfsLog(LOG_ERROR, "EIO from %s %d", __FILE__, __LINE__);
                        sof_delete(&sof);
                        return -EIO;
                    }
                } else {
                    FileAttr	fa;
                    int statResult;

                    // O_TRUNC *not* specified; we need to keep the old data blocks
                    
                    // Check to see if file exists...
                    memset(&fa, 0, sizeof(FileAttr));
                    statResult = ar_get_attr(ar, (char *)path, &fa);
                    if (statResult != 0 && statResult != ENOENT) {
                        // Error checking for existing attr => fail
                        srfsLog(LOG_ERROR, "statResult %d caused EIO from %s %d", statResult, __FILE__, __LINE__);
                        sof_delete(&sof);
                        return -EIO;
                    } else {
                        if (statResult != ENOENT) {
                            // File exists
                            if (((fi->flags & O_CREAT) != 0) && ((fi->flags & O_EXCL) != 0)) {
                                // Exclusive creation requested, but file already exists => fail
                                sof_delete(&sof);
                                return -EEXIST;
                            } else {
                                WritableFileReference    *wf_ref;
                                
                                wf_ref = wft_create_new_file(wft, path, S_IFREG | 0666, &fa, pbr);
                                if (wf_ref != NULL) {
                                    result = skfs_associate_new_file(path, fi, wf_ref);
                                } else {
                                    sof_delete(&sof);
                                    return -EIO;
                                }
                            }
                        } else {
                            WritableFileReference    *wf_ref;
                            
                            // File does not exist; create new file
                            wf_ref = wft_create_new_file(wft, path, S_IFREG | 0666);
                            if (wf_ref != NULL) {
                                result = skfs_associate_new_file(path, fi, wf_ref);
                            } else {
                                srfsLog(LOG_ERROR, "EIO from %s %d", __FILE__, __LINE__);
                                sof_delete(&sof);
                                return -EIO;
                            }
                        }
                    }
                }
            } else {
                // wf already exists in wft...(file is currently being written on this node)
                // in this clause, we must delete the existing reference if anything goes wrong
                if (((fi->flags & O_CREAT) != 0) && ((fi->flags & O_EXCL) != 0)) {
                    // Exclusive creation requested, but file already exists => fail
                    wfr_delete(&existing_wf_ref, aw, fbwSKFS, ar->attrCache);
                    sof->wf_ref = (uint64_t)NULL;
                    srfsLog(LOG_ERROR, "EEXIST from %s %d", __FILE__, __LINE__);
                    sof_delete(&sof);
                    return -EEXIST;
                }
                if ((fi->flags & O_TRUNC) != 0) {
                    // Can't open an already open file with O_TRUNC
                    wfr_delete(&existing_wf_ref, aw, fbwSKFS, ar->attrCache);
                    sof->wf_ref = (uint64_t)NULL;
                    srfsLog(LOG_ERROR, "EIO from %s %d", __FILE__, __LINE__);
                    sof_delete(&sof);
                    return -EIO;
                }
                result = skfs_associate_new_file(path, fi, existing_wf_ref);
            }
            return result;
        } else {
            srfsLog(LOG_ERROR, "EACCES from %s %d", __FILE__, __LINE__);
            sof_delete(&sof);
            return -EACCES;
        }
    }
}

static int skfs_mknod(const char *path, mode_t mode, dev_t rdev) {
    WritableFileReference    *wf_ref;
	
    srfsLogAsync(LOG_OPS, "_mn %s %x %x", path, mode, rdev);
	wf_ref = wft_create_new_file(wft, path, mode);
	if (wf_ref != NULL) {
        OpenDir *parentDir;
        uint64_t    parentDirUpdateTimeMillis;
        
        parentDir = NULL;
        parentDirUpdateTimeMillis = curTimeMillis();            
		if (odt_add_entry_to_parent_dir(odt, (char *)path, &parentDir)) {
			srfsLog(LOG_WARNING, "Couldn't create new entry in parent for %s", path);
		}
        wf_set_parent_dir(wfr_get_wf(wf_ref), parentDir, parentDirUpdateTimeMillis);
        wfr_delete(&wf_ref, aw, fbwSKFS, ar->attrCache);
		return 0;
	} else {
		return -EEXIST;
	}
}

static int _skfs_unlink(const char *path, int deleteBlocks) {
    // FUTURE - make parent removal and wft deletion locally atomic
    if (odt_rm_entry_from_parent_dir(odt, (char *)path)) {
        srfsLog(LOG_WARNING, "Couldn't rm entry in parent for %s", path);
        return -EIO;
    } else {
        return wft_delete_file(wft, path, deleteBlocks);
    }
}

static int skfs_unlink(const char *path) {
	srfsLogAsync(LOG_OPS, "_ul %s", path);
    // FUTURE - below is best-effort. enforce
    if (wft_contains(wft, path)) {
        srfsLog(LOG_ERROR, "Can't unlink writable file");
        return -EIO;
    } else {
        return _skfs_unlink(path, TRUE);
    }
}

/*
static void md5ErrorDebug(const char *path, SKVal *pRVal) {
    char fileName[SRFS_MAX_PATH_LENGTH];
    char modPath[SRFS_MAX_PATH_LENGTH];
    int	fd;
    int	i;
    int	modPathLength;

    strcpy(modPath, path);
    modPathLength = strlen(modPath);
    for (i = 0; i < modPathLength; i++) {
		if (modPath[i] == '/') {
			modPath[i] = '_';
		}
    }
    sprintf(fileName, "/tmp/%s.%d.%d", modPath, pRVal->m_len, clock());
    fd = open(fileName, O_CREAT | O_WRONLY);
    if (fd == -1) {
		printf("Unable to create md5 debug file\n");
    } else {
		int totalWritten;

		totalWritten = 0;
		while (totalWritten < pRVal->m_len) {
			int written;

			written = write(fd, &((char *)pRVal->m_pVal)[totalWritten], pRVal->m_len - totalWritten);	
			if (written == -1) {
				printf("Error writing md5 debug file\n");
				close(fd);
				return;
			}
			totalWritten += written;
		}
		close(fd);
    }
}
*/

static int skfs_read(const char *path, char *dest, size_t readSize, off_t readOffset,
                    struct fuse_file_info *fi) {
	int	totalRead;
    SKFSOpenFile    *sof;
	
	srfsLogAsync(LOG_OPS, "_r %x %s %d %ld", get_caller_pid(), path, readSize, readOffset);
    sof = (SKFSOpenFile*)fi->fh;
    if (!sof_is_valid(sof)) {
        srfsLog(LOG_WARNING, "invalid sof %llx", sof);
        return -EIO;
    } else {
        if (sof->type == OFT_NativeRelay && args->nativeFileMode != nf_blockReadOnly) {
            off_t    block;
            
            srfsLogAsync(LOG_INFO, "native read relay");
            srfsLog(LOG_FINE, "%s", path);
            block = readOffset / SRFS_BLOCK_SIZE;
            if (block >= sof->nextPrereadBlock) {
                if (args->nativeFileMode == nf_readRelay_localPreread) {
                    pbr_read_given_attr(pbr, path, NULL, readSize, readOffset, sof->attr, FALSE, _PREREAD_SIZE - 1, TRUE);
                } else {
                    br_request_remote_read(br, sof->nativePath, sof->attr, readSize, readOffset, _PREREAD_SIZE - 1);
                }
                sof->nextPrereadBlock = block + _PREREAD_SIZE; // Consider making threadsafe; a hint for now
            }
            totalRead = pread(sof->fd, dest, readSize, readOffset);
        } else {
            if (sof->type == OFT_WritableFile_Write) {
                // We currently do not support reading of files opened r/w
                return -EIO;
            } else {
                //srfsLogAsync(LOG_WARNING, "pbr read");
                totalRead = pbr_read(pbr, path, dest, readSize, readOffset, sof);
#if 0
                srfsLog(LOG_WARNING, "\nFile read stats");
                ac_display_stats(ar->attrCache);
                fbc_display_stats(fbr->fileBlockCache);
#endif
            }
        }
        return totalRead;
    }
}

#if FUSE_VERSION >= 29
static int skfs_read_buf(const char *path, struct fuse_bufvec **bufp,
                         size_t readSize, off_t readOffset, struct fuse_file_info *fi) {
    size_t	size;
    char	*buf;
    int	rVal;
    size_t	totalRead;

	srfsLogAsync(LOG_OPS, "_R %x %s %d %ld", get_caller_pid(), path, readSize, readOffset);
    {
        struct fuse_bufvec *src;
        char    *dest;
        int     result;
        
        src = (struct fuse_bufvec *)mem_alloc_no_dbg(1, sizeof(struct fuse_bufvec));
        *src = FUSE_BUFVEC_INIT(readSize);
        src->buf[0].flags = (enum fuse_buf_flags)(FUSE_BUF_IS_FD | FUSE_BUF_FD_SEEK);
        src->buf[0].fd = fi->fh;
        src->buf[0].pos = readOffset;
        *bufp = src;
        return 0;
    }
                         
#if 0
    struct fuse_bufvec *src;
    char    *dest;
    int     result;
    
	srfsLogAsync(LOG_OPS, "_R %x %s %d %ld", get_caller_pid(), path, readSize, readOffset);
    // This is a proof-of-concept implementation
    // A real implementation will probably want to allow
    // native files to be passed back via fd/offset
    src = (struct fuse_bufvec *)mem_alloc_no_dbg(1, sizeof(struct fuse_bufvec));
    dest = (char *)mem_alloc_no_dbg(readSize, 1);
    result = skfs_read(path, dest, readSize, readOffset, fi);
    if (result < 0) {
        return result;
    } else {
        *src = FUSE_BUFVEC_INIT(readSize);
        /*
        src->buf[0].flags = FUSE_BUF_IS_FD | FUSE_BUF_FD_SEEK;
        src->buf[0].fd = fi->fh;
        src->buf[0].pos = offset;
        */
        //src->buf[0].flags = 0;
        src->buf[0].mem = dest;
        *bufp = src;
        //srfsLog(LOG_WARNING, "_R returning 0");
        return 0;
    }
#endif    
}
#endif

int skfs_write(const char *path, const char *src, size_t writeSize, off_t writeOffset, 
          struct fuse_file_info *fi) {
    WritableFile *wf;
    size_t  totalBytesWritten;
    
	srfsLogAsync(LOG_OPS, "_w %s %d %ld", path, writeSize, writeOffset);
	wf = wf_fuse_fi_fh_to_wf(fi);
	if (wf == NULL) {
		srfsLog(LOG_ERROR, "skfs_write. No valid fi->fh found for %s", path);			
		return -EIO;
	} else {
		return wf_write(wf, src, writeSize, writeOffset, fbwSKFS, pbr, fbr->fileBlockCache);
	}
}

int skfs_release(const char *path, struct fuse_file_info *fi) {
    WritableFileReference *wf_ref;
    SKFSOpenFile    *sof;
    
    sof = (SKFSOpenFile*)fi->fh;
    fi->fh = NULL;
    if (!sof_is_valid(sof)) {
        wf_ref = NULL;
    } else {
        switch (sof->type)  {
            case OFT_WritableFile_Write:
                wf_ref = sof->wf_ref;
                break;
            case OFT_WritableFile_Read:
                wf_ref = NULL;
                break;
            case OFT_NativeRelay:
                FileAttr    fa;

                // Undo FA_NATIVE_LINK_MAGIC for this attribute
                ar_get_attr(ar, (char *)path, &fa, stat_mtime_micros(&sof->attr->stat) + 1);
                close(sof->fd);
                sof->fd = 0;
                wf_ref = NULL;
                break;
            case OFT_NativeStandard:
                wf_ref = NULL;
                break;
            default:
                fatalError("Invalid SKFSOpenFile type", __FILE__, __LINE__);
        }
        sof_delete(&sof);
    }
    
	if (wf_ref == NULL) {
		srfsLog(LOG_FINE, "skfs_release. Ignoring. No valid wf_ref found for %s", path);
		return 0;
	} else {
        WritableFile    *wf;
        
		srfsLogAsync(LOG_OPS, "_re %s %lx", path, fi);	
        wfr_sanity_check(wf_ref);
        wf = wfr_get_wf(wf_ref);
        if (wf->pendingRename != NULL) {
            int     rc;
            char    _old[SRFS_MAX_PATH_LENGTH];
            char    _new[SRFS_MAX_PATH_LENGTH];
            
            strncpy(_old, wf->path, SRFS_MAX_PATH_LENGTH);
            strncpy(_new, wf->pendingRename, SRFS_MAX_PATH_LENGTH);
            rc = wfr_delete(&wf_ref, aw, fbwSKFS, ar->attrCache);
            srfsLog(LOG_WARNING, "release found pending rename %s --> %s", _old, _new);
            skfs_rename(_old, _new);
            return rc;
        } else {
            return wfr_delete(&wf_ref, aw, fbwSKFS, ar->attrCache);
        }        
	}
}

int skfs_flush(const char *path, struct fuse_file_info *fi) {
    WritableFile *wf;
	
	wf = wf_fuse_fi_fh_to_wf(fi);
	if (wf == NULL) {
		srfsLog(LOG_FINE, "skfs_release. Ignoring. No valid fi->fh found for %s", path);			
		return 0;
	} else {
		srfsLogAsync(LOG_OPS, "_f %s", path);
		return -wf_flush(wf, aw, fbwSKFS, ar->attrCache);
	}
    return 0;
}

static void init_util_sk() {
    SKNamespace	*systemNS;
    SKNamespacePerspectiveOptions *nspOptions;
    
    pUtilSession = sd_new_session(sd);
    systemNS = pUtilSession->getNamespace(SK_SYSTEM_NS);
    nspOptions = systemNS->getDefaultNSPOptions();
    systemNSP = systemNS->openSyncPerspective(nspOptions);
    delete systemNS;
}

static uint64_t _get_sk_system_uint64(const char *stat) {
    SKVal       *pval;
    uint64_t    bytes;

    pval = systemNSP->get(stat);
    if (pval != NULL) {
        if (pval->m_pVal != NULL && pval->m_len > 0 && pval->m_len < _MAX_REASONABLE_DISK_STAT_LENGTH){
            bytes = strtoull((const char *)pval->m_pVal, NULL, 10);
        } else {
            bytes = 0;
        }
        sk_destroy_val(&pval);
    } else {
        bytes = 0;
    }
    return bytes;
}

static int skfs_statfs(const char *path, struct statvfs *s) {
    uint64_t    totalBytes;
    uint64_t    freeBytes;

	srfsLogAsync(LOG_OPS, "_s %s", path);
    
    totalBytes = _get_sk_system_uint64("totalDiskBytes");
    freeBytes = _get_sk_system_uint64("freeDiskBytes");
    if (totalBytes != 0 && freeBytes != 0) {
        memset(s, 0, sizeof(struct statvfs));
        
        s->f_bsize = SRFS_BLOCK_SIZE;
        
        s->f_blocks = totalBytes / SRFS_BLOCK_SIZE;
        s->f_bfree = freeBytes / SRFS_BLOCK_SIZE;
        s->f_bavail = freeBytes / SRFS_BLOCK_SIZE;
        
        s->f_files = 0;
        s->f_ffree = 0;
        s->f_favail = 0;
        
        s->f_fsid = 0;
        s->f_flag = 0;
        s->f_namemax = SRFS_MAX_PATH_LENGTH;
        return 0;
    } else {
        return -EIO;
    }
}

static int skfs_symlink(const char* to, const char* from) {
	srfsLogAsync(LOG_OPS, "_sl %s %s", to, from);
	if (!is_writable_path(from)) {
		return -EIO;
	} else {
		if (odt_add_entry_to_parent_dir(odt, (char *)from)) {
			srfsLog(LOG_WARNING, "Couldn't create new entry in parent for %s", from);
			return -EIO;
		} else {
            WritableFileReference *wf_ref;
            
            wf_ref = wft_create_new_file(wft, from, S_IFLNK | 0666);
            if (wf_ref == NULL) {
                return -EIO;
            } else {
                WritableFile *wf;
                int          writeResult;
                int          wfrDeletionResult;
                
                wf = wfr_get_wf(wf_ref);
                writeResult = wf_write(wf, to, strlen(to) + 1, 0, fbwSKFS, pbr, fbr->fileBlockCache);
                srfsLog(LOG_FINE, "writeResult %d", writeResult);
                wfrDeletionResult = wfr_delete(&wf_ref, aw, fbwSKFS, ar->attrCache);
                srfsLog(LOG_FINE, "wfrDeletionResult %d", wfrDeletionResult);
                if (writeResult != -1) {
                    return 0;
                } else {
                    return -1;
                }
            }
            /*
			wf = wf_new(from, S_IFLNK | 0666, aw);
			writeResult = wf_write(wf, to, strlen(to) + 1, 0, fbwSKFS, pbr, fbr->fileBlockCache);
			if (writeResult > 0) {
				int	flushResult;
				int	closeResult;
				
                flushResult = wf_flush(wf, aw, fbwSKFS, ar->attrCache);
                if (flushResult != 0) {
                    return -flushResult;
                } else {
                    closeResult = wf_close(wf, aw, fbwSKFS, ar->attrCache); // close also deletes wf
                    return -closeResult;
                }
			} else {
				wf_delete(&wf);
				return writeResult;
			}
            */
		}
	}
}

#ifdef HAVE_SETXATTR
/* xattr operations are optional and can safely be left unimplemented */
/*
static int skfs_getxattr(const char *path, const char *name, char *value,
                    size_t size) {
    int res = lgetxattr(path, name, value, size);
    if (res == -1)
        return -errno;
    return res;
}

static int skfs_listxattr(const char *path, char *list, size_t size) {
    int res = llistxattr(path, list, size);
    if (res == -1)
        return -errno;
    return res;
}
*/
#endif /* HAVE_SETXATTR */

static void read_fs_native_only_paths() {
	if (fsNativeOnlyFile != NULL) {
		char    *pgDef;	
        size_t  length;
		
		srfsLog(LOG_WARNING, "Reading fsNativeOnlyFile %s", fsNativeOnlyFile);
        length = 0;
		pgDef = file_read(fsNativeOnlyFile, &length);
		if (pgDef != NULL) {
			PathGroup	*newPG;
			
			newPG = (PathGroup *)pg_new("fsNativeOnlyPaths");
            trim_in_place(pgDef, length);
			pg_parse_paths(newPG, pgDef);
			fsNativeOnlyPaths = newPG;
			// NOTE - we intentionally leak the old pg as the size of
			// the lost memory is not worth the reference counting
			mem_free((void **)&pgDef, __FILE__, __LINE__);
			srfsLog(LOG_WARNING, "Done reading fsNativeOnlyFile %s", fsNativeOnlyFile);
		} else {
			srfsLog(LOG_ERROR, "Unable to read fsNativeOnlyFile %s", fsNativeOnlyFile);
		}		
	}
}

static void signal_handler(int signal) {
	srfsLog(LOG_WARNING, "Received signal %d", signal);
	//read_fs_native_only_paths();
}


static void segv_handler(int, siginfo_t*, void*) {
	print_stacktrace("segv_handler");
    exit(1);
}

static void abrt_handler(int, siginfo_t*, void*) {
	print_stacktrace("abrt_handler");
    exit(1);
}

static void term_handler() {
	print_stacktrace("terminate_handler");
}

static void unexpct_handler() {
	print_stacktrace("unexpected_handler");
    //skfs_destroy(NULL);
    //exit(1);
}

static int install_handler() {
	struct sigaction sigact;
	sigact.sa_flags = SA_SIGINFO | SA_ONSTACK;
/*
	sigact.sa_sigaction = segv_handler;
	if (sigaction(SIGSEGV, &sigact, (struct sigaction *)NULL) != 0) {
		fprintf(stderr, "error setting signal handler for %d (%s)\n",
		SIGSEGV, strsignal(SIGSEGV));
	}

    std::set_terminate(term_handler);
*/
/*
	sigact.sa_sigaction = abrt_handler;
	if (sigaction(SIGABRT, &sigact, (struct sigaction *)NULL) != 0) {
		fprintf(stderr, "error setting signal handler for %d (%s)\n",
		SIGABRT, strsignal(SIGABRT));
	}
*/
    std::set_unexpected(unexpct_handler);
	return 0;
}

static void *skfs_init(struct fuse_conn_info *conn) {
	pthread_spin_init(destroyLock, 0);
	srfsLogInitAsync(); // Important - this must be initialized here and not in main() due to fuse process configuration
	srfsLogAsync(LOG_OPS, "LOG_OPS async check");
	srfsLog(LOG_WARNING, "skfs_init()");
    install_handler();
	initDHT();
	fid_module_init();
	initReaders();
	initDirs();
    init_util_sk();
	wft = wft_new("WritableFileTable", aw, ar->attrCache, ar, fbwSKFS);
	initPaths();
	pthread_create(&statsThread, NULL, stats_thread, NULL);
	if(fsNativeOnlyFile) {
		//if nativeOnlyFile name is supplied, then create this thread 
		pthread_create(&nativeFileWatcherThread, NULL, nativefile_watcher_thread, NULL);
	}
	read_fs_native_only_paths();
	srfsRedirectStdio();
	srfsLog(LOG_WARNING, "skfs_init() complete");
	return NULL;
}

static void skfs_destroy(void* private_data) {
    int doDestroy;
    
	exitSignalReceived = TRUE;
    
    pthread_spin_lock(destroyLock);
    if (!destroyCalled) {
        destroyCalled = TRUE;
        doDestroy = TRUE;
    } else {
        doDestroy = FALSE;
    }
    pthread_spin_unlock(destroyLock);

    if (doDestroy) {
        srfsLog(LOG_WARNING, "skfs_destroy()");
        //srfsRedirectStdio(); //TODO: think about this
        /*
        srfsLog(LOG_WARNING, "skfs_destroy() waiting for threads");
        pthread_join(statsThread, NULL);
        if (fsNativeOnlyFile) {
            pthread_join(nativeFileWatcherThread, NULL);
        }
        srfsLog(LOG_WARNING, "skfs_destroy() done waiting for threads");
        */
        /*
        destroyPaths();
        below is crashing; for now we ignore this
        destroyReaders();
        destroyDHT();
        */
        srfsLog(LOG_WARNING, "skfs_destroy() complete");
    }
}

static struct fuse_operations skfs_oper = {
	/* getattr */ NULL,
	/* readlink */ NULL,
	/* getdir */ NULL,
	/* mknod */ NULL,
	/* mkdir */ NULL,
	/* unlink */ NULL,
	/* rmdir */ NULL,
	/* symlink */ NULL,
	/* rename */ NULL,
	/* link */ NULL,
	/* chmod */ NULL,
	/* chown */ NULL,
	/* truncate */ NULL,
	/* utime */ NULL,
	/* open */ NULL, //skfs_open,
	/* read */ NULL, //skfs_read,
	/* write */ NULL,
	/* statfs */ NULL,
	/* flush */ NULL,
	/* release */ NULL,
	/* fsync */ NULL,
	/* setxattr */ NULL,
	/* getxattr */ NULL,
	/* listxattr */ NULL,
	/* removexattr */ NULL,
	/* opendir */ NULL,
	/* readdir */ NULL,
	/* releaseddir */ NULL,
	/* fsyncdir */ NULL,
	/* init */ NULL,    //skfs_init,
	/* destroy */ NULL, //skfs_destroy,
	/* access */ NULL,
	/* create */ NULL,
	/* ftruncate */ NULL,
	/* fgetattr */ NULL,
	/* lock */ NULL,
	/* utimens */ NULL,
	/* bmap */ NULL
#if  FUSE_USE_VERSION > 27
	, /*flag_nullpath_ok*/ 0
	, /*flag_reserved*/ 0
	, /*ioctl*/ NULL
	, /*poll*/ NULL
#endif	
};

static void *stats_thread(void *) {
	int	detailPeriod;
	int	detailPhase;
	
	detailPeriod = statsDetailIntervalSeconds / statsIntervalSeconds;
	detailPhase = 0;
	while (!exitSignalReceived) {
		int	detailFlag;
		
		detailFlag = detailPhase == 0;
		sleep(statsIntervalSeconds);
		srfsLog(LOG_WARNING, "\n\t** stats **");
		ar_display_stats(ar, detailFlag);
		fbr_display_stats(fbr, detailFlag);
		detailPhase = (detailPhase + 1) % detailPeriod;
	}
	return NULL;
}


static void * nativefile_watcher_thread(void * unused) {
  //passed params are unused currently 

  // do forever in loop:
  //  - if file not exist, keep checking until it appears
  //  - then, set native only paths
  //  - set watcher
  //  - process events accordingly

  int length, i = 0;
  char buffer[INOTIFY_BUF_LEN];

  while ( !exitSignalReceived ){
	  int fd = -1;
	  int wd = -1;
	  // if cannot open file, then sleep and re-try.
	  FILE* fh = NULL;
	  while( (fh = fopen(fsNativeOnlyFile, "r")) == NULL  ) {
		srfsLog(LOG_WARNING, "cannot find or open native only file %s, sleeping \n", fsNativeOnlyFile );
		sleep( 20 );
	  }
	  fclose(fh); fh = NULL; //close handle opened above

	  read_fs_native_only_paths();

	  fd = inotify_init();

	  if ( fd < 0 ) {
		perror( "inotify_init" );
	  }
  
	  srfsLog(LOG_INFO, "inotify_add_watch %d \n", fd );

	  wd = inotify_add_watch( fd, fsNativeOnlyFile, IN_MODIFY | IN_CREATE | IN_MOVE_SELF | IN_DELETE_SELF );
	  if ( wd < 0 ) {
		perror( "inotify_add_watch" );
	  }

	  bool oldFileIsValid = 1;  //indicates validity/obsoletion of the watched file 
	  while  ( oldFileIsValid ) {  // READ loop
		i = 0;
		//srfsLog(LOG_FINE, "calling read %d %d \n", wd, fd  );
		length = read( fd, buffer, INOTIFY_BUF_LEN );  

		if ( length < 0 ) {
		  perror( "read" );
		}
  
		//srfsLog(LOG_FINE, "processing read ... %d\n", length );
		while ( i < length ) {  // EVENT loop
		  struct inotify_event *event = ( struct inotify_event * ) &buffer[ i ];
		  srfsLog(LOG_FINE, "event  %d : %d : %s\n", event->len, event->mask, event->name );
		  //if ( event->len ) {
			if ( event->mask & IN_CREATE ) {
				srfsLog(LOG_INFO, "The file %s was created.\n", event->name );
				read_fs_native_only_paths();
			}
			else if ( event->mask & IN_DELETE_SELF ) {
				srfsLog(LOG_INFO, "The file %s was deleted.\n", event->name );
				oldFileIsValid = 0;
				//break;  //file's gone 
			}
			else if (  event->mask & IN_MOVE_SELF ) {
				srfsLog(LOG_INFO, "The file %s was moved.\n", event->name );
				oldFileIsValid = 0;
				//break;
			}
			else if ( event->mask & IN_MODIFY ) {
				srfsLog(LOG_INFO, "IN_MODIFY.\n", event->name );
				read_fs_native_only_paths();
			}
		  //}
		  i += EVENT_SIZE + event->len;
		}  // end of EVENT loop

	  } // end of READ loop

	  if(wd >= 0)
		( void ) inotify_rm_watch( fd, wd );
	  if(fd >= 0)
		( void ) close( fd );

  }  // end of main loop
  return NULL;
}

void initFuse() {
    skfs_oper.getattr = skfs_getattr;
    skfs_oper.read = skfs_read;
#if FUSE_VERSION >= 29
//    skfs_oper.read_buf = skfs_read_buf;
#endif
    skfs_oper.readlink = skfs_readlink;
    skfs_oper.readdir = skfs_readdir;
    skfs_oper.flush = skfs_flush;
	skfs_oper.init = skfs_init;
    skfs_oper.access = skfs_access;
	skfs_oper.destroy = skfs_destroy;
	
    skfs_oper.open = skfs_open;
    skfs_oper.release = skfs_release;
    skfs_oper.mknod = skfs_mknod;
    skfs_oper.write = skfs_write;
	skfs_oper.chmod = skfs_chmod;
	skfs_oper.chown = skfs_chown;
	skfs_oper.truncate = skfs_truncate;
	skfs_oper.utimens = skfs_utimens;
	skfs_oper.releasedir = skfs_releasedir;
    skfs_oper.opendir = skfs_opendir;
    skfs_oper.symlink = skfs_symlink;
	
	skfs_oper.mkdir = skfs_mkdir;

	skfs_oper.rmdir = skfs_rmdir;
	skfs_oper.unlink = skfs_unlink;	
    
    skfs_oper.rename = skfs_rename;
    
    skfs_oper.statfs = skfs_statfs;
}

void initDHT() {
    try {
        defaultChecksum    = args->checksum;
        defaultCompression = args->compression;

        LoggingLevel lvl = LVL_WARNING;
        if (!strcmp(args->logLevel, "FINE")) {
            lvl = LVL_ALL;
        } else if (!strcmp(args->logLevel, "INFO")) {
            lvl = LVL_INFO;
        } else if (!strcmp(args->logLevel, "OPS")) {
            lvl = LVL_WARNING;
        //} else {
        //	lvl = LVL_INFO;
        }

        pClient = SKClient::getClient(lvl, args->jvmOptions);
        if (!pClient) {
            srfsLog(LOG_WARNING, "dht client init failed. Will continue with NFS-only");
        }
        
        SKValueCreator  *vc;
        
        vc = pClient->getValueCreator();
        if (vc != NULL) {
            myValueCreator = getValueCreatorAsUint64(vc);
            delete vc;
        } else {
            fatalError("NULL pClient->getValueCreator()", __FILE__, __LINE__);
        }
        srfsLog(LOG_WARNING, "myValueCreator %x", myValueCreator);

        SKGridConfiguration * pGC = SKGridConfiguration::parseFile(args->gcname);
        SKClientDHTConfiguration * pCdc = pGC->getClientDHTConfiguration();
        sessOption = new SKSessionOptions(pCdc, args->host) ;
        pGlobalSession = pClient->openSession(sessOption);
        if (!pGlobalSession ) {
            srfsLog(LOG_WARNING, "dht session init failed. Will continue with NFS-only\n");
        }

        delete pGC;
        delete pCdc;
        srfsLog(LOG_FINE, "out initDHT");
    } catch(std::exception& e) { 
        srfsLog(LOG_WARNING, "initDHT() exception: %s", e.what()); 
    }
}

void destroyDHT() {
	delete pGlobalSession; pGlobalSession = NULL;
	delete pClient; pClient = NULL;
    delete sessOption; sessOption = NULL;
	srfsLog(LOG_WARNING, "SKClient shutdown");
	SKClient::shutdown();
}

// FUTURE: probably remove task output support
PathGroup *initTaskOutputPaths(int* taskOutputPort ) {
	PathGroup	*taskOutputPaths;

	srfsLog(LOG_WARNING, "initTaskOutputPaths %s", (char *)args->taskOutputPaths);
	taskOutputPaths = pg_new("taskOutputPaths");
	pg_parse_paths(taskOutputPaths, (char *)args->taskOutputPaths);
	if (pg_size(taskOutputPaths) == 1) {
		char	*delimiter;
		char	*gcName;
		char	*path;

		path = pg_get_member(taskOutputPaths, 0);
		delimiter = strchr(pg_get_member(taskOutputPaths, 0), '#');
		if (delimiter != NULL) {
			*delimiter = '\0';
			gcName = delimiter + 1;
			srfsLog(LOG_WARNING, "Setting grid configuration %s", gcName);
			//*taskOutputPort = dht_set_grid_configuration(gcName, NULL, args->host);
			ar_store_dir_attribs(ar, path);
		}
	} else if (pg_size(taskOutputPaths) > 1) {
		srfsLog(LOG_WARNING, "Too many taskOutputPaths for first-cut implementation\n");
	}
	return taskOutputPaths;
}

void initPaths() {
	ar_parse_native_aliases(ar, (char *)args->nfsMapping);
	ar_parse_no_error_cache_paths(ar, (char *)args->noErrorCachePaths);
	ar_parse_no_link_cache_paths(ar, (char *)args->noLinkCachePaths);
	fsNativeOnlyFile = (char *)args->fsNativeOnlyFile;
	ar_parse_snapshot_only_paths(ar, (char *)args->snapshotOnlyPaths);
	fbr_parse_compressed_paths(fbr, (char *)args->compressedPaths);
	fbr_parse_no_fbw_paths(fbr, (char *)args->noFBWPaths);
	fbr_parse_permanent_suffixes(fbr, (char *)args->permanentSuffixes);
	//Namespaces for SKFS must be created by external scripts ; 
    ar_create_alias_dirs(ar, odt);
}

void destroyPaths() {
}

FileBlockCache *createFileBlockCache(int numSubCaches, int transientCacheSize, FileIDToPathMap *f2p) {
    int evictionBatch;
    
	if (transientCacheSize == 0) {
		transientCacheSize = FBR_TRANSIENT_CACHE_SIZE;
	}
    evictionBatch = int_max(int_min(FBR_TRANSIENT_CACHE_EVICTION_BATCH, transientCacheSize / numSubCaches), 1);
    
	return fbc_new(_FBC_NAME, transientCacheSize, evictionBatch, f2p, numSubCaches);
}

void initReaders() {
	uint64_t	transientCacheSizeBlocks;
    FileBlockCache  *fbc;

	sd = sd_new((char *)args->host, (char *)args->gcname, NULL, args->compression, 
				args->dhtOpMinTimeoutMS, args->dhtOpMaxTimeoutMS, 
				SRFS_DHT_OP_DEV_WEIGHT, SRFS_DHT_OP_DHT_WEIGHT);

	f2p = f2p_new();
	aw = aw_new(sd);
	awSKFS = aw_new(sd);    
    
	transientCacheSizeBlocks = (uint64_t)args->transientCacheSizeKB * (uint64_t)1024 / (uint64_t)SRFS_BLOCK_SIZE;
    fbc = createFileBlockCache(args->cacheConcurrency, transientCacheSizeBlocks, f2p);
	fbwCompress = fbw_new(sd, TRUE, fbc, args->fbwReliableQueue);
	fbwRaw = fbw_new(sd, FALSE, fbc, args->fbwReliableQueue);
	fbwSKFS = fbw_new(sd, TRUE, fbc, args->fbwReliableQueue);
	rtsAR_DHT = rts_new(SRFS_RTS_DHT_ALPHA, SRFS_RTS_DHT_OP_TIME_INITIALIZER);
	rtsAR_NFS = rts_new(SRFS_RTS_NFS_ALPHA, SRFS_RTS_NFS_OP_TIME_INITIALIZER);
	rtsFBR_DHT = rts_new(SRFS_RTS_DHT_ALPHA, SRFS_RTS_DHT_OP_TIME_INITIALIZER);
	rtsFBR_NFS = rts_new(SRFS_RTS_NFS_ALPHA, SRFS_RTS_NFS_OP_TIME_INITIALIZER);
	rtsODT = rts_new(SRFS_RTS_DHT_ALPHA, SRFS_RTS_DHT_OP_TIME_INITIALIZER);
	ar = ar_new(f2p, sd, aw, rtsAR_DHT, rtsAR_NFS, args->cacheConcurrency, args->attrTimeoutSecs * 1000);
	ar_store_dir_attribs(ar, SKFS_BASE, 0555);
	ar_store_dir_attribs(ar, SKFS_WRITE_BASE, 0777);
	int taskOutputPort = -1;
	//PathGroup * pg = initTaskOutputPaths(&taskOutputPort);
	//srfsLog(LOG_WARNING, "taskOutputPort %d", taskOutputPort);
	ar_set_g2tor(ar, NULL);
	fbr = fbr_new(f2p, fbwCompress, fbwRaw, sd, rtsFBR_DHT, rtsFBR_NFS, fbc);
	pbr = pbr_new(ar, fbr, NULL);

    if (args->nativeFileMode == nf_readRelay_distributedPreread) {
        int _port;
            
        if (args->brPort >= 0) {
            _port = args->brPort;
        } else {
            _port = BR_DEFAULT_PORT;
        }
        br = br_new(_port, pbr, ar);
        if (args->brRemoteAddressFile != NULL) {
            br_read_addresses(br, args->brRemoteAddressFile);
        }
    } else {
        br = NULL;
    }
#if 0
    wft = wft_new("WritableFileTable");
#endif
}

// Initialization writable directory structure
void initDirs() {
	rcst_init();
	odt = odt_new(ODT_NAME, sd, aw, ar, rtsODT, args->reconciliationSleep, args->odwMinWriteIntervalMillis);
	if (odt_mkdir_base(odt)) {
		fatalError("odt_mkdir_base skfs failed", __FILE__, __LINE__);
	}
	if (odt_opendir(odt, SKFS_BASE, NULL) != 0) {
		fatalError("odt_openDir skfs failed", __FILE__, __LINE__);
	}
	if (odt_opendir(odt, SKFS_WRITE_BASE, NULL) != 0) {
		fatalError("odt_openDir skfs failed", __FILE__, __LINE__);
	}    
    if (odt_add_entry(odt, SKFS_BASE, SKFS_WRITE_BASE_NO_SLASH)) {
        srfsLog(LOG_WARNING, "Couldn't create new entry in parent for %s", SKFS_WRITE_BASE_NO_SLASH);
        fatalError("Couldn't create dir", __FILE__, __LINE__);
    }
    wf_set_sync_dir_updates(args->syncDirUpdates);
}

void destroyReaders(){
	//nft_delete(&nft);
	pbr_delete(&pbr);
	fbr_delete(&fbr);
	ar_delete(&ar);
	odt_delete(&odt);
	rts_delete(&rtsFBR_NFS);
	rts_delete(&rtsFBR_DHT);
	rts_delete(&rtsAR_NFS);
	rts_delete(&rtsAR_DHT);
	fbw_delete(&fbwRaw);
	fbw_delete(&fbwCompress);
	fbw_delete(&fbwSKFS);
	aw_delete(&aw);
	aw_delete(&awSKFS);
	f2p_delete(&f2p);
	sd_delete(&sd);
}

static void initLogging() {
    char	logPath[SRFS_MAX_PATH_LENGTH];
    char	*lastSlash;
    
    strcpy(logPath, args->mountPath);
    lastSlash = strrchr(logPath, '/');
    if (lastSlash != NULL) {
        *lastSlash = '\0';
    }
    
	sprintf(logFileName, "%s/logs/fuse.log.%d", logPath, getpid());
	srfsLogSetFile(logFileName);
	srfsLog(LOG_WARNING, "skfs block size %d", SRFS_BLOCK_SIZE);
	if (args->logLevel != NULL) {
		if (!strcmp(args->logLevel, "FINE")) {
			setSRFSLogLevel(LOG_FINE);
		} else if (!strcmp(args->logLevel, "OPS")) {
			setSRFSLogLevel(LOG_OPS);
		} else if (!strcmp(args->logLevel, "INFO")) {
			setSRFSLogLevel(LOG_INFO);
		} else {
			setSRFSLogLevel(LOG_WARNING);
		}
	} else {
		setSRFSLogLevel(LOG_WARNING);
	}
	srfsLog(LOG_FINE, "LOG_FINE check");
	srfsLog(LOG_OPS, "LOG_OPS check");
	srfsLog(LOG_WARNING, "LOG_WARNING check");
}

bool addEnvToFuseArg(const char * envVarName, const char * fuseOpt, const char *defaultFuseOpt) {
	char * pEnd = NULL;
	long envLong = 0;
	const char * pEnvStr = getenv(envVarName);
	if(pEnvStr)
		envLong = strtol(pEnvStr, &pEnd, 10);
	std::string entryStr = ( !pEnvStr || envLong < 1 || envLong == LONG_MAX ) ?
		defaultFuseOpt : std::string(fuseOpt) + std::string(pEnvStr, pEnd - pEnvStr);
	fuse_opt_add_arg(&fuseArgs, entryStr.c_str());
	
	srfsLog( LOG_WARNING, envVarName );
	srfsLog( LOG_WARNING, entryStr.c_str() );
	return TRUE;
}

void parseArgs(int argc, char *argv[], CmdArgs *arguments) {
    argp_parse(&argp, argc, argv, 0, 0, arguments);
    
    if (arguments->entryTimeoutSecs < 0) {
        arguments->entryTimeoutSecs = SKFS_DEF_ENTRY_TIMEOUT_SECS;
    }
    if (arguments->attrTimeoutSecs < 0) {
        arguments->attrTimeoutSecs = SKFS_DEF_ATTR_TIMEOUT_SECS;
    }
    if (arguments->negativeTimeoutSecs < 0) {
        arguments->negativeTimeoutSecs = SKFS_DEF_NEGATIVE_TIMEOUT_SECS;
    }    
}

static void add_fuse_option(char *option) {
    int result;
    
    result = fuse_opt_add_arg(&fuseArgs, option);
    srfsLog(LOG_WARNING, "%s", option);
    fflush(0);
    if (result != 0) {
        srfsLog(LOG_WARNING, "%s failed: %d", option, result);
        fflush(0);
        fatalError("Adding option failed", __FILE__, __LINE__);
    }
}

#ifndef UNIT_TESTS
int main(int argc, char *argv[]) {        
    printf("skfsd %s %s\n", __DATE__, __TIME__);
    umask(022);
    pClient = NULL; pGlobalSession = NULL;

	initFuse();

    fuse_opt_add_arg(&fuseArgs, argv[0]);
    initDefaults(&_args);
    //argp_parse(&argp, argc, argv, 0, 0, &_args);
	parseArgs(argc, argv, &_args);
    displayArguments(&_args);
    //checkArguments(&_args);

	initLogging();

	if (RUNNING_ON_VALGRIND) {
		fprintf(stderr,
			"**** WARNING : Valgrind has been detected. If this fails to run in Valgrind\n"
			"**** then lookup procedures for valgrind-and-fuse-file-systems \n"
			"**** Sleeping for 5 seconds \n");
		sleep(5);
		fprintf(stderr, "\n");
	}
	if (args->verbose || RUNNING_ON_VALGRIND) {  //if either on Valgrind or Debug env var was specified
		add_fuse_option("-d");
	    srfsLog(LOG_WARNING, "Verbose on");
	} else {
	    srfsLog(LOG_WARNING, "Verbose off");
	}

	if (!access(FUSE_CONF_FILE, R_OK)) {
	    srfsLog(LOG_WARNING, "Found %s", FUSE_CONF_FILE);
	    add_fuse_option("-oallow_other");
	} else {
	    srfsLog(LOG_WARNING, "No %s", FUSE_CONF_FILE);
	}
	add_fuse_option("-odefault_permissions");
	
	if (args->enableBigWrites) {
        srfsLog(LOG_WARNING, "Enabling big writes");
		add_fuse_option("-obig_writes");
	}
    
    //add_fuse_option("-odirect_io"); // for debugging only    
    
	add_fuse_option("-ononempty");
    add_fuse_option("-ouse_ino");
    add_fuse_option("-oauto_cache");
    //add_fuse_option("-owriteback_cache");
    
	sprintf(fuseEntryOption, "-oentry_timeout=%d", args->entryTimeoutSecs);
	sprintf(fuseAttrOption, "-oattr_timeout=%d", args->attrTimeoutSecs);
	sprintf(fuseACAttrOption, "-oac_attr_timeout=%d", args->attrTimeoutSecs);
	sprintf(fuseNegativeOption, "-onegative_timeout=%d", args->negativeTimeoutSecs);
	
	addEnvToFuseArg("SKFS_ENTRY_TIMEOUT", "-oentry_timeout=", fuseEntryOption);
	addEnvToFuseArg("SKFS_ATTR_TIMEOUT", "-oattr_timeout=", fuseAttrOption);
	addEnvToFuseArg("SKFS_AC_ATTR_TIMEOUT", "-oac_attr_timeout=", fuseACAttrOption);
	addEnvToFuseArg("SKFS_NEGATIVE_TIMEOUT", "-onegative_timeout=", fuseNegativeOption);
	//addEnvToFuseArg("SKFS_MAX_READAHEAD", "-omax_readahead=", "-omax_readahead=1048576");
	addEnvToFuseArg("SKFS_MAX_READAHEAD", "-omax_readahead=", "-omax_readahead=5242880");
    
	//fuse_opt_add_arg(&fuseArgs, "-osubtype=SKFS");
	srfsLog(LOG_WARNING, "Calling fuse_main");
	int retCode = fuse_main(fuseArgs.argc, fuseArgs.argv, &skfs_oper, NULL);
	fprintf(stderr, "fuse_main() returned %d\n", retCode);
	//fuseArgs are not freed
	return retCode;
}
#endif  //UNIT_TESTS
