// ReaderStats.h

#ifndef _SKFS_H_
#define _SKFS_H_

/////////////
// includes
#include "skconstants.h"

//////////
// types

typedef enum NativeFileMode {nf_blockReadOnly, nf_readRelay_localPreread, nf_readRelay_distributedPreread} NativeFileMode;

typedef struct CmdArgs {
		const char *mountPath;
        int	    verbose;
        const char  *host;
        //int	    port;
        const char  *zkLoc;
        //const char  *dhtName;
        const char  *gcname;
        const char  *nfsMapping;
        const char  *permanentSuffixes;
        const char  *noErrorCachePaths;
        const char  *noLinkCachePaths;
        const char  *fsNativeOnlyFile;
        const char  *snapshotOnlyPaths;
        const char  *taskOutputPaths;
        const char  *compressedPaths;
        const char  *noFBWPaths;
		int		fbwReliableQueue;
		SKCompression::SKCompression compression;
		SKChecksumType::SKChecksumType checksum;
		int	transientCacheSizeKB;
		int	cacheConcurrency;
		const char	*logLevel;
		const char	*jvmOptions;
		int enableBigWrites;
        int entryTimeoutSecs;
        int attrTimeoutSecs;
        int negativeTimeoutSecs;
        uint64_t dhtOpMinTimeoutMS;
        uint64_t dhtOpMaxTimeoutMS;
        NativeFileMode  nativeFileMode;
        char *brRemoteAddressFile;
        int brPort;
        char *reconciliationSleep;
        uint64_t    odwMinWriteIntervalMillis;
        int syncDirUpdates;
} CmdArgs;

extern CmdArgs *args;

///////////////
// prototypes

void initDHT();
void destroyDHT();

void initDefaults(CmdArgs *arguments);
void parseArgs(int argc, char *argv[], CmdArgs *arguments);

#endif //_SKFS_H_
