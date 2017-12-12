#ifndef UTIL_H
#define UTIL_H

#include "skbasictypes.h"
#include "skcontainers.h"
#include "SKClient.h"
#include "SKSession.h"
#include "SKAsyncNSPerspective.h"
#include "SKSyncNSPerspective.h"
#include "SKStoredValue.h"
#include "SKValueCreator.h"
#include "SKGridConfiguration.h"
#include "SKClientDHTConfiguration.h"
#include "SKSessionOptions.h"
#include "SKNamespacePerspectiveOptions.h"
#include "SKPutOptions.h"
#include "SKGetOptions.h"
#include "SKWaitOptions.h"
#include "SKNamespaceOptions.h"
#include "SKAsyncPut.h"
#include "SKAsyncRetrieval.h"
#include "SKAsyncValueRetrieval.h"
#include "SKAsyncSingleValueRetrieval.h"
#include "SKAsyncSyncRequest.h"
#include "SKAsyncSnapshot.h"

#include "SKRetrievalException.h"
#include "SKPutException.h"
#include "SKSyncRequestException.h"
#include "SKSnapshotException.h"
#include "SKWaitForCompletionException.h"
#include "SKNamespaceCreationException.h"
#include "SKNamespaceLinkException.h"
#include "SKNamespaceDeletionException.h"
#include "SKNamespaceRecoverException.h"
#include "SKClientException.h"
#include "SKOpSizeBasedTimeoutController.h"
#include "SKSecondaryTarget.h"
#include "SKNamespaceOptions.h"
#include "SKVersionProvider.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <signal.h>
#include <time.h>
#include <limits.h>

#include <string>
#include <sstream>
#include <map>
#include <set>
#include <vector>
#include <iostream>
#include <fstream>
#include <chrono>

using namespace std;
using namespace std::chrono;

#ifndef __GNUC__
#define EOSTR   (-1)
#define ERR(s, c)       if(opterr){\
        char errbuf[2];\
        errbuf[0] = c; errbuf[1] = '\n';\
        fputs(argv[0], stderr);\
        fputs(s, stderr);\
        fputc(c, stderr);}

int     opterr = 1;
int     optind = 1;
int     optopt;
char    *optarg;

int getopt(int argc, char **argv, char *opts)
{
        static int sp = 1;
        register int c;
        register char *cp;

        if(sp == 1)
                if(optind >= argc ||
                   argv[optind][0] != '-' || argv[optind][1] == '\0')
                        return(EOSTR);
                else if(strcmp(argv[optind], "--") == NULL) {
                        optind++;
                        return(EOSTR);
                }
        optopt = c = argv[optind][sp];
        if(c == ':' || (cp=strchr(opts, c)) == NULL) {
                ERR(": illegal option -- ", c);
                if(argv[optind][++sp] == '\0') {
                        optind++;
                        sp = 1;
                }
                return('?');
        }
        if(*++cp == ':') {
                if(argv[optind][sp+1] != '\0')
                        optarg = &argv[optind++][sp+1];
                else if(++optind >= argc) {
                        ERR(": option requires an argument -- ", c);
                        sp = 1;
                        return('?');
                } else
                        optarg = argv[optind++];
                sp = 1;
        } else {
                if(argv[optind][++sp] == '\0') {
                        sp = 1;
                        optind++;
                }
                optarg = NULL;
        }
        return(c);
}
#endif  /* __GNUC__ */

/**
 * this function should be called from catch(...){  } handler block
 * it re-throws the caught exception and handles it accordingly
 * Purpose of this function is to eliminate exception handling code duplication
 * Arguments:
 * const char * msg - message from the context,
 * const char * filename - source file in which exception caught
 * const int lineNum - line number
 */
void exhandler( const char * msg, const char * filename, const int lineNum, const char * nsName); 
void     usage(char const * const name_, char const * const pMsg_);


struct CmdLineOptions {
	string gcName;
	string host;
	string action;
	string ns; // SilverKing namespace
	string key = "Hello";
	string value = "World";
	string logfile;
	string nsOptions = "versionMode=CLIENT_SPECIFIED,revisionMode=UNRESTRICTED_REVISIONS,storageType=FILE,consistencyProtocol=TWO_PHASE_COMMIT";
	string jvmOptions = "-Xmx1024M,-Xcheck:jni";
	int valueVersion = 0;
	SKCompression::SKCompression compressType = SKCompression::NONE;
	// int valueVersion = 1;
	// SKCompression::SKCompression compressType = SKCompression::LZ4;
	SKRetrievalType retrievalType = VALUE_AND_META_DATA;
	int threshold = 100;
	int timeout = INT_MAX;
	int verbose = 0;
};

class Util {
	public:
		static CmdLineOptions parseCmdLine(int argc, char ** argv);
		static CmdLineOptions parseCmdLineMeta(int argc, char ** argv);
		static SKCompression::SKCompression getCompressionType(string compr);
		static StrValMap getStrValMap(const vector<string>& ks, const vector<string>& vs);
		static void getKeyValues(StrValMap& valMap, const map<string, string>& kvs);
		static StrVector getStrKeys(const vector<string>& ks);

		static vector<string> getValues(StrValMap* strValMap);
		static vector<string> getValues(StrSVMap* svMap, SKRetrievalType retrievalType = VALUE_AND_META_DATA);
		static map<string, string> getStrMap(StrValMap* strValMap);
		static map<string, string> getStrMap(StrSVMap* svMap, SKRetrievalType retrievalType = VALUE_AND_META_DATA);
	
		typedef std::chrono::high_resolution_clock high_resolution_clock;
		typedef std::chrono::time_point<high_resolution_clock> time_point;
		static void logElapsedTime(const time_point beginTime_, const string & dhtOp_, const string & ns_, const string & key_);
		class HighResolutionClock {
		public:
			HighResolutionClock(const string& label_, const string& ns_ = std::string(), const string& key_ = std::string()) : _label(label_), _ns(ns_), _key(key_), _begin(high_resolution_clock::now()) {}
			~HighResolutionClock() {
				logElapsedTime(_begin, _label, _ns, _key);
			}
			private:
				string _label;
				string _ns;
				string _key;
				time_point _begin;
		};

	private:
		static CmdLineOptions parseCmdLine(int argc, char ** argv, CmdLineOptions& cmdLineOptions);

};

#endif
