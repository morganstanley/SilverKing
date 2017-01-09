//#include "compat.h"
#ifdef _WIN32
#include <process.h>
#include <Windows.h>
#include "dbghelp.h"
#else
#include <unistd.h>
#endif

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
#include <exception>
using namespace std;
using std::string;
using std::cout;
using std::endl;
using std::exception;
using std::set;

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

/*
#include "jace/Jace.h"
using jace::java_new;
using jace::java_cast;
using namespace jace;
#include "jace/proxy/java/lang/String.h"
using jace::proxy::java::lang::String;
#include "jace/proxy/java/lang/Throwable.h"
using jace::proxy::java::lang::Throwable;
*/

#if defined(_MSC_VER)
#define strtoll _strtoi64
#endif

typedef SKMap<std::string, std::string>	 StrStrMap;
typedef SKMap<std::string, SKVal*>	     StrValMap;
typedef SKVector<const std::string*>     KeyVector;
typedef SKVector<std::string> 			 StrVector;
typedef SKMap<std::string, SKStoredValue*> StrSVMap;

const uint32_t connectTimeoutMillis = 10 * 60 * 1000;

#ifdef _MSC_VER
extern "C" void win_abrt_handler(int signal_num) {
	print_stacktrace("abrt_handler");
    exit(1);
}
#else
static void abrt_handler(int, siginfo_t*, void*) {
	print_stacktrace("abrt_handler");
    exit(1);
}
#endif

/*
static void segv_handler(int, siginfo_t*, void*) {
	print_stacktrace("segv_handler");
    exit(1);
}

static void term_handler() {
	print_stacktrace("terminate_handler");
}

static void unexpct_handler() {
	print_stacktrace("unexpected_handler");
}
*/

static int install_handler() {
#ifdef _MSC_VER
	signal(SIGABRT, &win_abrt_handler);
#else
	struct sigaction sigact;
	sigact.sa_flags = SA_SIGINFO | SA_ONSTACK;
	/*
	sigact.sa_sigaction = segv_handler;
	if (sigaction(SIGSEGV, &sigact, (struct sigaction *)NULL) != 0) {
		fprintf(stderr, "error setting signal handler for %d (%s)\n",
		SIGSEGV, strsignal(SIGSEGV));
	}
	*/
	sigact.sa_sigaction = abrt_handler;
	if (sigaction(SIGABRT, &sigact, (struct sigaction *)NULL) != 0) {
		fprintf(stderr, "error setting signal handler for %d (%s)\n",
		SIGABRT, strsignal(SIGABRT));
	}

	/*
    std::set_terminate(term_handler);
    std::set_unexpected(unexpct_handler);
	*/
#endif
	return 0;
}
// 

#ifndef __GNUC__
#define EOSTR	(-1)
#define ERR(s, c)	if(opterr){\
	char errbuf[2];\
	errbuf[0] = c; errbuf[1] = '\n';\
	fputs(argv[0], stderr);\
	fputs(s, stderr);\
	fputc(c, stderr);}
	//(void) write(2, argv[0], (unsigned)strlen(argv[0]));\
	//(void) write(2, s, (unsigned)strlen(s));\
	//(void) write(2, errbuf, 2);}

int	opterr = 1;
int	optind = 1;
int	optopt;
char	*optarg;

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

/* caller is responsible for deallocation */
static char * binaryToHex(const unsigned char* source, int length)
{
    char * pBuf = (char*)malloc(length*2+1);
    for(int i = 0; i < length; i++)
    {
        sprintf(pBuf+2*i, "%02x", source[i]);
    }
    return pBuf;
}

static void print_meta( SKOpResult::SKOpResult rc_, const char *ns_, const char *key_, SKStoredValue *pMeta_)
{
    if (rc_ == SKOpResult::SUCCEEDED)
    {
      fprintf(stdout, "-----------------------------------\n");
      fprintf(stdout, "Meta data for [%s:%s]\n", ns_, key_);

	  const char * pMetaStr =  NULL;
	  pMetaStr = pMeta_->toString(true);
	  if(pMetaStr) {
		fprintf(stdout, "%s\n",  pMetaStr);
		free((void*)pMetaStr); pMetaStr = NULL;
	  }
	  
	  /* //or manually process values here:
      fprintf(stdout, "length:         %d\n",  pMeta_->getUncompressedLength());
      fprintf(stdout, "storedLength:   %d\n", pMeta_->getStoredLength());
      fprintf(stdout, "version:        %llu\n", (unsigned long long) pMeta_->getVersion());

      SKValueCreator * pCreator = pMeta_->getCreator();
      SKVal * pIp = pCreator->getIP();
      if(pIp->m_len != sizeof(uint32_t))
          fprintf(stderr, "ERROR: Incorrect length of Creator SKP V4 address : %u\n", (unsigned int)pIp->m_len);

      uint32_t * ip = (uint32_t*)pIp->m_pVal;
	  const unsigned char* ipp= reinterpret_cast<const unsigned char*>(ip);
      fprintf(stdout, "creatorIP:      %d.%d.%d.%d\n", ipp[0], ipp[1],ipp[2],ipp[3] );
      sk_destroy_val(&pIp);
      delete pCreator;
	  
	  int64_t timeNs = pMeta_->getCreationTime();
      time_t tmpT = timeNs/(1000*1000*1000);
	  int nsTime = (int) (timeNs % (1000*1000*1000));
	  tm * tmTime = localtime(&tmpT);
	  tmTime->tm_year += 30 ; //year start is 2000 instead of 1970
	  tmpT = mktime ( tmTime );
      char *ctimeStr = strdup(ctime(&tmpT));
      char *eol;
      if ( (eol = strchr(ctimeStr, '\r')) != NULL || (eol=strchr(ctimeStr, '\n')) != NULL)
        *eol = '\0';
      fprintf(stdout, "creationTime:   %llu (%s, %dns)\n", (unsigned long long) timeNs, ctimeStr, nsTime);
      free((void*)ctimeStr);
	  
      // *********** User Data *********** 
      SKVal * pUserData = pMeta_->getUserData();
      if( pUserData ) {
          if ( pUserData->m_len > 0 )
          {
            fprintf(stdout, "userData:       %s(%d)\n", (char*)pUserData->m_pVal, (int)pUserData->m_len );
          }
          sk_destroy_val(&pUserData);
      }
      else 
          fprintf(stdout, "userData:       NULL\n" );

      // *********** Checksum *********** 
      SKVal * pChecksum = pMeta_->getChecksum();
      if( pChecksum ) {
          if ( pChecksum->m_len > 0 )
          {
            char * pHex= binaryToHex((unsigned char*)pChecksum->m_pVal, (int)pChecksum->m_len);
            fprintf(stdout, "Checksum:       0x%s (%d)\n", pHex, (int)pChecksum->m_len );
            delete pHex;
          }
          sk_destroy_val(&pChecksum);
      }
      else 
          fprintf(stdout, "Checksum:      NULL\n" );

      fprintf(stdout, "Compression:    %d \n", (int)pMeta_->getCompression());
      fprintf(stdout, "ChecksumType:   %d \n", (int)pMeta_->getChecksumType());
	  */

    }
    else if (rc_ == SKOpResult::NO_SUCH_VALUE)
    {
      fprintf(stdout, "no such value for %s:%s\n", ns_, key_);
    }
    else
    {
      fprintf(stdout, "error getting %s:%s (%d)\n", ns_, key_, rc_);
    }
}

void usage(char const * const name_, char const * const pMsg_)
{
  if (pMsg_) fprintf(stderr, "%s\n", pMsg_);
  fprintf(stderr, "usage:\n");
  fprintf(stderr, "%s <OPTIONS>\n", name_);
  fprintf(stderr, "\t-H             print this help page\n");
  fprintf(stderr, "\t-g GCNAME      Grid Configuration Name\n");
  fprintf(stderr, "\t-h HOST        DHT node server name\n");
  fprintf(stderr, "\t-a ACTION      put|mput|get|waitfor|mget|mwaitfor|getmeta|mgetmeta|sync|snapshot|amput|amget|amwaitfor|amgetmeta|asnapshot|async|createns|clone|linkto|deletens|recoverns\n");
  fprintf(stderr, "\t-n NAMESPACE\n");
  fprintf(stderr, "\t-k KEY\n");
  fprintf(stderr, "\t-v VALUE\n");
  fprintf(stderr, "\t-F FILE        put the content of the file\n");
  fprintf(stderr, "\t-c COMPRESSION none|zip|bzip2|snappy|lz4\n");
  fprintf(stderr, "\t-T SECONDS     waitfor timeout seconds\n");
  fprintf(stderr, "\t-V             turn on verbose logging\n");
  fprintf(stderr, "\t-f FILE        file to send the log to\n");
  fprintf(stderr, "\t-R number_of_runs\n");
  fprintf(stderr, "\t-t threshold_percentage  (for mwaitfor)\n");
  fprintf(stderr, "\t-i number      version number for stored key\n");
  fprintf(stderr, "\t-o nsOptions   namespace options storageType=[FILE|RAM],consistencyProtocol=[TWO_PHASE_COMMIT|LOOSE],versionMode=[SINGLE_VERSION|CLIENT_SPECIFIED|SEQUENTIAL|...]\n");
  fprintf(stderr, "\t-P PARENT      for clone and linkto: parent namespace name, default is none\n");
  fprintf(stderr, "\t-m TYPE        getmeta Retrieval type VALUE|META_DATA|VALUE_AND_META_DATA|EXISTENCE, default is VALUE_AND_META_DATA\n");

  //fprintf(stderr, "\t-M MODE        rd|rw|none cache mode, default is none\n");
  //fprintf(stderr, "\t-N number of namespaces (for m* operations)\n");
  fprintf(stderr, "\t-K number      number of keys (for m* operations)\n");
  
  //fprintf(stderr, "\t-d DEST        the dest to make namespace\n");
  //fprintf(stderr, "\t-Z ZKLOCS      zookeeper addresses\n");
  fprintf(stderr, "\t-s             use file storage\n");
  fprintf(stderr, "\t-J jvmOptions    options to jvm e.g. \'-Xmx1024M,-Xcheck:jni\'\n");
  //fprintf(stderr, "\t-r disable randomisazion in zookeeper connect (for debugging)\n");
  //fprintf(stderr, "\t-O OPTIONS storage options for DHT\n");

  if (pMsg_) exit(1);
  exit(0);
}

/**
 * this function should be called from catch(...){  } handler block
 * it re-throws the caught exception and handles it accordingly
 * Purpose of this function is to eliminate exception handling code duplication
 * Arguments:
 * const char * msg - message from the context, 
 * const char * filename - source file in which exception caught
 * const int lineNum - line number
 */
void exhandler( const char * msg, const char * filename, const int lineNum, const char * nsName) {
	std::ostringstream str ;
	str << string(msg) << " " << filename << ":" << lineNum << " ";
	try {
		throw;
	} catch(SKRetrievalException & e) {
        e.printStackTrace();
        fprintf( stderr, "SKRetrievalException %s %s:%d\n%s\n failed keys are :\n", msg, filename, lineNum, e.getDetailedFailureMessage().c_str());
        SKVector<string> * failKeys = e.getFailedKeys();
        if(failKeys->size() > 0) {
            int nKeys = failKeys->size();
            for(int ikey = 0; ikey < nKeys; ++ikey){
                SKOperationState::SKOperationState opst = e.getOperationState(failKeys->at(ikey));
                if(opst == SKOperationState::FAILED) {
                  // these keys are failed with cause
                  SKFailureCause::SKFailureCause fc = e.getFailureCause(failKeys->at(ikey));
                  fprintf( stdout, "\t key:%s -> state:%d, cause:%d\n", failKeys->at(ikey).c_str(), (int)opst, (int)fc);
                } else {
                  if(opst == SKOperationState::INCOMPLETE){
                    // these are timed-out keys
                    fprintf( stdout, "\t key:%s -> state:%d\n", failKeys->at(ikey).c_str(), (int)opst);
                  }
                  else {
                    // these are successfully retrieved keys
                    SKStoredValue * pStoredVal = e.getStoredValue(failKeys->at(ikey));
                    SKVal* pVal = pStoredVal->getValue();
                    fprintf(stdout, "got %s : %s => value: %s\n", nsName, failKeys->at(ikey).c_str(),  (char*)pVal->m_pVal);
                    //if (rt==VALUE_AND_META_DATA || rt == META_DATA) 
                    //    print_meta( SKOpResult::SUCCEEDED, nsName, failKeys->at(ikey).c_str(), pStoredVal);  //FIXME!
                    sk_destroy_val(&pVal);
                    delete pStoredVal;
                  }
                  
                }
            }
        }
        delete failKeys;

	} catch(SKPutException & e) {
        fprintf( stderr, "SKPutException %s %s:%d\n%s\n failed keys are :\n", msg, filename, lineNum, e.getDetailedFailureMessage().c_str());
        SKVector<string> * failKeys = e.getFailedKeys();
        if(failKeys->size() > 0) {
            int nKeys = failKeys->size();
            for(int ikey = 0; ikey < nKeys; ++ikey){
                SKOperationState::SKOperationState opst = e.getOperationState(failKeys->at(ikey));
                if(opst == SKOperationState::FAILED) {
                  // these keys are failed with cause
                  SKFailureCause::SKFailureCause fc = e.getFailureCause(failKeys->at(ikey));
                  fprintf( stdout, "\t key:%s -> state:%d, cause:%d\n", failKeys->at(ikey).c_str(), (int)opst, (int)fc);
                } else {
                   fprintf( stdout, "\t key:%s -> state:%d\n", failKeys->at(ikey).c_str(), (int)opst);
                }
            }
        }
        if(failKeys)
            delete failKeys;

        e.printStackTrace();
        print_stacktrace("exhandler");

	} catch (SKNamespaceCreationException & e) {
        e.printStackTrace();
	} catch (SKSyncRequestException & e) {
        e.printStackTrace();
        fprintf( stderr, "SKSyncRequestException %s %s:%d\n%s\n", msg, filename, lineNum, e.what() );
	} catch (SKSnapshotException & e) {
        e.printStackTrace();
        fprintf( stderr, "SKSnapshotException %s %s:%d\n%s\n", msg, filename, lineNum, e.what() );
	} catch (SKWaitForCompletionException & e) {
        e.printStackTrace();
        fprintf( stderr, "SKWaitForCompletionException %s %s:%d\n%s\n", msg, filename, lineNum, e.what() );
	} catch (SKClientException & e) {
        e.printStackTrace();
        fprintf( stderr, "SKClientException %s %s:%d\n%s\n", msg, filename, lineNum, e.what());
        //exit (-1);
    } catch (std::exception const& e) {
        print_stacktrace("exhandler");
        fprintf( stderr, "std::exception %s in %s:%d\n%s\n", msg, filename, lineNum, e.what());
        //exit (-1);
    }
	// catch (...) { 
    // Propagate everything else
    //}
}

/**
 * populate a vector with a number of keys or get keys from file 
 * StrVector & keys - vector to populate with keys
 * int numberOfKeys - number of keys to put into vector
 * const char * key - string that will be used to create keys, e.g key0, key1, ...
 * const char * fileWithKeys - name of the file with keys, each line representing a key
 * const char * appName -  string to pass to 
 */
void getKeys( StrVector & keys, int numberOfKeys, const char * key, const char * fileWithKeys, const char * appName) {
	const int buffsize = 512;
	char keyBuf[buffsize];
	if(!fileWithKeys || strlen(fileWithKeys) < 1 ) {
		if (!key || strlen(key) < 1) usage(appName, "invalid key");	
	
		for ( int i=0; i < numberOfKeys; i++ ) {
			if ( numberOfKeys > 1) {
			  sprintf( keyBuf, "%s%d", key, i);
			  //fprintf(stderr, "adding key : %s \n", keyBuf);
			  keys.push_back(keyBuf);
			}
			else {
			  //fprintf(stderr, "adding key : %s \n", key);
			  keys.push_back(key);
			}
			
		}
	} else {
		ifstream keyfile;
		keyfile.open (fileWithKeys, ifstream::in);
		int keysAdded = 0;
		while (keyfile.good()) {
			keyfile.getline(keyBuf, buffsize);
			if(strlen(keyBuf)> 1) {
				//fprintf(stderr, "adding key : %s \n", keyBuf);
				keys.push_back(keyBuf);
				++keysAdded;
			}
		}
		keyfile.close();
		if(!keysAdded) {
			usage(appName, "error reading put file");
		}
	}
}

void getKeyValues( StrValMap & vals, int numberOfKeys, const char * key, const char * value, const char * putFile, const char * appName) {
	const int buffsize = 16*1024;
	char keyBuf[buffsize];
	char valueBuf[buffsize];
	if(!putFile) {
		if (!key) usage(appName, "no keys specified");
		for( int j = 0; j < numberOfKeys; ++j) {
            SKVal* pDhtVal = sk_create_val();
			if ( numberOfKeys < 2 ) {
				//insert empty values into dht , if no value provided
                if(value)
                    sk_set_val(pDhtVal, strlen(value), (void*)value);
			    vals.insert(StrValMap::value_type(string(key), pDhtVal ));
			    //fprintf( stderr, "put: add [%s] - [%s]\n", key, (char*)(pDhtVal->m_pVal) );
			}
			else {
			  sprintf( keyBuf, "%s%d", key, j);
			  sprintf( valueBuf, "%s-%d", value, j);
              sk_set_val(pDhtVal, strlen(valueBuf), (void*)valueBuf);
			  vals.insert(StrValMap::value_type(string(keyBuf), pDhtVal) );
			  //fprintf( stderr, "put: add [%s] - [%s]\n", keyBuf, (char*)(pDhtVal->m_pVal) );
			}
		}
	} 
	else if (putFile)
	{
	  FILE *fh = fopen(putFile, "r");
	  if (!fh) usage(appName, "invalid put file");
	  struct stat st;
	  if (0 != fstat(fileno(fh), &st)) usage(appName, "invalid put file");
	  char *pBuf = (char*)malloc(st.st_size);
	  if (1 != fread(pBuf, st.st_size, 1, fh)) usage(appName, "error reading put file");
	  fclose(fh);
      SKVal* pDhtVal = sk_create_val();
      sk_set_val_zero_copy(pDhtVal, st.st_size, pBuf);
	  vals.insert(StrValMap::value_type(key, pDhtVal ));
	}
}

void showValues(StrValMap * vals, const char * ns) {
    if(!vals || vals->size() == 0){
	    fprintf(stderr, "error getting keys from namespace %s \n", ns);
	}
	else {
		StrValMap::const_iterator cit ;
		for(cit = vals->begin() ; cit != vals->end(); cit++ ){
			SKVal * pval = cit->second;
			if( pval != NULL ){
                fprintf(stdout, "got %s : %s  => value: %s\n", ns, cit->first.c_str(), string((char*)pval->m_pVal,pval->m_len).c_str());
		        sk_destroy_val( &pval );
            }else  {
                fprintf(stdout, "got %s : %s  => missing value\n", ns, cit->first.c_str());
            }
		}
    }
}
void showStoredValues(StrSVMap * svMap, const char * ns, SKRetrievalType rt) {
    if(!svMap || svMap->size() == 0){
	    fprintf(stderr, "error getting keys from namespace %s \n", ns);
	}
	else {
		StrSVMap::const_iterator cit ;
		for(cit = svMap->begin() ; cit != svMap->end(); cit++ ){
            SKStoredValue * pStoredVal = cit->second;
		    if(pStoredVal == NULL){
			    fprintf(stdout, "got key %s : %s => missing value \n", ns, cit->first.c_str() );
		    }
		    else{
                SKVal* pVal = pStoredVal->getValue();
				if(pVal) {
					fprintf(stdout, "got %s : %s => value: %s\n", ns, cit->first.c_str(),  string((char*)pVal->m_pVal,pVal->m_len).c_str());
				} else {
					if(rt == META_DATA || rt == EXISTENCE)
						fprintf(stdout, "got %s : %s => value retrieval was not requested \n", ns, cit->first.c_str() );
					else
						fprintf(stdout, "Failed to get value for %s : %s \n", ns, cit->first.c_str() );
				}
			    if (rt==VALUE_AND_META_DATA || rt == META_DATA) 
                    print_meta( SKOpResult::SUCCEEDED, ns, cit->first.c_str(), pStoredVal);  //FIXME!

                sk_destroy_val(&pVal);
			    delete pStoredVal;
		    }
        }
    }

}


int main(int argc, char *argv[])
{
  char const *host = 0;
  char const *action = 0;
  char const *ns = 0;
  char const *key = 0;
  char const *value = 0;
  char const *logfile = 0;
  char const *nsOptions = 0;
  char const *nsOptions2 = "storageType=FILE,consistencyProtocol=TWO_PHASE_COMMIT,versionMode=SINGLE_VERSION";
  char const *jvmOptions = 0;
  //FILE *logFile = 0;
  int verbose = 0;
  char const *putFile = 0;
  int timeout = INT_MAX;  //FIXME! DHT_WAIT_FOREVER
  SKCompression::SKCompression comprType = SKCompression::NONE;
  SKStorageType::SKStorageType storageType = SKStorageType::RAM;
  SKRetrievalType retrieveType = VALUE_AND_META_DATA;
  char const *parent = 0;
  char	*gcname = NULL;
  long long int valueVersion = 0;
  char * compress = NULL;
  char * retr = NULL;
  int threshold = 100;

  install_handler();

//  int numberOfNamespaces = 2;
  int numberOfKeys = 1;
  int nRuns = 1;
  int c;
  extern char *optarg;
  //while ((c = getopt(argc, argv, "g:Hh:a:n:k:v:F:Vf:c:s:T:N:K:t:rR:O:i:")) != -1)
  while ((c = getopt(argc, argv,   "g:Hh:a:n:k:v:F:Vf:c:s:T:K:m:t:rR:i:o:P:J:")) != -1)
  {
    switch (c)
    {
      case 'g' :
		gcname = optarg;
        break;
      case 'h' :
        host = optarg;
        break;
      case 'a' :
        action = optarg;
        break;
      case 'n' :
        ns = optarg;
        break;
      case 'k' :
        key = optarg;
        break;
      case 'v' :
        value = optarg;
        break;
      case 'f' :
        logfile = optarg;
        break;
      case 'V' :
        verbose = 1;
        break;
      case 'i' :
	    char * pEnd;
        valueVersion = strtoll(optarg, &pEnd, 10) ; 
        break;
      //case 'Z' :
      //  zklocs = optarg;
      //  break;
      //case 'D' :
      //  dhtname = optarg;
      //  break;
      case 'c' :
		compress = optarg;
		if(strncmp(compress,"none",4) == 0)
            comprType = SKCompression::NONE;
		else if(strncmp(compress,"zip",3) == 0)
			comprType = SKCompression::ZIP;
		else if(strncmp(compress,"bzip2",5) == 0)
			comprType = SKCompression::BZIP2;
		else if(strncmp(compress,"snappy",6) == 0)
			comprType = SKCompression::SNAPPY;
		else if(strncmp(compress,"lz4",3) == 0)
			comprType = SKCompression::LZ4;
		else {
			fprintf(stderr, "Wrong compression argument '%s' \n", optarg);
			usage( argv[0], "wrong compression argument");
			return 1;
		}
        break;
      case 's' :
        storageType = SKStorageType::FILE;
        break;
      case 'F' :
        putFile = optarg;
        break;
      case 'T' :
        timeout = atoi(optarg);
        break;
    //  case 'N':
    //    numberOfNamespaces = atoi(optarg);
    //    break;
      case 'K':
        numberOfKeys = atoi(optarg);
        break;
      case 'H':
        usage(argv[0], 0);
        break;
      case 'm' :
		retr = optarg;
		if(strncmp(retr,"VALUE_AND_META_DATA",19) == 0)
			retrieveType = VALUE_AND_META_DATA;
		else if(strncmp(retr,"META_DATA",9) == 0)
			retrieveType = META_DATA;
		else if(strncmp(retr,"VALUE",5) == 0)
            retrieveType = VALUE;
		else if(strncmp(retr,"EXISTENCE",9) == 0)
			retrieveType = EXISTENCE;
		else {
			fprintf(stderr, "Wrong retrieval type argument '%s' \n", optarg);
			usage( argv[0], "wrong retrieval type argument");
			return 1;
		}
        break;
      case 't':
		threshold = atoi(optarg);
		break;
	  case 'R':
		nRuns = atoi(optarg);
		break;
      case 'o' :
        nsOptions = optarg;
        break;
      case 'J' :
        jvmOptions = optarg;
        break;
       case 'P':
		  parent = optarg;
		  break;
      default :
        fprintf(stderr, "Unknown option '%c'", c);
        usage(argv[0], "unknown option");
        return 0;
    }
  }

  if (!action) usage(argv[0], "missing action");
  if (!ns)     usage(argv[0], "missing namespace");
  if (!gcname) usage(argv[0], "missing Grid Configuration Name");
  if (!host)   usage(argv[0], "missing Host Name");

  LoggingLevel llvl;
  llvl = (verbose)? LVL_ALL : LVL_OFF ;
  SKClient * client = NULL;
  SKGridConfiguration * pGC = NULL;
  SKClientDHTConfiguration * pCdc = NULL;
  SKSessionOptions * sessOption = NULL;
  SKSession * session = NULL;
  SKPutOptions * pPutOpt = NULL;
  SKNamespacePerspectiveOptions * pNspOptions = NULL;

  try { 
      bool inited = SKClient::init(llvl, jvmOptions);
	  if(!inited){
		  fprintf( stderr, "Failed to initialize JVM \n");
		  return 1;
	  }
	  if(logfile)
	    SKClient::setLogFile(logfile);
      client = SKClient::getClient();
      //fprintf( stderr, "got client %s:%d\n", __FILE__, __LINE__);
 	  pGC = SKGridConfiguration::parseFile(gcname);
      pCdc = pGC->getClientDHTConfiguration();
      sessOption = new SKSessionOptions( pCdc, host) ;
      delete pCdc; pCdc = NULL; 
	  delete pGC; pGC = NULL;

	  session = client->openSession(sessOption);
	//  SKSession * session = client->openSession(pGC, host);
	  SKOpSizeBasedTimeoutController * pOsCtrl = new SKOpSizeBasedTimeoutController();
	  std::set<SKSecondaryTarget*> * pTgts = new std::set<SKSecondaryTarget*>();
	  SKVal * userData = sk_create_val();
	  pPutOpt =  new SKPutOptions( pOsCtrl, pTgts, comprType, SKChecksumType::NONE,
			false, (int64_t) 0, userData);
	  sk_destroy_val(&userData);
	  delete pOsCtrl; pOsCtrl = NULL;
	  delete pTgts; pOsCtrl = NULL;  

	  //pNspOptions->defaultPutOptions(pPutOpt);
	  
  } catch(SKClientException &ce) {
      fprintf( stderr, "SKClientException %s:%d\n  message(): %s\n", __FILE__, __LINE__, ce.getMessage().c_str());
      exhandler("Client init", __FILE__, __LINE__, ns  );
  } catch(std::exception &ex) {
      fprintf( stderr, "std::exception %s:%d\n  what(): %s\n", __FILE__, __LINE__, ex.what());
      exhandler("Client init", __FILE__, __LINE__, ns  );
  } catch(...) {
      fprintf( stderr, "unknown exception %s:%d\n", __FILE__, __LINE__ );
	  exhandler("Client init", __FILE__, __LINE__, ns  );
  }

  SKNamespace * pNamespace = NULL;
  if(session ) {

  if (strlen(action) > 0 && strncmp(action, "a", 1) == 0) //non-empty, expects "a*" for async methods : amput, amget, amwaitfor, asnapshot, async
  {
    pNamespace = session->getNamespace(ns);
	pNspOptions = pNamespace->getDefaultNSPOptions()->defaultPutOptions(pPutOpt);
	SKAsyncNSPerspective * ansp = pNamespace->openAsyncPerspective(pNspOptions);
    //SKAsyncNSPerspective * ansp = session->openAsyncNamespacePerspective(ns, pNspOptions);

	if(ansp != NULL && pNspOptions ) {
	 for( int runCnt=0; runCnt < nRuns; ++runCnt) 
	 {
		if ( nRuns > 1 ) fprintf( stderr, "Running iteration %d of %d\n", runCnt, nRuns);

        //-------------------------------- AMPut  ---------------------------------------
		if (strlen(action) == 5 && strncmp(action, "amput", 5) == 0)
		{
			SKAsyncPut * pPut = NULL;
			if (!key) usage(argv[0], "missing key");

			StrValMap vals;
			if ( NULL == value)  value = "Default value";
			getKeyValues( vals, numberOfKeys, key, value, putFile, argv[0]);
			if(compress) {
				pPut = ansp->put(&vals, pPutOpt);
			}
			else {
				pPut = ansp->put(&vals);
			}

			//input data cleanup
			StrValMap::const_iterator cit ;
			for(cit = vals.begin() ; cit != vals.end(); cit++ ){
				SKVal * pVal = cit->second;
				sk_destroy_val(&pVal);
			}
            vals.clear();
            
			
			//do whatever processing here, while async put runs in async mode
			//...
			//to sync up with put completion, call waitForCompletion 
            try {
			    if(timeout == INT_MAX) {
				    pPut->waitForCompletion();
			    }
			    else {
				    //timeout was provided, use it
				    bool done = pPut->waitForCompletion(timeout, SECONDS);
				    if (verbose)  fprintf( stderr, "AsyncPut waitForCompletion %d\n", done);
				    if(!done) {
					    if(pPut->getState()==SKOperationState::INCOMPLETE) {
						    fprintf( stderr, "Async Put has not completed in %d s \n", timeout);
						    //completion requires more time. we either have to loop on waitForCompletion or just waitForCompletion
						    pPut->waitForCompletion();
					    }
				    }
			    }
            } catch (...){
                exhandler( "caught in amput", __FILE__, __LINE__, ns);
            }
			if ( pPut->getState()==SKOperationState::FAILED ) {
				SKFailureCause::SKFailureCause reason = pPut->getFailureCause();
				fprintf( stderr, "Async Put %p FailureCause : %d \n", pPut, reason);
			}
			pPut->close();
			delete pPut;			
			if (verbose)  fprintf( stderr, "AsyncPut done\n");
		}
        //-------------------------------- AMGet  ---------------------------------------
		else if (strlen(action) == 5 && strncmp(action, "amget", 5) == 0)
		{
			fprintf( stderr, "Invoking Async Get\n");
			if (!key) usage(argv[0], "missing key");
			StrVector keys;
			getKeys( keys, numberOfKeys, key, putFile, argv[0]);
			if ( valueVersion ) {
                SKVersionConstraint* pVc = SKVersionConstraint::maxBelowOrEqual(valueVersion);
				SKRetrievalType rt = VALUE_AND_META_DATA;
                //SKGetOptions getOpt = SKGetOptions(rt, pVc);
                SKGetOptions *getOpt = ansp->getOptions()->getDefaultGetOptions()->retrievalType(rt)->versionConstraint(pVc);
				SKAsyncRetrieval * pRetrieval = ansp->get(&keys, getOpt);
                delete pVc;
                delete getOpt;
				if(!pRetrieval) {
					fprintf( stderr, "Failed to create AsyncRetrieval \n");
					continue;
				}

				/****** do processing here then catchup with async call:  ********/
                try {
				    pRetrieval->waitForCompletion();
				    if ( pRetrieval->getState()==SKOperationState::FAILED ) {
					    SKFailureCause::SKFailureCause reason = pRetrieval->getFailureCause();
					    fprintf( stderr, "Async Get %p FailureCause : %d \n", pRetrieval, reason);
				    }
				    StrSVMap * svMap = NULL;
				    svMap = pRetrieval->getStoredValues();
                    showStoredValues(svMap, ns, VALUE);
				    if (svMap) delete svMap;
                } catch (...){
                    exhandler( "caught in amget", __FILE__, __LINE__, ns );
                }
				pRetrieval->close();
				delete pRetrieval;
			}
			else {
				StrValMap * vals = NULL ;
				SKAsyncValueRetrieval * pValueRetrieval = NULL;
				pValueRetrieval = ansp->get(&keys);
				if(!pValueRetrieval) {
					fprintf( stderr, "Failed to create AsyncValueRetrieval \n");
					continue;
				}
                try {
				    pValueRetrieval->waitForCompletion();
				    if ( pValueRetrieval->getState()==SKOperationState::FAILED ) {
					    SKFailureCause::SKFailureCause reason = pValueRetrieval->getFailureCause();
					    fprintf( stderr, "AsyncValueRetrieval %p FailureCause : %d \n", pValueRetrieval, reason);
				    }
				    vals = pValueRetrieval->getValues(); 
                    showValues(vals, ns); //it also deletes SKVal from map
                    if (vals) {
			            delete vals;
                    }
                } catch (...){
                    exhandler( "caught in amget", __FILE__, __LINE__, ns );
                }
				pValueRetrieval->close();
				delete pValueRetrieval;
			}
			fprintf( stderr, "Async Get completed\n");
		}
        //-------------------------------- AMWaitFor  ---------------------------------------
		else if (strlen(action) == 9 && strncmp(action, "amwaitfor", 9) == 0)
		{
			fprintf( stderr, "Invoking Async WaitFor\n");
			
			if (!key) usage(argv[0], "missing key");
			StrVector keys;
			getKeys( keys, numberOfKeys, key, putFile, argv[0]);
			if (timeout != INT_MAX || threshold != 100 || valueVersion ) {
                SKVersionConstraint* pVc = NULL;
                if ( valueVersion ) {
                    pVc = SKVersionConstraint::maxBelowOrEqual(valueVersion);
                }
                else{
                    pVc = new SKVersionConstraint(LLONG_MIN, LLONG_MAX, GREATEST);
                }
				SKRetrievalType rt = VALUE_AND_META_DATA;
                SKTimeoutResponse::SKTimeoutResponse timeResp = SKTimeoutResponse::TIGNORE;
                SKWaitOptions *waitOpt = ansp->getOptions()->getDefaultWaitOptions()->retrievalType(rt)->versionConstraint(pVc)->timeoutSeconds(timeout)->threshold(threshold)->timeoutResponse(timeResp);
				SKAsyncRetrieval * pRetrieval = ansp->waitFor(&keys, waitOpt);
                delete pVc;
				if(!pRetrieval) {
					fprintf( stderr, "Failed to create AsyncRetrieval \n");
					continue;
				}
                try {
				    pRetrieval->waitForCompletion();
				    if ( pRetrieval->getState()==SKOperationState::FAILED ) {
					    SKFailureCause::SKFailureCause reason = pRetrieval->getFailureCause();
					    fprintf( stderr, "Async WaitFor %p FailureCause : %d \n", pRetrieval, reason);
				    }
				    StrSVMap * svMap = NULL;
				    svMap = pRetrieval->getStoredValues();
                    showStoredValues(svMap, ns, VALUE);
				    if (svMap) delete svMap;
                } catch (...){
                    exhandler( "caught in amwaitfor", __FILE__, __LINE__, ns );
                }

				pRetrieval->close();
				delete pRetrieval;
			}
			else {
				StrValMap * vals = NULL ;
				SKAsyncValueRetrieval * pValueRetrieval = ansp->waitFor(&keys);
				if(!pValueRetrieval) {
					fprintf( stderr, "Failed to create AsyncValueRetrieval \n");
					continue;
				}
                try {
				    pValueRetrieval->waitForCompletion();
				    if ( pValueRetrieval->getState()==SKOperationState::FAILED ) {
					    SKFailureCause::SKFailureCause reason = pValueRetrieval->getFailureCause();
					    fprintf( stderr, "AsyncValueRetrieval %p FailureCause : %d \n", pValueRetrieval, reason);
				    }
				    vals = pValueRetrieval->getValues(); 
                    showValues(vals, ns); //it also deletes SKVal from map
                    if (vals) {
			            delete vals;
                    }
                } catch (...){
                    exhandler( "caught in amwaitfor", __FILE__, __LINE__, ns );
                }
				pValueRetrieval->close();
				delete pValueRetrieval;
			}
			fprintf( stderr, "Async WaitFor completed\n");
		}
        //-------------------------------- AMGetMeta  ---------------------------------------
        else if (strlen(action) == 9 && strncmp(action, "amgetmeta", 9) == 0)
		{
			fprintf( stderr, "Invoking Async Get Meta\n");
			if (!key) usage(argv[0], "missing key");
			StrVector keys;
			getKeys( keys, numberOfKeys, key, putFile, argv[0]);

            SKVersionConstraint* pVc = NULL;
            if ( valueVersion ) {
                pVc = SKVersionConstraint::maxBelowOrEqual(valueVersion);
            }
            else{
                pVc = new SKVersionConstraint(LLONG_MIN, LLONG_MAX, GREATEST);
            }
            //SKGetOptions getOpt = SKGetOptions(retrieveType, pVc);
            SKGetOptions *getOpt = ansp->getOptions()->getDefaultGetOptions()->retrievalType(retrieveType)->versionConstraint(pVc);
			SKAsyncRetrieval * pRetrieval = ansp->get(&keys, getOpt);
            delete pVc;
            delete getOpt;
			if(!pRetrieval) {
				fprintf( stderr, "Failed to create AsyncRetrieval \n");
				continue;
			}
            try {
			    pRetrieval->waitForCompletion();
			    if ( pRetrieval->getState()==SKOperationState::FAILED ) {
				    SKFailureCause::SKFailureCause reason = pRetrieval->getFailureCause();
				    fprintf( stderr, "Async Snapshot %p FailureCause : %d \n", pRetrieval, reason);
			    }
			    StrSVMap * svMap = NULL;
			    svMap = pRetrieval->getStoredValues();
                showStoredValues(svMap, ns, retrieveType);
			    if (svMap) delete svMap;
            } catch (...){
                exhandler( "caught in amwaitfor", __FILE__, __LINE__, ns );
            }

			pRetrieval->close();
			delete pRetrieval;
			verbose && fprintf( stderr, "Async MGetMeta completed\n");
		}
		else
		{
			usage(argv[0], "invalid action");
		}
	 } //for
	 delete ansp;  //handled by SKSession

	} //if ansp != NULL
  }
  //-------------------------------- CreateNamespace  ----------------------------------
  else if (strlen(action) == 8 && strncmp(action, "createns", 8) == 0){
    SKNamespaceOptions * pNSO = NULL;
    SKNamespace * pNs = NULL;
    try {
		if(nsOptions && strlen(nsOptions) > 1)
		{
			pNSO = SKNamespaceOptions::parse( nsOptions );
			pNs = session->createNamespace( ns, pNSO );
		} else {
			pNs = session->createNamespace( ns );
		}
    } catch ( SKNamespaceCreationException & e ){
            fprintf( stderr,  "caught SKNamespaceCreationException in %s:%d\n%s\n", __FILE__, __LINE__, e.what() );
    } catch (...){
        exhandler( "caught in createNamespace", __FILE__, __LINE__, ns );
    }
    if ( pNs == NULL )
        fprintf( stderr, "Failed to create NameSpace %s with options %s\n", ns, nsOptions);
    else {
        fprintf( stderr, "Created NameSpace %s with options %s\n", ns, nsOptions);
        delete pNs;
    }
    delete pNSO;
  }
  else if (strlen(action) == 5 && strncmp(action, "clone", 5) == 0){
      if(!parent || strlen(parent) < 1) usage(argv[0], "invalid parent");
      SKNamespace * pParentNs = session->getNamespace( parent );
      SKNamespace * pNs = NULL;
	  try {
		  if(valueVersion > 1) {
			pNs = pParentNs->clone(ns, valueVersion);
			fprintf( stdout, "Cloned versioned NameSpace %s into %s\n", parent, ns);
		  }
		  else {
			pNs = pParentNs->clone(ns);
			fprintf( stdout, "Cloned write-once NameSpace %s into %s\n", parent, ns);
		  }
		  delete pNs; pNs = NULL;
		  delete pParentNs; pParentNs = NULL;
	  } catch ( SKNamespaceLinkException & e ){
          fprintf( stderr,  "caught SKNamespaceLinkException in %s:%d\n%s\n", __FILE__, __LINE__, e.what() );
	  }
  }
  else if (strlen(action) == 6 && strncmp(action, "linkto", 6) == 0){
	  try {
		  if(!parent || strlen(parent) < 1) usage(argv[0], "invalid parent");
		  SKNamespace * pNs = session->getNamespace( ns );
		  pNs->linkTo(parent);
		  fprintf( stdout, "Linked NameSpace %s to parent %s\n", ns, parent);
		  delete pNs;
	  } catch ( SKNamespaceLinkException & e ){
          fprintf( stderr,  "caught SKNamespaceLinkException in %s:%d\n%s\n", __FILE__, __LINE__, e.what() );
	  }
  }
  else if (strlen(action) == 8 && strncmp(action, "deletens", 8) == 0){
	  try {
		  if(!ns || strlen(ns) < 1) usage(argv[0], "invalid namespace name");
		  session->deleteNamespace( ns );
		  fprintf( stdout, "NameSpace %s deleted\n", ns);
	  } catch ( SKNamespaceDeletionException & e ){
          fprintf( stderr,  "Failed to delete %s : DeletionException %s\nin %s:%d\n", ns, e.what(), __FILE__, __LINE__ );
	  }
  }
  else if (strlen(action) == 9 && strncmp(action, "recoverns", 9) == 0){
	  try {
		  if(!ns || strlen(ns) < 1) usage(argv[0], "invalid namespace name");
		  session->recoverNamespace( ns );
		  fprintf( stdout, "NameSpace %s recovered\n", ns);
	  } catch ( SKNamespaceRecoverException & e ){
          fprintf( stderr,  "Failed to recover %s : RecoverException %s\nin %s:%d\n", ns, e.what(), __FILE__, __LINE__ );
	  } catch (SKClientException & e) {
		exhandler( "SKClientException in recoverNamespace ", __FILE__, __LINE__, ns );
		return 1;
	  }
  }
  else if(strlen(action) == 6 && strncmp(action, "setnso", 6) == 0){
	SKNamespaceOptions * pNsOptions = NULL;
	try {
		fprintf( stderr, "inside setnso \n" );
		pNsOptions = SKNamespaceOptions::parse(nsOptions2);
		char * pOptStr = pNsOptions->toString();
		fprintf( stdout, "pNsOptions : %s\n", pOptStr );
		delete pOptStr; pOptStr = NULL;

		pNsOptions->storageType(storageType); // or e.g. SKStorageType::RAM
		pNsOptions->consistencyProtocol(TWO_PHASE_COMMIT);
		pNsOptions->versionMode(SEQUENTIAL);
		pNsOptions->segmentSize(33554432);
		pNsOptions->allowLinks(true);
		pOptStr = pNsOptions->toString();
		fprintf( stdout, "pNsOptions : %s\n",  pOptStr );
		delete pOptStr; pOptStr = NULL;

		delete pNsOptions; pNsOptions = NULL;
	} catch (SKClientException & ce ){
		exhandler( "SKClientException in setnso", __FILE__, __LINE__, ns );
		if(pNsOptions) 
			delete pNsOptions;
	}
  }
  else if (strlen(action) > 0 && strncmp(action, "a", 1) != 0) //non-empty, sync
  {
	SKSyncNSPerspective* snsp = NULL;
	try{
		pNamespace = session->getNamespace(ns);
		pNspOptions = pNamespace->getDefaultNSPOptions()->defaultPutOptions(pPutOpt);
		//fprintf( stderr, "calling openSyncNamespacePerspective \n" );
		snsp = session->openSyncNamespacePerspective(ns, pNspOptions);
	} catch (SKClientException & e) {
		exhandler( "SKClientException in openSyncNamespacePerspective ", __FILE__, __LINE__, ns );
		return 1;
	}


    /*  //---------------------------- Put Option Test --------------------------------
    pPutOpt->version(1374544094989);

    SKVal* pval = sk_create_val();
    char * pstr = strdup("userData Test");
    sk_set_val_zero_copy(pval, strlen(pstr), (void *)pstr );
    pPutOpt->userData(pval);

    int64_t putVer = pPutOpt->getVersion();
    SKCompression::SKCompression cmprs = pPutOpt->getCompression();
    SKVal* pRetVal = pPutOpt->getUserData();
    fprintf( stderr, "version: %lld  ", (long long int)putVer );
    fprintf( stderr, "compr: %d   userData %s \n", cmprs, (char*)pRetVal->m_pVal );
    fprintf( stderr, "PutOptions:toString %s \n", pPutOpt->toString().c_str() );

    sk_destroy_val(&pval);
    sk_destroy_val(&pRetVal);
    */ //---------------------------- Put Option Test --------------------------------

    if(!snsp) {
        fprintf( stderr, "Error: cannot create SyncNamespacePerspective %s \n", ns);
        return 1;
    }

    //-------------------------------- Put  ---------------------------------------
	if (strlen(action) == 3 && strncmp(action, "put", 3) == 0)
	{
		if (!key) usage(argv[0], "missing key");
		std::string strkey(key);
        SKVal* pval = sk_create_val();
        if(value && strlen(value)>0) {
            sk_set_val(pval, strlen(value), (void *)value);
        }
        try {
		    for( int runCnt=0; runCnt < nRuns; ++runCnt) {
			    if ( nRuns > 1 )  fprintf( stderr, "Running iteration %d of %d\n", runCnt, nRuns);
			    if(compress)
				    snsp->put(&strkey, pval, pPutOpt);
			    else
				    snsp->put(&strkey, pval);
            }
            fprintf(stderr, "Put succeed\n");
        } catch (SKPutException & pe ){
			fprintf(stdout, "SKPutException in testdht : %s" , pe.what() ); 
		} catch (SKClientException & ce ){
			exhandler( "caught in put", __FILE__, __LINE__, ns );
        } catch (...){
            exhandler( "caught in put", __FILE__, __LINE__, ns );
        }
        sk_destroy_val(&pval);

	}
    //-------------------------------- MPut  ---------------------------------------
	else if (strcmp(action, "mput") == 0)
	{
		if (!key) usage(argv[0], "missing key");

		StrValMap vals;
		if ( NULL == value)
		  value = "Default value";
		  
		getKeyValues( vals, numberOfKeys, key, value, putFile, argv[0]);
        try {
		    for( int runCnt=0; runCnt < nRuns; ++runCnt) {
			    if ( nRuns > 1 )  fprintf( stderr, "Running iteration %d of %d\n", runCnt, nRuns);
                if(compress) {
			        snsp->put(&vals, pPutOpt);
                }
                else {
			        snsp->put(&vals);
                }
		    }
            fprintf(stderr, "Put succeed\n");
        } catch (SKPutException & pe ){
			fprintf(stdout, "SKPutException in testdht : %s" , pe.what() ); 
			StrVector * failedKeys = pe.getFailedKeys();
			fprintf(stderr, "Put Failed for:\n");
			int fsz = failedKeys->size();
			for(int i=0; i<fsz; i++) 
				fprintf(stderr, "\t%s:\n", failedKeys->at(i).c_str());
			delete failedKeys;
		} catch (SKClientException & ce ){
			exhandler( "caught in mput", __FILE__, __LINE__, ns );
        } catch (...){
            exhandler( "caught in mput", __FILE__, __LINE__, ns );
        }


		//input data cleanup
		StrValMap::const_iterator cit ;
		for(cit = vals.begin() ; cit != vals.end(); cit++ ){
			SKVal * pVal = cit->second;
			sk_destroy_val(&pVal);
		}
        vals.clear();
	}
    //-------------------------------- Get  ---------------------------------------
	else if (strlen(action) == 3 && strncmp(action, "get", 3) == 0)
	{
		if (!key) usage(argv[0], "missing key");
        string strkey(key);
		if(valueVersion ) {
            SKVersionConstraint* pVc = SKVersionConstraint::maxBelowOrEqual(valueVersion);
			SKRetrievalType rt = VALUE_AND_META_DATA;
            //SKGetOptions getOpt = SKGetOptions(rt, pVc);
            SKGetOptions *getOpt = snsp->getOptions()->getDefaultGetOptions()->retrievalType(rt)->versionConstraint(pVc);
            
            SKStoredValue * pStoredVal = NULL;
            try {
			    for( int runCnt=0; runCnt < nRuns; ++runCnt) {
				    if ( nRuns > 1 )  fprintf( stderr, "Running iteration %d of %d\n", runCnt, nRuns);
				    if (pStoredVal) { delete pStoredVal ; pStoredVal = NULL ; } 
			        pStoredVal = snsp->get(&strkey, getOpt);
			    }
			} catch (SKClientException & ce ){
				exhandler( "caught in get", __FILE__, __LINE__, ns );
            } catch (...){
                exhandler( "caught in get", __FILE__, __LINE__, ns );
            }
		    if(pStoredVal == NULL){
			    fprintf(stdout, "got key %s : %s => missing value \n", ns, key );
		    }
		    else{
                SKVal* pVal = pStoredVal->getValue();
			    fprintf(stdout, "got %s : %s => value: %s\n", ns, key,  string((char*)pVal->m_pVal,pVal->m_len).c_str());
                sk_destroy_val(&pVal);
			    delete pStoredVal;
		    }
            delete pVc;
            delete getOpt;
		}
		else {
            SKVal * pval = NULL;
            try { 
			    for( int runCnt=0; runCnt < nRuns; ++runCnt) {
				    if ( nRuns > 1 )  fprintf( stderr, "Running iteration %d of %d\n", runCnt, nRuns);
				    if (pval)
                        sk_destroy_val( &pval );
			        pval = snsp->get(&strkey);
			    }
			} catch (SKClientException & ce ){
				exhandler( "caught in get", __FILE__, __LINE__, ns );
            } catch (...){
                exhandler( "caught in get", __FILE__, __LINE__, ns );
            }

            if( pval ){
				fprintf(stdout, "got %s : %s  => value: %s\n", ns, key, string((char*)pval->m_pVal,pval->m_len).c_str() );
                 sk_destroy_val( &pval );
            }else  {
                fprintf(stdout, "got %s : %s  => missing value\n", ns, key);
            }
		}
	}
    //-------------------------------- MGet  ---------------------------------------
	else if (strlen(action) == 4 && strncmp(action, "mget", 4) == 0)
	{
		if (!key) usage(argv[0], "missing key");
		
		StrVector keys;
		getKeys( keys, numberOfKeys, key, putFile, argv[0]);
		if ( valueVersion ) {
			StrSVMap * svMap = NULL;
            SKVersionConstraint* pVc = SKVersionConstraint::maxBelowOrEqual(valueVersion);
			SKRetrievalType rt = VALUE_AND_META_DATA;
            //SKGetOptions getOpt = SKGetOptions(rt, pVc);
            SKGetOptions *getOpt = snsp->getOptions()->getDefaultGetOptions()->retrievalType(rt)->versionConstraint(pVc);
            try {
			    for( int runCnt=0; runCnt < nRuns; ++runCnt) {
				    if ( nRuns > 1 )  fprintf( stderr, "Running iteration %d of %d\n", runCnt, nRuns);
				    if (svMap) { 
						StrSVMap::iterator cit ;
						for(cit = svMap->begin() ; cit != svMap->end(); cit++ ){
							SKStoredValue * pStoredVal = cit->second;
                            if(pStoredVal) {
		                        delete pStoredVal; pStoredVal = NULL;
							}
						}
						delete svMap ; svMap = NULL ;
                    } 
			        svMap = snsp->get(&keys, getOpt);
			    }
            } catch (...){
                exhandler( "caught in mget", __FILE__, __LINE__, ns );
            }

            showStoredValues(svMap, ns, VALUE);
			if (svMap) delete svMap;
            delete pVc;
            delete getOpt;
		}
		else {
			StrValMap * vals = NULL ;
            try {
			    for( int runCnt=0; runCnt < nRuns; ++runCnt) {
				    if ( nRuns > 1 )  fprintf( stderr, "Running iteration %d of %d\n", runCnt, nRuns);
			        if (vals) { 
						StrValMap::const_iterator cit ;
						for(cit = vals->begin() ; cit != vals->end(); cit++ ){
							SKVal * pVal = cit->second;
							sk_destroy_val(&pVal);
						}
						delete vals ; vals = NULL ;
                    } 
			        vals = snsp->get(&keys);
			    }
			} catch (SKClientException & ce ){
				exhandler( "caught in mget", __FILE__, __LINE__, ns );
            } catch (...){
                exhandler( "caught in mget", __FILE__, __LINE__, ns );
            }

            showValues(vals, ns);  //it also deletes SKVal from map
            if (vals) {
			    delete vals;
            }
		}
	}
    //-------------------------------- GetMeta  ---------------------------------------
	else if (strlen(action) == 7 && strncmp(action, "getmeta", 7) == 0)
	{
		if (!key) usage(argv[0], "missing key");
		StrVector keys;
		keys.push_back(key);
		
		StrSVMap * svMap = NULL;
        SKVersionConstraint* pVc = NULL;
        if ( valueVersion ) {
            pVc = SKVersionConstraint::maxBelowOrEqual(valueVersion);
        } else {
            pVc = new SKVersionConstraint(LLONG_MIN, LLONG_MAX, GREATEST);
        }
        //SKGetOptions getOpt = SKGetOptions(retrieveType, pVc);
        SKGetOptions *getOpt = snsp->getOptions()->getDefaultGetOptions()->retrievalType(retrieveType)->versionConstraint(pVc);
		//fprintf(stderr, "calling snsp get metatdata for %s:%s \n", ns, key);
        try {
		    for( int runCnt=0; runCnt < nRuns; ++runCnt) {
			    if ( nRuns > 1 )  fprintf( stderr, "Running iteration %d of %d\n", runCnt, nRuns);
			    if (svMap) { 
                    //cleanup the map containing pointers
					StrSVMap::const_iterator cit ;
					for(cit = svMap->begin() ; cit != svMap->end(); cit++ ){
						SKStoredValue * pStoredVal = cit->second;
                        if(pStoredVal) {
		                    delete pStoredVal; pStoredVal = NULL;
						}
					}
					delete svMap ; svMap = NULL ;
                } 
		        svMap = snsp->get(&keys, getOpt);
		    }
		    fprintf(stderr, "retrieved metatdata for %s:%s \n", ns, key);
        } catch (SKClientException & ce ){
			exhandler( "caught in getmeta", __FILE__, __LINE__, ns );
        } catch (...){
            exhandler( "caught in getmeta", __FILE__, __LINE__, ns );
        }

        showStoredValues(svMap, ns, retrieveType);
		if (svMap) delete svMap;
        delete pVc;
        delete getOpt;
	} 
    //-------------------------------- MGetMeta  ---------------------------------------
	else if (strcmp( action, "mgetmeta") == 0) 
	{
		if (!key) usage(argv[0], "missing key");
		StrVector keys;
		getKeys( keys, numberOfKeys, key, putFile, argv[0]);
		StrSVMap * svMap = NULL;

        SKVersionConstraint* pVc = NULL;
        if ( valueVersion ) {
            pVc = SKVersionConstraint::maxBelowOrEqual(valueVersion);
        } else {
            pVc = new SKVersionConstraint(LLONG_MIN, LLONG_MAX, GREATEST);
        }
        //SKGetOptions getOpt = SKGetOptions(retrieveType, pVc);
        SKGetOptions *getOpt = snsp->getOptions()->getDefaultGetOptions()->retrievalType(retrieveType)->versionConstraint(pVc);
        try { 
		    for( int runCnt=0; runCnt < nRuns; ++runCnt) {
			    if ( nRuns > 1 )  fprintf( stderr, "Running iteration %d of %d\n", runCnt, nRuns);
			    if (svMap) { 
                    //cleanup the map containing pointers
					StrSVMap::const_iterator cit ;
					for(cit = svMap->begin() ; cit != svMap->end(); cit++ ){
						SKStoredValue * pStoredVal = cit->second;
                        if(pStoredVal) {
		                    delete pStoredVal; pStoredVal = NULL;
						}
					}
					delete svMap ; svMap = NULL ;
                } 
		        svMap = snsp->get(&keys, getOpt);
		    }
        } catch (SKRetrievalException & e){
            fprintf( stderr, "Caught SKRetrievalException in mgetmeta %s:%d\n%s\n failed keys are :\n", __FILE__, __LINE__, e.getDetailedFailureMessage().c_str());
            SKVector<string> * failKeys = e.getFailedKeys();
            int nKeys = failKeys->size();
            for(int ikey = 0; ikey < nKeys; ++ikey){
                SKOperationState::SKOperationState opst = e.getOperationState(failKeys->at(ikey));
                if(opst == SKOperationState::FAILED) {
                  // these keys are failed with cause
                  SKFailureCause::SKFailureCause fc = e.getFailureCause(failKeys->at(ikey));
                  fprintf( stdout, "\t key:%s -> state:%d, cause:%d\n", failKeys->at(ikey).c_str(), (int)opst, (int)fc);
                } else {
                  if(opst == SKOperationState::INCOMPLETE){
                    // these are timed-out keys
                    fprintf( stdout, "\t key:%s -> state:%d\n", failKeys->at(ikey).c_str(), (int)opst);
                  }
                  else {
                    // these are successfully retrieved keys
                    SKStoredValue * pStoredVal = e.getStoredValue(failKeys->at(ikey));
                    SKVal* pVal = pStoredVal->getValue();
	                fprintf(stdout, "got %s : %s => value: %s\n", ns, failKeys->at(ikey).c_str(),  (char*)pVal->m_pVal);
	                if (retrieveType==VALUE_AND_META_DATA || retrieveType == META_DATA) 
                        print_meta( SKOpResult::SUCCEEDED, ns, failKeys->at(ikey).c_str(), pStoredVal);  //FIXME!
                    sk_destroy_val(&pVal);
                    delete pStoredVal;
                  }
                  
                }
            }
            delete failKeys;
        } catch (SKClientException & ce ){
			exhandler( "caught in mgetmeta", __FILE__, __LINE__, ns );
        } catch (...){
            exhandler( "caught in mgetmeta", __FILE__, __LINE__, ns );
        }
        showStoredValues(svMap, ns, retrieveType);
		if (svMap) delete svMap;
        delete pVc;
        delete getOpt;
	}
    //-------------------------------- WaitFor  ---------------------------------------
	else if (strlen(action) == 7 && strncmp(action, "waitfor", 7) == 0)
	{

		if (!key) usage(argv[0], "missing key");
        SKVersionConstraint* pVc = NULL;

        if ( valueVersion ) {
            pVc = SKVersionConstraint::maxBelowOrEqual(valueVersion);
        }
        else{
            pVc = new SKVersionConstraint(LLONG_MIN, LLONG_MAX, GREATEST);
        }
		SKRetrievalType rt = VALUE_AND_META_DATA;
        SKTimeoutResponse::SKTimeoutResponse timeResp = SKTimeoutResponse::EXCEPTION;
        //SKWaitOptions waitOpt = SKWaitOptions(rt, pVc, timeout, threshold, timeResp);
        SKWaitOptions *waitOpt = snsp->getOptions()->getDefaultWaitOptions()->retrievalType(rt)->versionConstraint(pVc)->timeoutSeconds(timeout)->threshold(threshold)->timeoutResponse(timeResp);
        try {
		    for( int runCnt=0; runCnt < nRuns; ++runCnt) {
			    if ( nRuns > 1 )  fprintf( stderr, "Running iteration %d of %d\n", runCnt, nRuns);
				SKStoredValue* pStoredVal = NULL; 
			    pStoredVal = snsp->waitFor(key, waitOpt);
                SKVal* pVal = NULL; 
				if( pStoredVal ) {
					pVal = pStoredVal->getValue();
					if(pVal && pVal->m_len > 0 ) {
						fprintf(stdout, "got %s : %s => value: %s\n", ns, key,  string((char*)pVal->m_pVal,pVal->m_len).c_str());
						if (rt==VALUE_AND_META_DATA || rt == META_DATA) 
							print_meta( SKOpResult::SUCCEEDED, ns, key, pStoredVal);  //FIXME!
						sk_destroy_val(&pVal);
					}
					else
						fprintf(stdout, "got %s : %s => value: <NULL>\n", ns, key);
					delete pStoredVal;
				}
				else
					fprintf(stdout, "got %s : %s => missing value \n", ns, key);


    		    fprintf(stderr, "waitfor completed for %s:%s \n", ns, key);
		    }
        } catch (SKRetrievalException & e ){
			fprintf( stderr, "SKRetrievalException %s:%d\n%s\n", __FILE__, __LINE__, e.what() );
        } catch (SKClientException & ce ){
			exhandler( "caught in waitfor", __FILE__, __LINE__, ns );
        } catch (...){
            exhandler( "caught in waitfor", __FILE__, __LINE__, ns );
            fprintf( stderr, "Caught ... in waitfor \n");
        }
        delete pVc;
        delete waitOpt;

		//fprintf(stderr, "sleep 5 useconds for the possible more notifications from dht, feel free to kill\n");
		//usleep(10);
	}
    //-------------------------------- MWaitFor  ---------------------------------------
	else if (strlen(action) == 8 && strncmp(action, "mwaitfor", 8) == 0)
	{
		if (!key) usage(argv[0], "missing key");
		StrVector keys;
		getKeys( keys, numberOfKeys, key, putFile, argv[0]);
		StrSVMap * svMap = NULL;
        SKVersionConstraint* pVc = NULL;
        if ( valueVersion ) {
            pVc = SKVersionConstraint::maxBelowOrEqual(valueVersion);
        }
        else{
            pVc = new SKVersionConstraint(LLONG_MIN, LLONG_MAX, GREATEST);
        }
		SKRetrievalType rt = VALUE_AND_META_DATA;
        //SKTimeoutResponse::SKTimeoutResponse timeResp = SKTimeoutResponse::TIGNORE;
        SKTimeoutResponse::SKTimeoutResponse timeResp = SKTimeoutResponse::EXCEPTION;
        //SKWaitOptions waitOpt = SKWaitOptions(rt, pVc, timeout, threshold, timeResp);
        SKWaitOptions *waitOpt = snsp->getOptions()->getDefaultWaitOptions()->retrievalType(rt)->versionConstraint(pVc)->timeoutSeconds(timeout)->threshold(threshold)->timeoutResponse(timeResp);
        try {
		    for( int runCnt=0; runCnt < nRuns; ++runCnt) {
			    if ( nRuns > 1 )  fprintf( stderr, "Running iteration %d of %d\n", runCnt, nRuns);
			    if (svMap) { 
                    //cleanup the map containing pointers
					StrSVMap::const_iterator cit ;
					for(cit = svMap->begin() ; cit != svMap->end(); cit++ ){
						SKStoredValue * pStoredVal = cit->second;
                        if(pStoredVal) {
		                    delete pStoredVal; pStoredVal = NULL;
						}
					}
					delete svMap ; svMap = NULL ;
                } 
			    svMap = snsp->waitFor(&keys, waitOpt);
		    }
            fprintf(stderr, "mwaitfor succedded \n");
        } catch (SKRetrievalException & e){
            fprintf( stderr, "Caught SKRetrievalException in mwaitfor %s:%d\n%s\n failed keys are :\n", __FILE__, __LINE__, e.getDetailedFailureMessage().c_str());
            SKVector<string> * failKeys = e.getFailedKeys();
            int nKeys = failKeys->size();
            for(int ikey = 0; ikey < nKeys; ++ikey){
                SKOperationState::SKOperationState opst = e.getOperationState(failKeys->at(ikey));
                if(opst == SKOperationState::FAILED) {
                  // these keys are failed with cause
                  SKFailureCause::SKFailureCause fc = e.getFailureCause(failKeys->at(ikey));
                  fprintf( stdout, "\t key:%s -> state:%d, cause:%d\n", failKeys->at(ikey).c_str(), (int)opst, (int)fc);
                } else {
                  if(opst == SKOperationState::INCOMPLETE){
                    // these are timed-out keys
                    fprintf( stdout, "\t key:%s -> state:%d\n", failKeys->at(ikey).c_str(), (int)opst);
                  }
                  else {
                    // these are successfully retrieved keys
                    SKStoredValue * pStoredVal = e.getStoredValue(failKeys->at(ikey));
                    SKVal* pVal = pStoredVal->getValue();
	                fprintf(stdout, "got %s : %s => value: %s\n", ns, failKeys->at(ikey).c_str(),  string((char*)pVal->m_pVal,pVal->m_len).c_str());
	                if (rt==VALUE_AND_META_DATA || rt == META_DATA) 
                        print_meta( SKOpResult::SUCCEEDED, ns, failKeys->at(ikey).c_str(), pStoredVal);  //FIXME!
                    sk_destroy_val(&pVal);
                    delete pStoredVal;
                  }
                  
                }
            }
            delete failKeys;
        } catch (SKClientException & ce ){
			exhandler( "caught in mwaitfor", __FILE__, __LINE__, ns );
        } catch (...){
            exhandler( "caught in mwaitfor", __FILE__, __LINE__, ns );
            fprintf( stderr, "Caught ... in mwaitfor \n");
        }

        if (svMap) {
            showStoredValues(svMap, ns, VALUE);
            delete svMap;
        }
        delete pVc;
        delete waitOpt;
	}
	else
	{
		usage(argv[0], "invalid action");
	}
	delete snsp;
  }

 } // if(session)

  fflush(stdout);
  fflush(stderr);
  if(pNamespace) {delete pNamespace; pNamespace = NULL;}
  if(pNspOptions) { delete pNspOptions; pNspOptions = NULL; }
  if(pPutOpt) { delete pPutOpt; pPutOpt = NULL; }
  if(session) { session->close(); delete session; session = NULL; }
  if(sessOption) { delete sessOption; sessOption = NULL;}
  if(client) { delete client; client = NULL; }
  SKClient::shutdown();

  return 0;
}
