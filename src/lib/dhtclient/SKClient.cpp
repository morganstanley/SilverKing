#include "SKClient.h"
#include "SKSession.h"
#include "SKClientDHTConfigurationProvider.h"
#include "SKGridConfiguration.h"
#include "SKSessionOptions.h"
#include "SKValueCreator.h"
#include "skconstants.h"
#include "jenumutil.h"

#include <string>
#include <vector>
using namespace std;
using std::string;
#include <iostream>
using std::cout;
using std::endl;
#include <sstream>
#include <fstream>
#include <boost/thread/recursive_mutex.hpp>
#include <sys/types.h>
#include <string.h>
#include <exception>

#include "jace/Jace.h"
using jace::java_new;
using jace::java_cast;
using namespace jace;
#include "jace/StaticVmLoader.h"
using jace::StaticVmLoader;
#include "jace/JClass.h"
using jace::JClass;
#include "jace/JClassImpl.h"
using jace::JClassImpl;
#include "jace/JNIException.h"
using jace::JNIException;
#include "jace/OptionList.h"
using jace::OptionList;
#include "jace/VirtualMachineShutdownError.h"
using jace::VirtualMachineShutdownError;

#include "jace/proxy/java/lang/Throwable.h"
using ::jace::proxy::java::lang::Throwable;
#include "jace/proxy/java/lang/String.h"
using jace::proxy::java::lang::String;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/DHTClient.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::DHTClient;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/DHTSession.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::DHTSession;
#include "jace/proxy/com/ms/silverking/cloud/dht/SessionOptions.h"
using jace::proxy::com::ms::silverking::cloud::dht::SessionOptions;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/ClientDHTConfigurationProvider.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::ClientDHTConfigurationProvider;
#include "jace/proxy/com/ms/silverking/cloud/dht/gridconfig/SKGridConfiguration.h"
namespace jgc = jace::proxy::com::ms::silverking::cloud::dht::gridconfig;  //namespace aliasing
#include "jace/proxy/com/ms/silverking/cloud/dht/ValueCreator.h"
using jace::proxy::com::ms::silverking::cloud::dht::ValueCreator;


#ifdef _MSC_VER
  #include <process.h>
  #include <Windows.h>
  #include <io.h>
  #include "jace/Win32VmLoader.h"
  using jace::Win32VmLoader;
  static const char * jvmHeapMax="-Xmx1G";
  static const char * jvmHeapStart="-Xms16M";
#else
  #include <unistd.h>
  #include <dlfcn.h>
  #include "jace/UnixVmLoader.h"
  using ::jace::UnixVmLoader;
  static const char * jvmHeapStart="-Xms128M";
  static const char * jvmHeapMax="-Xmx4G";
#endif

#define ppstringize(a) #a
#ifndef _MSC_VER
  const string dirSep = "/";           //directory separator 
  const string libSep = ":";           //lib path separator
  #define SK_JVM_LIB_PATH_ENV "LD_PRELOAD"  // env. variable with preloaded .so libs
  const char *pJvmLib = "libjvm.so";  // jvm dll name
  const char *pJvmLibDefaultPath = "/jre/lib/amd64/server/";
  #define JAVA_HOME_ENV "SK_JAVA_HOME"
  const char *javaHome;
  #define JACE_HOME_ENV "SK_JACE_HOME"
  const char *jaceHome;
  #define _putenv_s( name, val ) ( setenv( name, val, 1 ) )
#else
  #define R_OK     4       /* read permission.  */
  #define W_OK     2       /* write permission.  */
  //#define   X_OK 1       /* execute permission - unsupported */
  #define F_OK     0       /* existence.  */
  #define access _access

  const string dirSep = "\\";          //directory separator 
  const string jarSep = ";";           //jar classpath separator
  const string libSep = ";";           //lib path separator
  #define SK_JVM_LIB_PATH_ENV "LD_PRELOAD"  // env. variable with preloaded dlls
  const char * pJvmLib = "jvm.dll";    // jvm dll name
  #ifdef _WIN64
    const char * jvmPath = "jre\\bin\\server\\jvm.dll";
    const char * defaultLdLibPath=";.;jre\\bin\\server;pthreads;boost\\msvc100_64\\lib;jace\\Release\\dynamic;" ;
    const char * defaultSkClassPath = NULL;
  #else  //_WIN32 - there's no java 1.8 for win32
    const char * jvmPath = "jre\\bin\\server\\jvm.dll";
    const char * defaultLdLibPath=";.;jre\\bin\\server;pthreads;boost\\msvc100\\lib;jace\\Release\\dynamic;" ;
    const char * defaultSkClassPath= "";
  #endif
#endif

//env vars utilized by SK
#define GC_SK_JVM_JDWP_OPT "GC_SK_JVM_JDWP_OPT"
#define GC_SK_JVM_HPROF_OPT "GC_SK_JVM_HPROF_OPT"
#define SK_CLASSPATH "SK_CLASSPATH"
#define SK_JVM_ARGS "SK_JVM_ARGS"
#ifndef _MSC_VER
  #define SK_LIB_PATH "LD_LIBRARY_PATH"
#else
  #define SK_LIB_PATH "PATH"
#endif

vector<string> * parseJvmArgs(const char * str) {
    vector<string> * result = new vector<string>();

    char delim1 = ',';
    if (!str || strlen(str) == 0) {
        str = getenv(SK_JVM_ARGS);
    } 

    if (str) {
        do {
            const char *begin = str;

            while(*str != delim1 && *str ) //&& *(str+1) != '-'
                str++;

            if ( str - begin > 1 ) {
                result->push_back(string(begin, str));
            }
        } while (0 != *str++);
        //result->push_back("-XX:ParallelGCThreads=4"); 
    } 
    if (result->size() == 0) {
        result->push_back(jvmHeapMax);
        result->push_back(jvmHeapStart);
        result->push_back("-XX:ParallelGCThreads=4"); 
        result->push_back("-Xcheck:jni");
    }
    return result;
}

std::string resolveLibPath(void) {
	std::string path;
	
  #ifndef _MSC_VER
	Dl_info dl_info;
	dladdr((void *)sk_create_val, &dl_info);
	path = dl_info.dli_fname;
  #else
	char thisLibPath[2048];
	HMODULE dllHandle = GetModuleHandleA("silverking");
	if (!dllHandle) {
		cout << "ERROR: cannot get silverking handle" <<endl;
		throw std::exception();
	}
	if (!GetModuleFileNameA( dllHandle, thisLibPath, 2048 )) {
		cout << "ERROR: cannot get silverking lib path" <<endl;
		throw std::exception();
	}

	path = thisLibPath;
  #endif
	return path;
}

string checkEnvLibPath(LoggingLevel level) {
    char      *envPath = getenv(SK_LIB_PATH);
    string    libPath ;

    if (!envPath || !strstr(envPath, "jdk") || !strstr(envPath, "jace")) {
        ostringstream ldpath;
        
        libPath = resolveLibPath();

        int lastSeparatorPos = libPath.size();
        lastSeparatorPos = libPath.rfind(dirSep, lastSeparatorPos - 1);
        string wdir = libPath.substr(0, lastSeparatorPos);

        if (envPath) {
              libPath = wdir + libSep + envPath + libSep 
                        + javaHome +"/jre/lib/amd64/server"+ libSep + jaceHome +"/lib/dynamic";
              if (level <= LVL_INFO) {
                cout << "libPath " << libPath << endl;
              }
        } else {
            libPath = wdir + javaHome +"/jre/lib/amd64/server"+ libSep + jaceHome +"/lib/dynamic";;
            if (_putenv_s(SK_LIB_PATH, libPath.c_str()) != 0) {
                cout << "failed to set " << SK_LIB_PATH <<endl;
                throw std::exception(); // env var is not specified
            } else {
                cout << "set " << SK_LIB_PATH << " " << libPath << endl;
            }
         }
    } else {
        libPath = envPath ;
    }
    return libPath;
}

static void addJvmDebugOpts(OptionList& list, const string &hprofName) {
#ifdef _DEBUG
	//various logging opts
	//string logFileDef = string("-Dorg.slf4j.simpleLogger.logFile=") + logName ;
	//list.push_back(jace::CustomOption(logFileDef ) );  // slf4j logger file
	//list.push_back(jace::CustomOption("-Dorg.slf4j.simpleLogger.defaultLogLevel=trace") );  // set slf4j log level to tracing

	list.push_back(jace::Verbose (Verbose::JNI));
	list.push_back(jace::Verbose (Verbose::CLASS));

	// set profiling options
	char * hprofEnvOpts = getenv(GC_SK_JVM_HPROF_OPT);
	if (hprofEnvOpts && strstr(hprofEnvOpts, "-agentlib:hprof") != NULL) {
		cout << "setting hprof options: " << hprofEnvOpts <<endl;
		list.push_back( jace::CustomOption( hprofEnvOpts ) );
	} else {
		string profOpts="-agentlib:hprof=heap=all,doe=y,format=a,thread=y,file=";
		profOpts += hprofName;
		list.push_back( jace::CustomOption( profOpts ) );
		cout << "enabling jvm profiling to file " << hprofName <<endl;
	}

	//enable remote debugging 
	list.push_back( jace::CustomOption( "-Xdebug" ) );
	char * remoteDebugOpts = getenv(GC_SK_JVM_JDWP_OPT);
	if (remoteDebugOpts && strstr(remoteDebugOpts, "-agentlib:jdwp") != NULL) {
		cout << "setting remoteDebugOpts: " << remoteDebugOpts <<endl;
		list.push_back( jace::CustomOption( remoteDebugOpts ) );
	} else {
		cout << "setting default remoteDebugOpts" <<endl;
		list.push_back( jace::CustomOption( "-agentlib:jdwp=transport=dt_socket,address=8357,server=y,suspend=n" ) );
		////list.push_back( jace::CustomOption( "-agentlib:jdwp=transport=dt_socket,address=127.0.0.1:9009,server=y,suspend=n" ) );
	}
	list.push_back( jace::CustomOption( "-XX:+TraceClassUnloading") );
	list.push_back( jace::CustomOption( string("-XX:HeapDumpPath=") + hprofName + string(".heap") ) );
	list.push_back( jace::CustomOption( "-XX:-AllowUserSignalHandlers") );

	list.push_back( jace::CustomOption( "-XX:+PrintGC") );
	list.push_back( jace::CustomOption( "-XX:+PrintGCDetails") );
	string gclog = string("-Xloggc:") + hprofName + string(".gclog") ;
	list.push_back( jace::CustomOption( gclog ) );
#endif  //_DEBUG
}

static const char *getJvmlibPath(LoggingLevel level) {
    char* p;
    
	p = getenv(SK_JVM_LIB_PATH_ENV);
    if (p == NULL) {
        int size;
        
        if (level <= LVL_INFO) {
            fprintf(stderr, "%s is NULL. Using default.\n", SK_JVM_LIB_PATH_ENV);
        }
        size = strlen(javaHome) + strlen(pJvmLibDefaultPath) + strlen(pJvmLib) + 1;
        p = (char *)calloc(size, 1); // 1-time leak
        snprintf(p, size, "%s%s%s", javaHome, pJvmLibDefaultPath, pJvmLib);
    }
    return p;
}

static bool isJvmInitialized = false;
SKClient * SKClient::pClient = NULL;
////pthread_mutex_t sClientLock = PTHREAD_MUTEX_INITIALIZER;
//boost::recursive_mutex sClientLock;
static boost::recursive_mutex sClientLock;

static void vmInit(LoggingLevel level, const char * pJvmOptions) {
	try	{
        javaHome = getenv(JAVA_HOME_ENV);
        if (javaHome == NULL) {
            fprintf(stderr, "FATAL ERROR: %s not set\n", JAVA_HOME_ENV);
            exit(-1);
        }
        jaceHome = getenv(JACE_HOME_ENV);
        if (jaceHome == NULL) {
            fprintf(stderr, "FATAL ERROR: %s not set\n", JACE_HOME_ENV);
            exit(-1);
        }
		string envPath = checkEnvLibPath(level);
		const char *jvmlibPath = getJvmlibPath(level);

	#if defined(_MSC_VER) 
		if (level < LVL_INFO) {
			cout <<"Using Win32VmLoader with java lib path: " <<jvmlibPath <<endl;
        }
		Win32VmLoader loader(jvmlibPath, JNI_VERSION_1_6);
    #elif defined(JACE_WANT_DYNAMIC_LOAD)
		if (level < LVL_INFO) {
			cout <<"Using UnixVmLoader with java lib path: " <<jvmlibPath <<endl;
        }
		UnixVmLoader loader(jvmlibPath, JNI_VERSION_1_6);
	#else
		if (level < LVL_INFO) {
			cout <<"Using StaticVmLoader " <<endl;
        }
		StaticVmLoader loader(JNI_VERSION_1_6);
	#endif

		jsize nVMs = -1;
		jint retCode = loader.getCreatedJavaVMs(NULL, 0, &nVMs); //get the number of created MVs
		//cout << "getCreatedJavaVMs " << (int) nVMs <<"\n";
		if (retCode!=0) {
			//error
			cout << "getCreatedJavaVMs Failed with err code :" << (int) retCode <<"\n";
			throw std::exception();
		}
		if (nVMs > 0) {  //already initialized;
			isJvmInitialized = true;
			return;
		}

		OptionList list;

        vector<string> *pJvmArgs = parseJvmArgs(pJvmOptions);
        vector<string>::iterator it ;
        for (it = pJvmArgs->begin(); it!=pJvmArgs->end(); it++) {
    		list.push_back(jace::CustomOption(*it));
            //cout <<"Java Custom Options: " <<it->c_str() <<endl;
        }
        delete pJvmArgs;

		//get set of *.jar from env var
		char *pClassPath = getenv(SK_CLASSPATH);
        if (pClassPath && strlen(pClassPath) > 1) {
			list.push_back(jace::ClassPath(pClassPath));
            if (level < LVL_WARNING) {
	    	    cout << "env ClassPath : " << pClassPath << endl;
            }
        } else {
            fprintf(stderr, "FATAL_ERROR: Unresolved CLASSPATH\n");
            exit(-1);
        }

        ostringstream os ;
        int myPid;
		string logName;
		string hprofName;
#ifdef  _WINDOWS
        myPid = (int)_getpid();
        os << "skclient."  <<(int)myPid << ".log" ;

		const int pathLen = 4096;
		TCHAR tempPath[pathLen] ;
		int actualPathLen = ::GetTempPath(pathLen, tempPath);
		cout << "set sk client logs to: " << tempPath <<endl;
		logName = string (tempPath) + os.str();
		hprofName = string (tempPath) + string("hprof.") + os.str();
#else
        myPid = getpid();
        os << "/tmp/skclient." << (int)getuid() <<"." <<(int)myPid ;
		logName = os.str() +  ".log";
		hprofName = os.str() + ".jvmprof";
#endif
   		//list.push_back(jace::CustomOption("-Dorg.slf4j.simpleLogger.defaultLogLevel=error") );
		if (level == LVL_ALL) {
			addJvmDebugOpts(list, hprofName);
		}

		string libPathOpt = string("-Djava.library.path=") + envPath ;  // "base\\ia32.nt.xp\\Debug";
		list.push_back(jace::CustomOption(libPathOpt) );

        jace::createVm(loader, list, false);
		if (level <= LVL_INFO) {
			cout << "vm created " <<endl;
        }
		SKClient::setLogLevel(level);

		isJvmInitialized = true;
	} catch (VirtualMachineShutdownError&) {
		cout << "The JVM was terminated in mid-execution. " << "\n";
        throw;
	} catch (JNIException& jniException) {
		cout << "An unexpected JNI error has occurred: " << jniException.what() << "\n";
        throw;
	} catch (Throwable& t) {
        cout << "A Throwable caugth in SKClient c-tor: "  << "\n";
        Log::logErrorWarning(t);
		t.printStackTrace();
        throw;
	}
    printf("%s %d\n", __FILE__, __LINE__);
}

bool SKClient::init(LoggingLevel level, const char *pJvmOptions) {
    if (pClient == NULL) {

	    boost::recursive_mutex::scoped_lock  lock(sClientLock);
	    if (pClient == NULL){
			vmInit(level, pJvmOptions);
	    }
    }
	return true;
}

SKClient *SKClient::getClient(LoggingLevel level, const char *pJvmOptions) {
    if (pClient == NULL) {
	    boost::recursive_mutex::scoped_lock  lock(sClientLock);
	    if (pClient == NULL){
			if (isJvmInitialized == false) {
				vmInit(level, pJvmOptions);
			}
			pClient = new SKClient();
		}
	}
	return pClient;
}

SKClient *SKClient::getClient(LoggingLevel level) {
    return getClient(level, NULL);
}

SKClient *SKClient::getClient() {
    return getClient(LVL_WARNING, NULL);
}

SKClient::SKClient() {
	try	{
		pImpl = new DHTClient ( java_new<DHTClient>() );
        if ( pImpl->isNull() ) {
            cout << "failed to instantiate new DHT Client" <<endl;
			throw std::exception(); //CLASSPATH env var is not specified
        }
	} catch (VirtualMachineShutdownError&) {
		cout << "The JVM was terminated in mid-execution. " << endl;
        throw;
	} catch ( JNIException& jniException ) {
		cout << "An unexpected JNI error has occurred: " << jniException.what() << endl;
        throw;
	}
	catch (Throwable& t) {
        cout << "A Throwable caugth in SKClient c-tor: "  << endl;
        Log::logErrorWarning(t);
		t.printStackTrace();
        throw;
	}
}

SKValueCreator * SKClient::getValueCreator() {
	ValueCreator * pValueCreator  = new ValueCreator(java_cast<ValueCreator>(
		DHTClient::getValueCreator()
		)); 
	return new SKValueCreator(pValueCreator);
}

SKClient::~SKClient() {
	try {
		if (pImpl!=NULL) {
			DHTClient * po = (DHTClient*)pImpl;
			{
				boost::recursive_mutex::scoped_lock lock(sClientLock);
				if (this == SKClient::pClient)
					SKClient::pClient = NULL;
					
				pImpl = NULL;
			}
			delete po; 
		}
	} catch ( VirtualMachineShutdownError& ) {
		cout << "The JVM was terminated in SKClient d-tor. " << endl;
        throw;
	} catch ( JNIException& jniException ) {
		cout << "An unexpected JNI error has occurred in ~SKClient: " << jniException.what() << endl;
        throw;
	} catch ( Throwable& t ) {
        cout << "A Throwable caugth in SKClient d-tor: "  << endl;
        Log::logErrorWarning(t);
		t.printStackTrace();
        throw;
	} catch(std::exception& ex) {
		cout << "An unexpected std::exception has occurred in SKClient d-tor: " << ex.what() << endl;
        throw;
	} catch(...) {
		cout << "An unexpected error has occurred in SKClient d-tor " <<  endl;
        throw;
	}
}

void SKClient::shutdown() {
	try {
		{
		boost::recursive_mutex::scoped_lock lock(sClientLock);
		if (pClient) {
			delete pClient;
			pClient = NULL;
		}

		jace::detach();
		jace::destroyVm();
		}
	} catch ( VirtualMachineShutdownError& ) {
		cout << "The JVM was terminated in SKClient::shutdown. " << endl;
        throw;
	} catch ( JNIException& jniException ) {
		cout << "An unexpected JNI error has occurred in SKClient::shutdown: " << jniException.what() << endl;
        throw;
	} catch ( Throwable& t ) {
        cout << "A Throwable caugth in SKClient::shutdown: "  << endl;
        Log::logErrorWarning(t);
		t.printStackTrace();
        throw;
	} catch (std::exception& ex) {
		cout << "An unexpected std::exception has occurred in SKClient::shutdown: " << ex.what() << endl;
        throw;
	} catch( ... ) {
		cout << "An unexpected error has occurred in SKClient::shutdown " <<  endl;
        throw;
	}
}

void SKClient::setLogLevel(LoggingLevel level) {
    Level * pLevel = getJavaLogLevel(level);
	Log::setLevel(*pLevel);
    delete pLevel;
}

void SKClient::setLogFile(const char * fileName) {
    String logFileName = java_new<String>((char*)fileName);
    Log::setPrintStreams(logFileName);  //FIXME: this creates empty file but does not redirect output
}

bool SKClient::attach(bool daemon) {
	boost::recursive_mutex::scoped_lock lock(sClientLock);
	if (pClient) {
		jace::attach(0,0,daemon);
		return true;
	}
	return false;
}

void SKClient::detach() {
	boost::recursive_mutex::scoped_lock lock(sClientLock);
	if (pClient) {
		jace::detach();
    }
}

SKSession * SKClient::openSession(SKClientDHTConfigurationProvider * dhtConfigProvider) {
    ClientDHTConfigurationProvider * pGcp = (ClientDHTConfigurationProvider *)dhtConfigProvider->getPImpl();
    DHTSession * pSession = new DHTSession(java_cast<DHTSession>(pImpl->openSession( *pGcp )));
    return new SKSession (pSession);
}

SKSession * SKClient::openSession(SKGridConfiguration * pGridConf) {
    jgc::SKGridConfiguration * pGConf = (jgc::SKGridConfiguration*)pGridConf->getPImpl();
    ClientDHTConfigurationProvider cdcp =  java_cast<ClientDHTConfigurationProvider>(*pGConf);
    DHTSession * pSession = new DHTSession(java_cast<DHTSession>(pImpl->openSession( cdcp )));
    return new SKSession (pSession);
}

SKSession * SKClient::openSession(SKGridConfiguration * pGridConf, const char * preferredServer) {
    jgc::SKGridConfiguration * pGConf = (jgc::SKGridConfiguration *)pGridConf->getPImpl();
    ClientDHTConfigurationProvider cdcp =  java_cast<ClientDHTConfigurationProvider>(*pGConf);
	SessionOptions sessOpt = java_new<SessionOptions>(cdcp, java_new<String>((char *)preferredServer) );
    DHTSession * pSession = new DHTSession(java_cast<DHTSession>(pImpl->openSession( sessOpt )));
    return new SKSession (pSession);
}

SKSession * SKClient::openSession(SKSessionOptions * sessionOptions) {
    SessionOptions * pSessOpt = (SessionOptions *) sessionOptions->getPImpl();
    if (!pSessOpt || pSessOpt->isNull()){
        return NULL;
    }
    if (pImpl->isNull()){
        return NULL;
    }    
    DHTSession * pSession = new DHTSession(java_cast<DHTSession>(pImpl->openSession( *pSessOpt )));
    return new SKSession (pSession);
}
