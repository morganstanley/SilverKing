#include "jace/Jace.h"
using jace::java_new;
using jace::java_cast;
using namespace jace;

#include "jace/StaticVmLoader.h"
using jace::StaticVmLoader;
#ifdef _WIN32
  #include "jace/Win32VmLoader.h"
  using jace::Win32VmLoader;
#else
  #include "jace/UnixVmLoader.h"
  using ::jace::UnixVmLoader;
#endif


#include "jace/OptionList.h"
using jace::OptionList;

#include "jace/JArray.h"
using jace::JArray;

#include "jace/JClass.h"
using jace::JClass;
#include "jace/JClassImpl.h"
using jace::JClassImpl;

#include "jace/JNIException.h"
using jace::JNIException;

#include "jace/VirtualMachineShutdownError.h"
using jace::VirtualMachineShutdownError;

#include "jace/proxy/java/lang/Class.h"
#include "jace/proxy/java/lang/String.h"
#include "jace/proxy/java/lang/System.h"
#include "jace/proxy/java/io/PrintWriter.h"
#include "jace/proxy/java/io/IOException.h"
#include "jace/proxy/java/io/PrintStream.h"
using namespace jace::proxy::java::lang;
using namespace jace::proxy::java::io;

#include "jace/proxy/com/ms/silverking/cloud/dht/client/apps/SilverKingClient.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::apps::SilverKingClient;

#include <string>
using std::string;
#include <vector>
using std::vector;
#include <exception>
using std::exception;
#include <iostream>
using std::cout;
using std::endl;

vector<string> * getJvmArgsFromEnv(){
	const char * str = NULL;
    vector<string> * result = new vector<string>();
    char delim1 = ',';
    str = getenv("SK_JVM_ARGS");

    if (str) {
        do
        {
            const char *begin = str;

            while(*str != delim1 && *str ) //&& *(str+1) != '-'
                str++;

            if( str - begin > 1 ) {
                result->push_back(string(begin, str));
            }
        } while (0 != *str++);
    } 
    if(result->size() == 0) {
        result->push_back("-Xmx4G");
        result->push_back("-Xms32M");
        result->push_back("-Xcheck:jni");
        result->push_back("-XX:ParallelGCThreads=4");
    }
    return result;
}


/**
 * NOTE: 
 * jvm.dll must be in the system PATH at runtime. 
 * CLASSPATH should include necessary jars
 */
int main(const int argc, char* argv[])
{
	try
	{
		StaticVmLoader loader(JNI_VERSION_1_6);
		OptionList list;
		
        vector<string>* pJvmArgs = getJvmArgsFromEnv();
        vector<string>::iterator it ;
        for(it = pJvmArgs->begin(); it!=pJvmArgs->end(); it++ ) {
    		list.push_back(jace::CustomOption(*it));
        }
        delete pJvmArgs;
		
		char * pClassPath = getenv("CLASSPATH");
		if(strlen(pClassPath)>1)
			list.push_back(jace::ClassPath(pClassPath));
		else
			list.push_back(jace::ClassPath("jace-runtime.jar"));
		jace::createVm(loader, list, false);
		JArray<String> args(argc);
		for (int i = 0; i < args.length(); ++i)
			args[i] = argv[i];		
		SilverKingClient::main(args);
		return 0;
	}
	catch (VirtualMachineShutdownError&)
	{
		cout << "The JVM was terminated in mid-execution. " << endl;
		return -2;
	}
	catch (JNIException& jniException)
	{
		cout << "An unexpected JNI error has occurred: " << jniException.what() << endl;
		return -2;
	}
	catch (Throwable& t)
	{
		t.printStackTrace();
		return -2;
	}
	catch (std::exception& e)
	{
		cout << "An unexpected C++ error has occurred: " << e.what() << endl;
		return -2;
	}
}
