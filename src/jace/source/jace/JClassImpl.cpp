#include "jace/JClassImpl.h"

#include "jace/Jace.h"

#include "jace/BoostWarningOff.h"
#include <boost/thread/mutex.hpp>
#include "jace/BoostWarningOn.h"

using std::string;

BEGIN_NAMESPACE(jace)


JClassImpl::JClassImpl(const string& _internalName, const string& _signature):
  internalName(_internalName), 
  signature(_signature),
	theClass(0)
{
	mutex = new boost::mutex();
}


JClassImpl::JClassImpl(const string& _internalName): 
  internalName(_internalName),
  signature("L" + internalName + ";"),
	theClass(0)
{
	mutex = new boost::mutex();
}
	
/**
 * Destroys this JClassImpl.
 */
JClassImpl::~JClassImpl() throw ()
{
	delete mutex;
	if (theClass)
	{
		if (!isRunning())
			return;

		JNIEnv* env = attach();
		deleteGlobalRef(env, theClass);
  }
}


const string& JClassImpl::getInternalName() const
{
  return internalName;
}

const string& JClassImpl::getSignature() const
{
  return signature;
}

/**
 * Returns the JNI representation of this class.
 */
jclass JClassImpl::getClass() const throw (JNIException)
{
	boost::mutex::scoped_lock lock(*mutex);
	if (theClass == 0)
	{
		JNIEnv* env = attach();

		jobject classLoader = getClassLoader();
		jclass localClass;

		if (classLoader != 0)
		{
			std::string binaryName(getInternalName());
			size_t i = 0;
			
			// Replace '/' by '.' in the name
			while (true)
			{
				i = binaryName.find('/', i);
				if (i != std::string::npos)
				{
					binaryName[i] = '.';
					++i;
				}
				else
					break;
			}
			jclass classLoaderClass = env->GetObjectClass(classLoader);
			jmethodID loadClass = env->GetMethodID(classLoaderClass, "loadClass", "(Ljava/lang/String;)Ljava/lang/Class;");
			if (loadClass == 0)
			{
				string msg = "JClass::getClass - Unable to find the method Jace::getClassLoader().loadClass()";
				try
				{
					catchAndThrow();
				}
				catch (JNIException& e)
				{
					msg.append("\ncaused by:\n");
					msg.append(e.what());
				}
				throw JNIException(msg);
			}
			jstring javaString = env->NewStringUTF(binaryName.c_str());
			localClass = static_cast<jclass>(env->CallObjectMethod(classLoader, loadClass, javaString));
			env->DeleteLocalRef(javaString);
		}
		else
			localClass = env->FindClass(getInternalName().c_str());

		if (!localClass)
		{
			string msg = "JClass::getClass - Unable to find the class <" + getInternalName() + ">";
			try
			{
				catchAndThrow();
			}
			catch (JNIException& e)
			{
				msg.append("\ncaused by:\n");
				msg.append(e.what());
			}
			throw JNIException(msg);
		}

		theClass = static_cast<jclass>(newGlobalRef(env, localClass));
		deleteLocalRef(env, localClass);
	}
	return theClass;
}

END_NAMESPACE(jace)
