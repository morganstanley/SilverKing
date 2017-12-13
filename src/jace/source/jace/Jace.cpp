#include "jace/Jace.h"

#include "jace/OsDep.h"
#include "jace/Namespace.h"

#include "jace/JFactory.h"
using jace::JFactory;

#include "jace/proxy/JObject.h"
using jace::proxy::JObject;

#include "jace/proxy/JValue.h"
using jace::proxy::JValue;

#include "jace/Peer.h"
using jace::Peer;

#include "jace/VmLoader.h"
using ::jace::VmLoader;

#include "jace/VirtualMachineShutdownError.h"
using jace::VirtualMachineShutdownError;

#include "jace/VirtualMachineRunningError.h"
using jace::VirtualMachineRunningError;

#include <cstdarg>
#include <stdlib.h>

#include <iostream>
using std::cerr;
using std::cout;
using std::endl;

#include <algorithm>
using std::copy;
using std::replace;

#include <vector>
using std::vector;

#include <map>
using std::map;

#include <string>
using std::string;
using std::wstring;

#include "jace/BoostWarningOff.h"
#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/recursive_mutex.hpp>
#include <boost/thread/tss.hpp>
#include <boost/shared_ptr.hpp>
#include "jace/BoostWarningOn.h"

#ifdef WIN32
#include <windows.h>
#else
#include <sys/types.h>
#include <sys/syscall.h>
#endif


BEGIN_NAMESPACE(jace)

// A reference to the java virtual machine.
// We're under the assumption that there will always only be one of these.
JavaVM* jvm = 0;
jint jniVersion = 0;
unsigned int threadNumber = 0;

/**
 * Synchronizes access to "jvm" and "jniVersion" variables.
 */
static boost::recursive_mutex jvmMutex;

// The map of all of the java class factories.
typedef map<string,JFactory*> FactoryMap;

FactoryMap* getFactoryMap()
{
  static FactoryMap factoryMap;
  return &factoryMap;
}

/**
 * Converts std::wstring to a modified UTF-8 std::string.
 *
 * Adaptation of u_strToJavaModifiedUTF8() found in ustrtrns.cpp in icu4c 4.9.1.1 package found at
 * http://site.icu-project.org/
 */
std::string toUTF8(const wstring& src)
{
	wchar_t ch = 0;
	size_t count;
	std::string result;

	// Faster loop without ongoing checking for pSrcLimit and pDestLimit.
	wstring::const_iterator i = src.begin();
	while (i != src.end())
	{
		count = result.length();
		if (*i <= 0x7f)
		{
			// fast ASCII loop
			while (i != src.end() && *i <= 0x7f && *i != 0)
			{
				result += (char) *i;
				++i;
				--count;
			}
		}

		// Each iteration of the inner loop progresses by at most 3 UTF-8
		// bytes and one UChar.
		count /= 3;
		if (i + count > src.end())
		{
			// min(remaining dest/3, remaining src)
			count = src.end() - i;
		}
		if(count < 3)
		{
			// Too much overhead if we get near the end of the string,
			// continue with the next loop.
			break;
		}
		do
		{
			ch = *i++;
			if (ch <= 0x7f && ch != 0)
				result += (char) ch;
			else if(ch <= 0x7ff)
			{
				result += (char)((ch>>6)|0xc0);
				result += (char)((ch&0x3f)|0x80);
			}
			else
			{
				result += (char)((ch>>12)|0xe0);
				result += (char)(((ch>>6)&0x3f)|0x80);
				result += (char)((ch&0x3f)|0x80);
			}
		} while(--count > 0);
	}

	while (i != src.end())
	{
		ch = *i++;
		if (ch <= 0x7f && ch != 0)
		{
			result += (char) ch;
		}
		else if(ch <= 0x7ff)
		{
			result += (char)((ch>>6)|0xc0);
			result += (char)((ch&0x3f)|0x80);
		}
		else
		{
			result += (char)((ch>>12)|0xe0);
			result += (char)(((ch>>6)&0x3f)|0x80);
			result += (char)((ch&0x3f)|0x80);
		}
	}
	return result;
}

/**
 * Converts a modified UTF-8 std::string to a std::wstring.
 *
 * Adaptation of u_strFromJavaModifiedUTF8WithSub() found in ustrtrns.cpp in icu4c 4.9.1.1 package
 * found at http://site.icu-project.org/
 */
wstring fromUTF8(const string& src)
{
	char ch;
  size_t count;
  char t1, t2; // trail bytes
  std::wstring result;

	// Faster loop without ongoing checking for pSrcLimit and pDestLimit.
	string::const_iterator i = src.begin();
	while (i != src.end())
	{
		count = result.length();
		if (*i <= 0x7f)
		{
			// fast ASCII loop
			while (i != src.end() && *i <= 0x7f)
			{
				result += (char) *i;
				++i;
				--count;
			}
		}
		// Each iteration of the inner loop progresses by at most 3 UTF-8
		// bytes and one char.
		count /= 3;
		if (i + count > src.end())
		{
			// min(remaining dest/3, remaining src)
			count = src.end() - i;
		}
		if (count < 3)
		{
			// Too much overhead if we get near the end of the string,
			// continue with the next loop.
			break;
		}
    do
		{
      ch = *i;
      if(ch <= 0x7f)
			{
        result += (char) ch;
        ++i;
      }
			else
			{
        if (ch >= 0xe0)
				{
					// handle U+0000..U+FFFF inline
					t1 = (char) (*(i + 1) - 0x80);
					t2 = (char) (*(i + 2) - 0x80);
					if (ch <= 0xef && t1 <= 0x3f && t2 <= 0x3f)
					{
						// no need for (ch & 0xf) because the upper bits are truncated after <<12 in the cast
						// to (char)
						result += (char) ((ch << 12) | (t1 << 6) | t2);
            i += 3;
						continue;
          }
        }
				else
				{
					// handle U+0000..U+07FF inline
					t1 = (char) (*(i + 1) - 0x80);
					if (ch >= 0xc0 && t1 <= 0x3f)
					{
						result += (char) (((ch & 0x1f) << 6) | t1);
            i += 2;
						continue;
          }
        }
        throw string("Invalid char found: ") + ch;
			}
    }
		while (--count > 0);
  }

	while (i != src.end())
	{
		ch = *i;
    if (ch <= 0x7f)
		{
			result += (char) ch;
      ++i;
    }
		else
		{
      if (ch >= 0xe0)
			{
				// handle U+0000..U+FFFF inline
				t1 = (char) (*(i + 1) - 0x80);
				t2 = (char) (*(i + 2) - 0x80);
        if (ch <= 0xef && ((src.end() - i) >= 3) &&
            t1 <= 0x3f && t2 <= 0x3f)
				{
					// no need for (ch & 0xf) because the upper bits are truncated after <<12 in the cast to
					// char
					result += (char) ((ch << 12) | (t1 << 6) | t2);
          i += 3;
					continue;
        }
      }
			else
			{
				// handle U+0000..U+07FF inline
				t1 = (char) (*(i + 1) - 0x80);
				if (ch >= 0xc0 && ((src.end() - i) >= 2) &&
            t1 <= 0x3f)
				{
					result += (char) (((ch & 0x1f) << 6) | t1);
          i += 2;
          continue;
        }
      }
      throw string("Invalid char found: ") + ch;
		}
  }

  // do not fill the dest buffer just count the char needed
  while (i != src.end())
	{
		ch = *i;
    if (ch <= 0x7f)
	    ++i;
  	else
		{
			if (ch >= 0xe0)
			{
				// handle U+0000..U+FFFF inline
				if (ch <= 0xef && ((src.end() - i) >= 3) &&
            (char) (*(i + 1) - 0x80) <= 0x3f &&
            (char) (*(i + 2) - 0x80) <= 0x3f)
				{
					i += 3;
          continue;
        }
      }
			else
			{
				// handle U+0000..U+07FF inline
				if (ch >= 0xc0 && ((src.end() - i) >= 2) &&
            (char) (*(i + 1) - 0x80) <= 0x3f)
				{
					i += 2;
          continue;
        }
      }
      throw string("Invalid char found: ") + ch;
    }
  }
	return result;
}

/**
 * Converts std::wstring to a std::string encoded using the default platform encoding.
 *
 * REFERENCE: http://stackoverflow.com/questions/4804298/c-how-to-convert-wstring-into-string
 */
std::string toPlatformEncoding(const std::wstring& src)
{
	const std::locale locale("");
  typedef std::codecvt<wchar_t, char, std::mbstate_t> converter_type;
  const converter_type& converter = std::use_facet<converter_type>(locale);
  std::vector<char> target(src.length() * converter.max_length());
  std::mbstate_t state;
  const wchar_t* from_next;
  char* target_next;
  const converter_type::result result = converter.out(state, src.data(), src.data() + src.length(), 
		from_next, &target[0], &target[0] + target.size(), target_next);
  if (result == converter_type::ok || result == converter_type::noconv)
		return std::string(&target[0], target_next);
	throw wstring(L"Failed to convert wstring: ") + src;
}

std::string asString(JNIEnv* env, jstring str)
{
  const char* utfString = env->GetStringUTFChars(str, 0);
  if (!utfString)
	{
    std::string msg = "Unable to retrieve the character string for an exception message.";
    throw JNIException(msg);
  }
  std::string stdString = utfString;
  env->ReleaseStringUTFChars(str, utfString);
  return stdString;
}

/**
 * Implementation of catchAndThrow() using a specific JNIEnv.
 */
void catchAndThrow(JNIEnv* env)
{
  if (!env->ExceptionCheck())
    return;

  jthrowable jexception = env->ExceptionOccurred();

  // cout << "jace::catchAndThrow() - Discovered an exception: " << endl;
  // print(jexception);

  env->ExceptionClear();

  // Find the fully qualified name for the exception type, so
  // we can find a matching C++ proxy exception.
  //
  // In java, this looks like:
  //   String typeName = exception.getClass().getName();

  //  cout << "jace::catchAndThrow() - Retrieving the exception class type..." << endl;
  jclass throwableClass = env->FindClass("java/lang/Throwable");

  if (!throwableClass)
	{
    string msg = "Assert failed: Unable to find the class, java.lang.Throwable.";
    throw JNIException(msg);
  }

  jclass classClass = env->FindClass("java/lang/Class");

  if (!classClass)
	{
    string msg = "Assert failed: Unable to find the class, java.lang.Class.";
    throw JNIException(msg);
  }

  jmethodID throwableGetClass = env->GetMethodID(throwableClass, "getClass", "()Ljava/lang/Class;");

  if (!throwableGetClass)
	{
    string msg = "Assert failed: Unable to find the method, Throwable.getClass().";
    throw JNIException(msg);
  }

  deleteLocalRef(env, throwableClass);

  jmethodID classGetName = env->GetMethodID(classClass, "getName", "()Ljava/lang/String;");

  if (!classGetName)
	{
    string msg = "Assert failed: Unable to find the method, Class.getName().";
    throw JNIException(msg);
  }

  jmethodID classGetSuperclass = env->GetMethodID(classClass, "getSuperclass", "()Ljava/lang/Class;");

  if (!classGetSuperclass)
	{
    string msg = "Assert failed: Unable to find the method, Class.getSuperclass().";
    throw JNIException(msg);
  }

  deleteLocalRef(env, classClass);

  jobject exceptionClass = env->CallObjectMethod(jexception, throwableGetClass);

  if (env->ExceptionOccurred())
	{
    env->ExceptionDescribe();
    string msg = string("jace::catchAndThrow()\n") +
                 "An error occurred while trying to call getClass() on the thrown exception.";
    throw JNIException(msg);
  }

  jstring exceptionType = static_cast<jstring>(env->CallObjectMethod(exceptionClass, classGetName));

  if (env->ExceptionOccurred())
	{
    env->ExceptionDescribe();
    string msg = string("jace::catchAndThrow()\n") +
                 "An error occurred while trying to call getName() on the class of the thrown exception.";
    throw JNIException(msg);
  }

  string exceptionTypeString = asString(env, exceptionType);

  // Now, find the matching factory for this exception type.
  while (true)
	{
    FactoryMap::iterator it = getFactoryMap()->find(exceptionTypeString);

    // If we couldn't find a match, try to find the parent exception type.
    if (it == getFactoryMap()->end())
		{
      // cout << "Finding super class for " << endl;
      // print(exceptionClass);

      jobject superClass = env->CallObjectMethod(exceptionClass, classGetSuperclass);

      if (env->ExceptionOccurred())
			{
        env->ExceptionDescribe();
        string msg = string("jace::catchAndThrow()\n") +
                     "An error occurred while trying to call getSuperclass() on the thrown exception.";
        throw JNIException(msg);
      }

      // We get NULL if we've already reached java.lang.Object, in which case,
      // we couldn't find any match at all.
      if (!superClass)
        break;

      deleteLocalRef(env, exceptionClass);
      deleteLocalRef(env, exceptionType);
      exceptionClass = superClass;

      exceptionType = static_cast<jstring>(env->CallObjectMethod(exceptionClass, classGetName));

      if (env->ExceptionOccurred())
			{
        env->ExceptionDescribe();
        throw JNIException("jace::catchAndThrow()\nAn error occurred while trying to call "
					"getName() on the superclass of the thrown exception.");
      }

      exceptionTypeString = asString(env, exceptionType);
			if (exceptionTypeString == "java.lang.Object")
			{
			  // Couldn't find a matching exception. Abort!
				break;
			}
      continue;
    }

    // Ask the factory to throw the exception.
    // cout << "jace::catchAndThrow() - Throwing the exception " << endl;
    // print(jexception);

    jvalue value;
    value.l = jexception;
    it->second->throwInstance(value);
  }

	exceptionClass = env->CallObjectMethod(jexception, throwableGetClass);

  if (env->ExceptionOccurred())
	{
    env->ExceptionDescribe();
    string msg = string("jace::catchAndThrow()\n") +
                 "An error occurred while trying to call getClass() on the thrown exception.";
    throw JNIException(msg);
  }

  exceptionType = static_cast<jstring>(env->CallObjectMethod(exceptionClass, classGetName));

  if (env->ExceptionOccurred())
	{
    env->ExceptionDescribe();
    string msg = string("jace::catchAndThrow()\n") +
                 "An error occurred while trying to call getName() on the class of the thrown exception.";
    throw JNIException(msg);
  }

  exceptionTypeString = asString(env, exceptionType);
  //    cout << "Unable to find an enlisted class factory matching the type <" + exceptionTypeString + ">" << endl;
  //    cout << "Throwing Exception instead." << endl;
  string msg = string("Can't find any linked in parent exception for ") + exceptionTypeString + "\n";
  throw JNIException(msg);
}

void registerShutdownHook(JNIEnv *env) throw (JNIException)
{
  jclass hookClass = env->FindClass("org/jace/util/ShutdownHook");
  if (!hookClass)
	{
    string msg = "Assert failed: Unable to find the class, org.jace.util.ShutdownHook.";
    throw JNIException(msg);
  }

  jmethodID hookGetInstance = env->GetStaticMethodID(hookClass, "getInstance", "()Lorg/jace/util/ShutdownHook;");
  if (!hookGetInstance)
	{
		env->DeleteLocalRef(hookClass);
    string msg = "Assert failed: Unable to find the method, ShutdownHook.getInstance().";
    throw JNIException(msg);
  }

	jobject hookObject = env->CallStaticObjectMethod(hookClass, hookGetInstance);
	if (!hookObject)
	{
		env->DeleteLocalRef(hookClass);
    string msg = "Unable to invoke ShutdownHook.getInstance()";
		try
		{
			catchAndThrow(env);
		}
		catch (std::exception& e)
		{
			msg.append("\ncaused by:\n");
			msg.append(e.what());
		}
    throw JNIException(msg);
	}

  jmethodID hookRegisterIfNecessary = env->GetMethodID(hookClass, "registerIfNecessary", "()V");
  if (!hookRegisterIfNecessary)
	{
		env->DeleteLocalRef(hookObject);
		env->DeleteLocalRef(hookClass);
		throw JNIException("Unable to find the method, ShutdownHook.registerIfNecessary().");
  }

	env->CallObjectMethodA(hookObject, hookRegisterIfNecessary, 0);
	try
	{
		catchAndThrow(env);
	}
	catch (std::exception& e)
	{
		string msg = "Exception thrown invoking ShutdownHook.registerIfNecessary()\n";
		msg.append("caused by:\n");
		msg.append(e.what());
		throw JNIException(msg);
	}
	env->DeleteLocalRef(hookObject);
	env->DeleteLocalRef(hookClass);
}

/**
 * Attaches the current thread to the virtual machine and returns the appropriate
 * JNIEnv for the thread. If the thread is already attached, this method method
 * does nothing.
 *
 * PRECONDITION: jvm is not null and jvmMutex is locked
 *
 * @param jvm the java virtual machine to attach the thread to
 * @param threadGroup the ThreadGroup associated with the thread, or null
 * @param name the thread name, or null
 * @param daemon true if the thread should be attached as a daemon thread
 * @throws JNIException if an error occurs while trying to attach the current thread.
 * @see AttachCurrentThread
 * @see AttachCurrentThreadAsDaemon
 */
JNIEnv* attachImpl(JavaVM* jvm, const jobject threadGroup, const char* name, const bool daemon) throw (JNIException)
{
	JNIEnv* env;
	if (jvm->GetEnv((void**) &env, jniVersion) == JNI_OK)
	{
		// Already attached
		return env;
	}

	JavaVMAttachArgs args = {0};
	args.version = jniVersion;
	if (name != 0)
	{
		args.name = new char[strlen(name)+1];
		strcpy(args.name, name);
	}
	else
	{
		string temp("NativeThread-");
		temp += toString(getCurrentThreadId());
		args.name = new char[temp.length() + 1];
		strcpy(args.name, temp.c_str());
	}
	args.group = threadGroup;
  jint result;
	if (!daemon)
		result = jvm->AttachCurrentThread(reinterpret_cast<void**>(&env), &args);
	else
		result = jvm->AttachCurrentThreadAsDaemon(reinterpret_cast<void**>(&env), &args);
	delete[] args.name;

  if (result != 0)
	{
    string msg = string("Jace::attach\n") +
                 "Unable to attach the current thread. The specific JNI error code is " +
                 toString(result);
    throw JNIException(msg);
  }
  return env;
}

void classLoaderDestructor(jobject* value)
{
	// Invoked by setClassLoader() or when the thread exits
	if (value == 0)
		return;

	// Read the thread state
	boost::recursive_mutex::scoped_lock lock(jvmMutex);
	if (jvm == 0)
	{
		// JVM is already shut down
		return;
	}
	JNIEnv* env;
	bool isDetached = jvm->GetEnv((void**) &env, jniVersion) == JNI_EDETACHED;
	
	if (isDetached)
		env = attachImpl(jvm, 0, 0, false);
	else
		assert(false);
	env->DeleteGlobalRef(*value);
	delete[] value;

	// Restore the thread state
	if (isDetached)
		detach();
}

boost::thread_specific_ptr<jobject> threadClassLoader(classLoaderDestructor);

/**
 * Allows createVm() and setJavaVm() to share code without recursive mutexes.
 *
 * PRECONDITION: _jvm is not null and jvmMutex is locked
 */
void setJavaVmImpl(JavaVM* _jvm) throw (JNIException)
{
	assert(_jvm != 0);
	JNIEnv* env = attachImpl(_jvm, 0, 0, false);
	registerShutdownHook(env);
	jvm = _jvm;
	jniVersion = env->GetVersion();
}

void createVm(const VmLoader& loader,
              const OptionList& options,
              bool ignoreUnrecognized) throw (JNIException)
{
	JavaVM* jvm;
  JNIEnv* env;
  JavaVMInitArgs vm_args;
  JavaVMOption* jniOptions = options.createJniOptions();

  vm_args.version = loader.getJniVersion();
  vm_args.options = jniOptions;
  vm_args.nOptions = jint(options.size());
  vm_args.ignoreUnrecognized = ignoreUnrecognized;

	boost::recursive_mutex::scoped_lock lock(jvmMutex);
  jint rc = loader.createJavaVM(&jvm, reinterpret_cast<void**>(&env), &vm_args);
  options.destroyJniOptions(jniOptions);

  if (rc != 0)
	{
    string msg = "Unable to create the virtual machine. The error was " + toString(rc);
    throw JNIException(msg);
  }
	setJavaVmImpl(jvm);
}

/**
 * Invoked by org.jace.util.ShutdownHook on VM shutdown.
 */
extern "C" JNIEXPORT void JNICALL Java_org_jace_util_ShutdownHook_signalVMShutdown(JNIEnv*, jclass)
{
	// Invoking DestroyJavaVM() from multiple threads will result in a deadlock (they will wait on each other to shut down).
	// Typically the main thread is blocked on DestroyJavaVM() and the shutdown hook is invoked
	// on another thread. As such, we reset jvm and jniVersion directly, without invoking DestroyJavaVM().
	boost::recursive_mutex::scoped_lock lock(jvmMutex);

	// Currently (JDK 1.7) JVM unloading is not supported. We do our best to ensure that the JVM
	// is not used past this point.
	jvm = 0;
	jniVersion = 0;
}


void destroyVm() throw (JNIException)
{
	jint jniVersionBeforeShutdown;
	JavaVM* jvmBeforeShutdown;
	{
		boost::recursive_mutex::scoped_lock lock(jvmMutex);
		if (jvm == 0)
		{
			// JVM already shut down
			return;
		}
		jniVersionBeforeShutdown = jniVersion;
		jvmBeforeShutdown = jvm;
	}

	// DestroyJavaVM()'s return value is only reliable under JDK 1.6 or newer; older versions always
	// return failure.
	//
	// NOTE: DestroyJavaVM() will block until the shutdown hook finishes executing
	jint result = jvmBeforeShutdown->DestroyJavaVM();
	if (jniVersionBeforeShutdown >= JNI_VERSION_1_6 && result != JNI_OK)
		throw JNIException("DestroyJavaVM() returned " + toString(result));
}


/**
 * Attaches the current thread to the virtual machine and returns the appropriate
 * JNIEnv for the thread. If the thread is already attached, this method method
 * does nothing.
 *
 * This method is equivilent to attach(0, 0, false).
 *
 * @throws JNIException if an error occurs while trying to attach the current thread.
 * @throws VirtualMachineShutdownError if the virtual machine is shut down
 * @see AttachCurrentThread
 * @see attach(const jobject, const char*, const bool)
 */
JNIEnv* attach() throw (JNIException, VirtualMachineShutdownError)
{
	return attach(0, 0, false);
}

/**
 * Attaches the current thread to the virtual machine and returns the appropriate
 * JNIEnv for the thread. If the thread is already attached, this method method
 * does nothing.
 *
 * @param threadGroup the ThreadGroup associated with the thread, or null
 * @param name the thread name, or null
 * @param daemon true if the thread should be attached as a daemon thread
 * @throws JNIException if an error occurs while trying to attach the current thread.
 * @throws VirtualMachineShutdownError if the virtual machine is shut down
 * @see AttachCurrentThread
 * @see AttachCurrentThreadAsDaemon
 */
JNIEnv* attach(const jobject threadGroup, const char* name, const bool daemon) throw (JNIException, VirtualMachineShutdownError)
{
	boost::recursive_mutex::scoped_lock lock(jvmMutex);
	if (jvm == 0)
		throw VirtualMachineShutdownError("The virtual machine is shut down");
	return attachImpl(jvm, threadGroup, name, daemon);
}

/**
 * Detaches the current thread from the virtual machine.
 */
void detach() throw ()
{
	boost::recursive_mutex::scoped_lock lock(jvmMutex);
	if (jvm == 0)
	{
		// The JVM is already shut down
		return;
	}
  jvm->DetachCurrentThread();
}


/**
 * Enlists a new factory for a java class with Jace.
 *
 * All java exception classes should enlist with Jace
 * on start-up. They can do this by adding a static member variable
 * of type JEnlister to their class definition.
 *
 * For example, java::lang::Object has a static member variable,
 *
 *   static JEnlister<Object> enlister;
 *
 * which is all that is required to register a new factory
 * for itself.
 */
void enlist(JFactory* factory)
{
  string name = factory->getClass().getInternalName();
  replace(name.begin(), name.end(), '/', '.');
  getFactoryMap()->insert(FactoryMap::value_type(name, factory));
  //  cout << "jace::enlist - Enlisted " << name << endl;
}


jobject newLocalRef(JNIEnv* env, jobject ref) throw (JNIException)
{
  jobject localRef = env->NewLocalRef(ref);
  if (!localRef)
	{
    throw JNIException(string("Jace::newLocalRef\n") +
                 "Unable to create a new local reference.\n" +
                 "It is likely that you have exceeded the maximum local reference count.\n" +
                 "You can increase the maximum count with a call to EnsureLocalCapacity().");
	}
  return localRef;
}

void deleteLocalRef(JNIEnv* env, jobject localRef)
{
  env->DeleteLocalRef(localRef);
}

jobject newGlobalRef(JNIEnv* env, jobject ref) throw (VirtualMachineShutdownError, JNIException)
{
  jobject globalRef = env->NewGlobalRef(ref);
  if (!globalRef)
	{
    throw JNIException(string("Jace::newGlobalRef\n") +
                 "Unable to create a new global reference.\n" +
                 "It is likely that you have exceeded the max heap size of your virtual machine.");
  }
  return globalRef;
}

void deleteGlobalRef(JNIEnv* env, jobject globalRef)
{
  env->DeleteGlobalRef(globalRef);
}

/**
 * Checks for a java exception.
 *
 * If a java exception has been thrown, the java exception is cleared,
 * and a corresponding C++ proxy exception is thrown.
 *
 * @internal We don't want to put a throw specification on this, because
 * it could throw any range of exceptions.
 *
 */
void catchAndThrow()
{
	catchAndThrow(attach());
}

::jace::Peer* getPeer(jobject jPeer)
{
  JNIEnv* env = attach();

  jclass peerClass = env->GetObjectClass(jPeer);
  jmethodID handleID = env->GetMethodID(peerClass, "jaceGetNativeHandle", "()J");

  if (!handleID)
	{
    string msg = "Unable to locate the method, \"jaceGetNativeHandle\".\n" \
                 "The class has not been properly enhanced.";
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

  jlong nativeHandle = env->CallLongMethod(jPeer, handleID);
  catchAndThrow();

  return reinterpret_cast< ::jace::Peer* >(nativeHandle);
}

JavaVM* getJavaVm()
{
  return jvm;
}

void setJavaVm(JavaVM* _jvm) throw (VirtualMachineRunningError, JNIException)
{
	if (_jvm == 0)
		throw new JNIException("jvm may not be null");
	boost::recursive_mutex::scoped_lock lock(jvmMutex);
  if (jvm != 0)
    throw VirtualMachineRunningError("The virtual machine is already running");
	setJavaVmImpl(_jvm);
}

/**
 * Returns the ClassLoader being used by the current thread.
 *
 */
jobject getClassLoader()
{
	jobject* value = threadClassLoader.get();
	if (value == 0)
		return 0;
	return value[0];
}

void setClassLoader(jobject classLoader)
{
	JNIEnv* env = attach();

	// boost::thread_specific_ptr can only store a pointer to jobject, but someone needs to keep
	// the underlying jobject alive so we use a dynamically-allocated array.
	jobject* ptr = new jobject[1];
	if (classLoader != 0)
		ptr[0] = newGlobalRef(env, classLoader);
	else
		ptr[0] = 0;
	try
	{
		threadClassLoader.reset(ptr);
	}
	catch (boost::thread_resource_error& e)
	{
		throw JNIException(e.what());
	}
}

string toString(jobject obj)
{
  JNIEnv* env = attach();
  jclass objectClass = env->FindClass("java/lang/Object");

  if (!objectClass)
	{
    string msg = "Assert failed: Unable to find the class, java.lang.Object.";
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

  jmethodID toString = env->GetMethodID(objectClass, "toString", "()Ljava/lang/String;");
  if (!toString)
	{
    string msg = "Assert failed: Unable to find the method, Object.toString().";
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

  jstring javaStr = static_cast<jstring>(env->CallObjectMethod(obj, toString));
  const char* strBuf = env->GetStringUTFChars(javaStr, 0);
  string value = string(strBuf);

  env->ReleaseStringUTFChars(javaStr, strBuf);

  deleteLocalRef(env, javaStr);
  deleteLocalRef(env, objectClass);

  return value;
}

void print(jobject obj)
{
  cout << toString(obj) << endl;
}

void printClass(jobject obj)
{
  JNIEnv* env = attach();
  jclass objClass = env->GetObjectClass(obj);
  print(objClass);
  deleteLocalRef(env, objClass);
}

bool isRunning()
{
	boost::recursive_mutex::scoped_lock lock(jvmMutex);
  return jvm != 0;
}

string getCurrentThreadId()
{
#ifdef _WIN32
  return toString(GetCurrentThreadId());
#else
  return toString(syscall(SYS_gettid));
#endif
}

END_NAMESPACE(jace)
