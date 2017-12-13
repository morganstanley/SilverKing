#ifndef JACE_JACE_H
#define JACE_JACE_H

#include "jace/OsDep.h"
#include "jace/Namespace.h"
#include "jace/JNIException.h"
#include "jace/VirtualMachineRunningError.h"
#include "jace/VirtualMachineShutdownError.h"
#include "jace/JFactory.h"
#include "jace/VmLoader.h"
#include "jace/OptionList.h"
#include "jace/JClass.h"
#include "jace/proxy/JObject.h"

#ifdef SUPPORTS_SSTREAM
  #include <sstream>
#else
  #include <strstream>
#endif


BEGIN_NAMESPACE(jace)
class Peer;
END_NAMESPACE(jace)

BEGIN_NAMESPACE_2(jace, proxy)
class JValue;
END_NAMESPACE_2(jace, proxy)


#include <jni.h>

/**
 * Jace helper functions.
 *
 * @author Toby Reyelts
 * @author Gili Tzabari
 */

BEGIN_NAMESPACE(jace)

/**
 * Creates a new Java Virtual Machine using the specified loader
 * with the specified options.
 *
 * To link with a virtual machine you may specify any dynamic loading
 * VmLoader (for example, UnixVmLoader or Win32VmLoader).
 *
 * This call results in a call to setVmLoader internally.
 * @throws VirtualMachineRunningError if the virtual machine is already running
 * @throws JNIException if the virtual machine cannot be created
 */
JACE_API void createVm(const VmLoader& loader,
											 const OptionList& options,
											 bool ignoreUnrecognized = true)
											 throw (JNIException);

/**
 * Destroys the current Java Virtual Machine and tells Jace that it
 * shouldn't try to re-attach any more threads.
 * After calling this function, most other functions will fail.
 *
 * If the current thread is attached, the VM waits until the current
 * thread is the only non-daemon user-level Java thread. If the
 * current thread is not attached, the VM attaches the current thread
 * and then waits until the current thread is the only non-daemon
 * user-level thread. The JDK/JRE still does not support VM unloading, however.
 *
 * @see http://java.sun.com/javase/6/docs/technotes/guides/jni/spec/invocation.html#destroy_java_vm
 * @throws JNIException if the virtual machine fails to shut down
 */
JACE_API void destroyVm() throw (JNIException);

/**
 * Sets the current running java virtual machine. This method can be used to implement a custom vm
 * loading policy outside of createVm.
 *
 * @param jvm a running java virtual machine
 * @throws VirtualMachineRunningError if a JVM is already running
 * @throws JNIException if an error occurs while registering the shutdown hook
 */
JACE_API void setJavaVm(JavaVM* jvm) throw(VirtualMachineRunningError, JNIException);

/**
 * Returns the current java virtual machine.
 *
 * @return null if no virtual machine is running
 */
JACE_API JavaVM* getJavaVm();


/**
 * Attaches the current thread to the virtual machine and returns the appropriate
 * JNIEnv for the thread. If the thread is already attached, this method method
 * does nothing.
 *
 * This method is equivilent to attach(0, 0, false).
 *
 * @throws JNIException if an error occurs while trying to attach the current thread.
 * @see AttachCurrentThread
 * @see attach(const jobject, const char*, const bool)
 * @throws JNIException if an error occurs while attaching the current thread
 * @throws VirtualMachineShutdownError if the virtual machine is not running
 */
JACE_API JNIEnv* attach() throw (JNIException, VirtualMachineShutdownError);


/**
 * Attaches the current thread to the virtual machine
 * and returns the appropriate JNIEnv for the thread.
 * If the thread is already attached, this method method does nothing.
 *
 * @param threadGroup the ThreadGroup associated with the thread, or null
 * @param name the thread name as a modified UTF-8 string, or null
 * @param daemon true if the thread should be attached as a daemon thread
 * @see AttachCurrentThread
 * @see AttachCurrentThreadAsDaemon
 * @see http://en.wikipedia.org/wiki/UTF-8#Modified_UTF-8
 * @throws JNIException if an error occurs while trying to attach the current thread.
 * @throws VirtualMachineShutdownError if the virtual machine is not running
 */
JACE_API JNIEnv* attach(const jobject threadGroup, const char* name, const bool daemon)
	throw (JNIException, VirtualMachineShutdownError);


/**
 * Detaches the current thread from the virtual machine.
 *
 * @see DetachCurrentThread
 */
JACE_API void detach() throw ();

/**
 * A central point for allocating new local references.
 * These references must be deallocated by a call to deleteLocalRef.
 *
 * @throws JNIException if the local reference can not be allocated.
 */
JACE_API jobject newLocalRef(JNIEnv* env, jobject ref) throw (JNIException);


/**
 * A central point for deleting local references.
 */
JACE_API void deleteLocalRef(JNIEnv* env, jobject localRef);


/**
 * A central point for allocating new global references.
 * These references must be deallocated by a call to deleteGlobalRef.
 *
 * @throws VirtualMachineShutdownError if the virtual machine is not running
 * @throws JNIException if the global reference can not be allocated.
 */
JACE_API jobject newGlobalRef(JNIEnv* env, jobject ref) throw (VirtualMachineShutdownError, JNIException);


/**
 * A central point for deleting global references.
 */
JACE_API void deleteGlobalRef(JNIEnv* env, jobject globalRef);


/**
 * Enlists a new factory for a java class with Jace.
 *
 * All java classes should enlist with Jace on start-up.
 * They can do this by adding a static member variable
 * of type JEnlister to their class definition.
 *
 * For example, java::lang::Object has a static member variable,
 *
 *   static JEnlister<Object> enlister;
 *
 * which is all that is required to register a new factory
 * for itself.
 */
JACE_API void enlist(JFactory* factory);


/**
 * Checks to see if a java exception has been thrown.
 *
 * If an exception has been thrown, a corresponding C++ proxy
 * exception is constructed and thrown.
 */
JACE_API void catchAndThrow();

/**
 * Returns the Peer for a given java Peer.
 */
JACE_API Peer* getPeer(jobject jPeer);

/**
 * Returns the ClassLoader being used by the current thread.
 *
 */
JACE_API jobject getClassLoader();

/**
 * Sets the ClassLoader to be used by the current thread.
 *
 * By default, Jace uses the JNIEnv->FindClass() to load classes,
 * but if a thread ClassLoader is defined then it is used to load
 * classes instead. A thread ClassLoader must be defined under
 * Java Webstart, Applets or any other framework that makes use
 * of custom ClassLoaders to load classes.
 *
 * NOTE: You must setClassLoader(0) to release the ClassLoader
 *       reference or detach() will do it for you on thread shutdown.
 */
JACE_API void setClassLoader(jobject classLoader);

/**
 * Returns the string representation of any type.
 */
template <class T> std::string toString(T value)
{
#ifdef SUPPORTS_SSTREAM
		std::stringstream stream;
#else
		std::strstream stream;
#endif
		stream << value;
		return stream.str();
}

/**
 * Returns the current thread id.
 */
JACE_API std::string getCurrentThreadId();

/**
 * Converts std::wstring to a modified UTF-8 std::string.
 */
JACE_API std::string toUTF8(const std::wstring& src);

/**
 * Converts a modified UTF-8 std::string to a std::wstring.
 */
JACE_API std::wstring fromUTF8(const std::string& src);

/**
 * Converts std::wstring to a std::string encoded using the default platform encoding.
 */
std::string toPlatformEncoding(const std::wstring& src);

/**
 * Returns the result of calling Object.toString() on obj.
 * Useful for low level debugging.
 */
JACE_API std::string toString(jobject obj);

/**
 * Indicates if Java Virtual Machine is running.
 */
JACE_API bool isRunning();

/**
 * Instantiate a new Java object.
 *
 * For example,
 *
 *  Map map = java_new<HashMap>();
 */
template <typename T>
T java_new()
{
	return T::Factory::create();
}

/**
 * Instantiate a new Java object.
 *
 * For example,
 *
 *  Map map = java_new<HashMap>(42);
 */
template <typename T, typename A0>
T java_new(A0 a0)
{
	return T::Factory::create(a0);
}

/**
 * Instantiate a new Java object.
 *
 * For example,
 *
 *  Map map = java_new<HashMap>(a0, a1);
 */
template <typename T, typename A0, typename A1>
T java_new(A0 a0, A1 a1)
{
	return T::Factory::create(a0, a1);
}

/**
 * Instantiate a new Java object.
 *
 * For example,
 *
 *  Map map = java_new<HashMap>(a0, a1, a2);
 */
template <typename T, typename A0, typename A1, typename A2>
T java_new(A0 a0, A1 a1, A2 a2)
{
	return T::Factory::create(a0, a1, a2);
}

/**
 * Instantiate a new Java object.
 *
 * For example,
 *
 *  Map map = java_new<HashMap>(a0, a1, a2, a3);
 */
template <typename T, typename A0, typename A1, typename A2, typename A3>
T java_new(A0 a0, A1 a1, A2 a2, A3 a3)
{
	return T::Factory::create(a0, a1, a2, a3);
}

/**
 * Instantiate a new Java object.
 *
 * For example,
 *
 *  Map map = java_new<HashMap>(a0, a1, a2, a3, a4);
 */
template <typename T, typename A0, typename A1, typename A2, typename A3, typename A4>
T java_new(A0 a0, A1 a1, A2 a2, A3 a3, A4 a4)
{
	return T::Factory::create(a0, a1, a2, a3, a4);
}

/**
 * Instantiate a new Java object.
 *
 * For example,
 *
 *  Map map = java_new<HashMap>(a0, a1, a2, a3, a4, a5);
 */
template <typename T, typename A0, typename A1, typename A2, typename A3, typename A4, typename A5>
T java_new(A0 a0, A1 a1, A2 a2, A3 a3, A4 a4, A5 a5)
{
	return T::Factory::create(a0, a1, a2, a3, a4, a5);
}

/**
 * Instantiate a new Java object.
 *
 * For example,
 *
 *  Map map = java_new<HashMap>(a0, a1, a2, a3, a4, a5, a6);
 */
template <typename T, typename A0, typename A1, typename A2, typename A3, typename A4, typename A5,
          typename A6>
T java_new(A0 a0, A1 a1, A2 a2, A3 a3, A4 a4, A5 a5, A6 a6)
{
	return T::Factory::create(a0, a1, a2, a3, a4, a5, a6);
}

/**
 * Instantiate a new Java object.
 *
 * For example,
 *
 *  Map map = java_new<HashMap>(a0, a1, a2, a3, a4, a5, a6, a7);
 */
template <typename T, typename A0, typename A1, typename A2, typename A3, typename A4, typename A5,
          typename A6, typename A7>
T java_new(A0 a0, A1 a1, A2 a2, A3 a3, A4 a4, A5 a5, A6 a6, A7 a7)
{
	return T::Factory::create(a0, a1, a2, a3, a4, a5, a6, a7);
}

/**
 * Instantiate a new Java object.
 *
 * For example,
 *
 *  Map map = java_new<HashMap>(a0, a1, a2, a3, a4, a5, a6, a7, a8);
 */
template <typename T, typename A0, typename A1, typename A2, typename A3, typename A4, typename A5,
          typename A6, typename A7, typename A8>
T java_new(A0 a0, A1 a1, A2 a2, A3 a3, A4 a4, A5 a5, A6 a6, A7 a7, A8 a8)
{
	return T::Factory::create(a0, a1, a2, a3, a4, a5, a6, a7, a8);
}

/**
 * Instantiate a new Java object.
 *
 * For example,
 *
 *  Map map = java_new<HashMap>(a0, a1, a2, a3, a4, a5, a6, a7, a8, a9);
 */
template <typename T, typename A0, typename A1, typename A2, typename A3, typename A4, typename A5,
          typename A6, typename A7, typename A8, typename A9>
T java_new(A0 a0, A1 a1, A2 a2, A3 a3, A4 a4, A5 a5, A6 a6, A7 a7, A8 a8, A9 a9)
{
	return T::Factory::create(a0, a1, a2, a3, a4, a5, a6, a7, a8, a9);
}

/**
 * Instantiate a new String.
 *
 * For example,
 *
 *  String map = java_new<String>("Hello");
 *
 * The compiler can't figure out how to go from a char* to JValue on its own.
 */
template <typename T>
T java_new(const char* text)
{
	return T::Factory::create(text);
}

/**
 * Instantiate a new String.
 *
 * For example,
 *
 *  String map = java_new<String>(::std::string("Hello"));
 *
 * The compiler can't figure out how to go from a char* to JValue on its own.
 */
template <typename T>
T java_new(const ::std::string& text)
{
	return T::Factory::create(text);
}

/**
 * Performs a safe cast from one JObject subclass to another.
 *
 * For example,
 *
 *  Object stringAsObject = String("Hello");
 *  String string = java_cast<String>(stringAsObject);
 *
 * @throws JNIException if obj is not convertible to type T.
 */
template <typename T> T java_cast(const ::jace::proxy::JObject& obj)
{
	JNIEnv* env = attach();
	jclass argClass = env->GetObjectClass(obj);

	if (!argClass)
	{
		std::string msg = "[java_cast] Failed to retrieve the class type for obj.";
		throw JNIException(msg);
	}

	const ::jace::JClass& resultClass = T::staticGetJavaJniClass();

	bool isValid = env->IsAssignableFrom(argClass, resultClass.getClass());
	env->DeleteLocalRef(argClass);

	if (isValid)
		return T(static_cast<jobject>(obj));

	std::string msg = "Can not cast to " + resultClass.getInternalName();
	throw JNIException(msg);
}

/**
 * Equal to Java's instanceof keyword.
 * Returns true if obj is non-null and can be cast to type T.
 *
 * For example,
 *
 *  Object stringAsObject = String("Hello");
 *
 *  if (instanceof<String>(stringAsObject))
 *    String str = java_cast<String>(stringAsObject);
 *
 *
 * @throws JNIException if obj is not convertible to type T.
 */
template <typename T> bool instanceof(const ::jace::proxy::JObject& object)
{
	if (object.isNull())
		return false;

	JNIEnv* env = attach();
	jclass argClass = env->GetObjectClass(object);

	if (!argClass)
	{
		std::string msg = "[instanceof] Failed to retrieve the dynamic class type for object.";
		throw JNIException(msg);
	}

	const ::jace::JClass& resultClass = T::staticGetJavaJniClass();

	bool isValid = env->IsAssignableFrom(argClass, resultClass.getClass());
	env->DeleteLocalRef(argClass);

	return isValid;
}

END_NAMESPACE(jace)

#endif
