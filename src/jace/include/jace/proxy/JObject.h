#ifndef JACE_JOBJECT_H
#define JACE_JOBJECT_H

#include "jace/OsDep.h"
#include "jace/Namespace.h"
#include "jace/JArguments.h"
#include "jace/proxy/JValue.h"


BEGIN_NAMESPACE_2(jace, proxy)

/**
 * The abstract base class for all C++ proxy objects generated for java.
 * These proxy classes are a C++ representation of a java class reference.
 *
 * ----------------------------------------------------------------------
 *
 * In addition to the rules in the JValue class, JObject's must
 * also adhere to the following set of rules:
 *
 * - All JObject's must be constructable from a JNI jobject.
 *   For example: String::String(jobject).
 *
 * ----------------------------------------------------------------------
 *
 * JObjects may be created from existing jobjects, or they may
 * create new jobjects.
 *
 * For example, the code:
 * {
 *   String foo(java_new<String>());
 * }
 *
 * creates a new java.lang.String through JNI and assigns it to a reference called "foo".
 *
 * Whereas the code:
 *
 * Java_com_foo_bar(jobject this, jstring aString)
 * {
 *   String foo(aString);
 * }
 *
 * does not create a new java.lang.String, but simply creates a reference to an
 * existing java.lang.String.
 *
 * In both cases, String creates a global reference to the jstring,
 * and does not release that global reference until it's lifetime
 * has ended.
 *
 * @author Toby Reyelts
 */
class JObject: public ::jace::proxy::JValue
{
public:
	/**
	 * Creates a new null reference.
	 *
	 * All subclasses of JObject should provide this constructor
	 * for their own subclasses.
	 */
	JACE_API explicit JObject();

	/**
	 * Creates a new reference to an existing jvalue.
	 */
	JACE_API explicit JObject(jvalue value);

	/**
	 * Creates a new reference to an existing jobject.
	 */
	JACE_API explicit JObject(jobject object);

	/**
	 * Creates a new reference to an existing object.
	 *
	 * @param object the object
	 */
	JACE_API JObject(const JObject& object);

	/**
	 * Destroys the existing java object.
	 */
	JACE_API virtual ~JObject() throw();

	/**
	 * Sets the reference to another object.
	 */
	JACE_API JObject& operator=(const JObject& other);

	/**
	 * Returns the underlying JNI jobject for this JObject.
	 *
	 * WARNING: The returned jobject is valid so long as its parent JObject is valid.
	 * Given the code: <code>jobject myThread = Thread::currentThread</code>
	 * the returned jobject will become invalid right after the assignment operation
	 * because the enclosing Thread goes out of scope and destroys its associated jobject.
	 */
	JACE_API operator jobject();

	/**
	 * Returns the underlying JNI jobject for this JObject.
	 *
	 * WARNING: The returned jobject is valid so long as its parent JObject is valid.
	 * Given the code: <code>jobject myThread = Thread::currentThread</code>
	 * the returned jobject will become invalid right after the assignment operation
	 * because the enclosing Thread goes out of scope and destroys its associated jobject.
	 *
	 * Users of this method should be careful not to modify the
	 * object through calls against the returned jobject.
	 */
	JACE_API operator jobject() const;

	/**
	 * Returns true if this JObject represents a null java reference.
	 *
	 * If this method returns true, it is not safe to call any proxy
	 * method on this JObject. Doing so will invoke undefined behavior.
	 */
	JACE_API bool isNull() const;

	/**
	 * Returns the JClass for this class.
	 */
	JACE_API static const ::jace::JClass& staticGetJavaJniClass() throw (::jace::JNIException);

	/**
	 * Returns the JClass that represents the static type of this class.
	 * For example, for a String Java object, referred to by a C++ proxy object,
	 * this method returns JClassImpl("java/lang/String").
	 */
	JACE_API virtual const JClass& getJavaJniClass() const throw (JNIException);

protected:
	/**
	 * Overridden so that a new global reference is created
	 * for the JNI jobject which is specified in value.
	 *
	 * @param value The JNI jvalue which represents this JObject.
	 * The jvalue must be must set with a jobject.
	 *
	 * @throws JNIException if the jobject has already been set,
	 *   or if the JVM runs out of memory while trying to create
	 *   a new global reference.
	 */
	JACE_API virtual void setJavaJniValue(jvalue value) throw (JNIException);

	/**
	 * Sets the jobject for this JObject.
	 *
	 * This method is simply a convenience method for calling
	 * setValue(jvalue) with a jobject.
	 */
	JACE_API void setJavaJniObject(jobject object) throw (JNIException);

	/**
	 * Constructs a new instance of the given class
	 * with the given arguments.
	 *
	 * @return the JNI jobject representing the new object.
	 *
	 * @throws JNIException if a JNI error occurs while trying to locate the method.
	 * @throws the corresponding C++ proxy exception, if a java exception
	 *   is thrown during method execution.
	 */
	JACE_API static jobject newObject(const ::jace::JClass& jClass, const ::jace::JArguments& arguments);
};


END_NAMESPACE_2(jace, proxy)

#endif
