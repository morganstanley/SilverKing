#ifndef JACE_JFIELD_H
#define JACE_JFIELD_H

#include "jace/OsDep.h"
#include "jace/Namespace.h"
#include "jace/JClass.h"
#include "jace/proxy/JObject.h"
#include "jace/JNIException.h"
#include "jace/JFieldProxy.h"
#include "jace/JFieldHelper.h"
#include "jace/proxy/types/JBoolean.h"
#include "jace/proxy/types/JByte.h"
#include "jace/proxy/types/JChar.h"
#include "jace/proxy/types/JDouble.h"
#include "jace/proxy/types/JFloat.h"
#include "jace/proxy/types/JInt.h"
#include "jace/proxy/types/JLong.h"
#include "jace/proxy/types/JShort.h"

#include <jni.h>

#include <string>

BEGIN_NAMESPACE(jace)

/**
 * Represents a java field.
 *
 * @author Toby Reyelts
 */
template <class Type> class JField
{
public:
	/**
	 * Creates a new JField representing the field with the
	 * given name.
	 */
	JField(const std::string& name): helper(name, Type::staticGetJavaJniClass())
	{
	}

	/**
	 * Retrieves the field belonging to the given object.
	 *
	 * @throws JNIException if an error occurs while trying to retrieve the field.
	 */
	JFieldProxy<Type> get(::jace::proxy::JObject& object)
	{
		jvalue value = helper.getField(object);
		JFieldProxy<Type> fieldProxy(helper.getFieldID(), value, object);
		JNIEnv* env = attach();
		deleteLocalRef(env, value.l);
		return fieldProxy;
	}


	/**
	 * Retrieves the value of the static field belonging to the given class.
	 *
	 * @throws JNIException if an error occurs while trying to retrieve the value.
	 */
	JFieldProxy<Type> get(const ::jace::JClass& jClass)
	{
		jvalue value = helper.getField(jClass);
		JFieldProxy<Type> fieldProxy(helper.getFieldID(), value, jClass.getClass());
		JNIEnv* env = attach();
		deleteLocalRef(env, value.l);
		return fieldProxy;
	}

private:
	::jace::JFieldHelper helper;

	jfieldID getFieldID(const ::jace::JClass& parentClass, bool isStatic = false)
	{
		return helper.getFieldID(parentClass, isStatic);
	}
};

END_NAMESPACE(jace)

/**
 * For those (oddball) compilers that need the template specialization
 * definitions in the header.
 */
#ifdef PUT_TSDS_IN_HEADER
  #include "jace/JField.tsd"
#else
  #include "jace/JField.tsp"
#endif

#endif
