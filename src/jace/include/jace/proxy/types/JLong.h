#ifndef JACE_TYPES_JLONG_H
#define JACE_TYPES_JLONG_H

#include "jace/OsDep.h"
#include "jace/Namespace.h"
#include "jace/Jace.h"
#include "jace/JClass.h"
#include "jace/proxy/JValue.h"
#include "jace/JNIException.h"
#include "jace/proxy/types/JInt.h"


BEGIN_NAMESPACE_3(jace, proxy, types)
class JInt;

/**
 * A representation of the java primitive long.
 *
 * @author Toby Reyelts
 */
class JLong: public JValue
{
public:
	/**
	 * Creates a new instance with the given value.
	 */
	JACE_API JLong(jvalue value);

	/**
	 * Creates a new instance with the given value.
	 */
	JACE_API JLong(jlong _long);

	/**
	 * Creates a new instance with the given value.
	 */
	JACE_API JLong(const ::jace::proxy::types::JInt& _int);

	/**
	 * Destroys the existing java object.
	 */
	JACE_API virtual ~JLong();

	/**
	 * Returns the value of this instance.
	 */
	JACE_API operator jlong() const;

	/**
	 * Compares this instance to another.
	 */
	JACE_API bool operator==(const JLong& _long) const;

	/**
	 * Compares this instance to another.
	 */
	JACE_API bool operator!=(const JLong& _long) const;


	/**
	 * Compares this instance to a primitive.
	 */
	JACE_API bool operator==(jlong val) const;

	/**
	 * Compares this instance to a primitive.
	 */
	JACE_API bool operator!=(jlong val) const;

	/**
	 * Returns the JClass for this class.
	 */
	JACE_API static const ::jace::JClass& staticGetJavaJniClass() throw (::jace::JNIException);

	/**
	 * Retrieves the JavaClass for this JObject.
	 *
	 * @throw JNIException if an error occurs while trying to retrieve the class.
	 */
	JACE_API virtual const ::jace::JClass& getJavaJniClass() const throw (::jace::JNIException);
};


END_NAMESPACE_3(jace, proxy, types)

#endif // #ifndef JACE_TYPES_JLONG_H
