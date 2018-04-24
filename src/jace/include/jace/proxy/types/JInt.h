#ifndef JACE_TYPES_JINT_H
#define JACE_TYPES_JINT_H

#include "jace/OsDep.h"
#include "jace/Namespace.h"
#include "jace/Jace.h"
#include "jace/JClass.h"
#include "jace/proxy/JValue.h"
#include "jace/JNIException.h"
#include "jace/proxy/types/JByte.h"

#include <iostream>


BEGIN_NAMESPACE_3(jace, proxy, types)
class JByte;

/**
 * A representation of the java primitive int.
 *
 * @author Toby Reyelts
 */
class JInt: public JValue
{
public:
	/**
	 * Creates a new instance with the given value.
	 */
	JACE_API JInt(jvalue value);

	/**
	 * Creates a new instance with the given value.
	 */
	JACE_API JInt(const jint _int);

	/**
	 * Creates a new instance with the given value.
	 */
	JACE_API JInt(const ::jace::proxy::types::JByte& _byte);

	/**
	 * Destroys the existing java object.
	 */
	JACE_API virtual ~JInt();

	/**
	 * Returns the value of this instance.
	 */
	JACE_API operator jint() const;

	/**
	 * Compares this JInt to another.
	 */
	JACE_API bool operator==(const JInt& _int) const;

	/**
	 * Compares this JInt to another.
	 */
	JACE_API bool operator!=(const JInt& _int) const;

	/**
	 * Compares this JInt to a jint.
	 */
	JACE_API bool operator==(jint val) const;

	/**
	 * Compares this JInt to a jint.
	 */
	JACE_API bool operator!=(jint val) const;

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

	JACE_API friend std::ostream& operator<<(std::ostream& stream, const JInt& val);
};


END_NAMESPACE_3(jace, proxy, types)

#endif // #ifndef JACE_TYPES_JINT_H
