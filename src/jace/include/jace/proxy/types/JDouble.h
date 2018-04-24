#ifndef JACE_TYPES_JDOUBLE_H
#define JACE_TYPES_JDOUBLE_H

#include "jace/OsDep.h"
#include "jace/Namespace.h"
#include "jace/Jace.h"
#include "jace/JClass.h"
#include "jace/proxy/JValue.h"
#include "jace/JNIException.h"


BEGIN_NAMESPACE_3(jace, proxy, types)

/**
 * A representation of the java primitive double.
 *
 * @author Toby Reyelts
 */
class JDouble : public JValue
{
public:
	/**
	 * Creates a new JDouble with the given value.
	 */
	JACE_API JDouble(jvalue value);

	/**
	 * Creates a new JDouble with the given value.
	 */
	JACE_API JDouble(jdouble value);

	/**
	 * Destroys the existing java object.
	 */
	JACE_API virtual ~JDouble();

	/**
	 * Returns the value of this instance.
	 */
	JACE_API operator jdouble() const;

	/**
	 * Compares this instance to another.
	 */
	JACE_API bool operator==(const JDouble& value) const;

	/**
	 * Compares this instance to another.
	 */
	JACE_API bool operator!=(const JDouble& value) const;

	/**
	 * Compares this instance to another.
	 */
	JACE_API bool operator==(jdouble value) const;

	/**
	 * Compares this instance to another.
	 */
	JACE_API bool operator!=(jdouble value) const;

	/**
	 * Returns the JClass for this class.
	 */
	JACE_API static const ::jace::JClass& staticGetJavaJniClass() throw (::jace::JNIException);

	/**
	 * Returns the JClass for this instance.
	 */
	JACE_API virtual const ::jace::JClass& getJavaJniClass() const throw (::jace::JNIException);
};


END_NAMESPACE_3(jace, proxy, types)

#endif // #ifndef JACE_TYPES_JDOUBLE_H
