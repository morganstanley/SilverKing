#ifndef JACE_TYPES_JFLOAT_H
#define JACE_TYPES_JFLOAT_H

#include "jace/OsDep.h"
#include "jace/Namespace.h"
#include "jace/Jace.h"
#include "jace/JClass.h"
#include "jace/proxy/JValue.h"
#include "jace/JNIException.h"


BEGIN_NAMESPACE_3(jace, proxy, types)

/**
 * A representation of the java primitive float.
 *
 * @author Toby Reyelts
 */
class JFloat : public JValue
{
public:
	/**
	 * Creates a new instance with the given value.
	 */
	JACE_API JFloat(jvalue value);

	/**
	 * Creates a new instance with the given value.
	 */
	JACE_API JFloat(jfloat value);

	/**
	 * Destroys the existing java object.
	 */
	JACE_API virtual ~JFloat();

	/**
	 * Returns the value of this instance.
	 */
	JACE_API operator jfloat() const;

	/**
	 * Compares this instance to another.
	 */
	JACE_API bool operator==(const JFloat& value) const;

	/**
	 * Compares this instance to another.
	 */
	JACE_API bool operator!=(const JFloat& value) const;

	/**
	 * Compares this instance to another.
	 */
	JACE_API bool operator==(jfloat value) const;

	/**
	 * Compares this instance to another.
	 */
	JACE_API bool operator!=(jfloat value) const;

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

#endif // #ifndef JACE_TYPES_JFLOAT_H
