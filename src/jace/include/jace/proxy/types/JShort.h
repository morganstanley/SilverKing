#ifndef JACE_TYPES_JSHORT_H
#define JACE_TYPES_JSHORT_H

#include "jace/OsDep.h"
#include "jace/Namespace.h"
#include "jace/Jace.h"
#include "jace/JClass.h"
#include "jace/proxy/JValue.h"
#include "jace/JNIException.h"


BEGIN_NAMESPACE_3(jace, proxy, types)

/**
 * A representation of the java primitive short.
 *
 * @author Toby Reyelts
 */
class JShort : public JValue
{
public:
	/**
	 * Creates a new instance with the given value.
	 */
	JACE_API JShort(jvalue value);

	/**
	 * Creates a new instance with the given value.
	 */
	JACE_API JShort(jshort value);

	/**
	 * Destroys the existing java object.
	 */
	JACE_API virtual ~JShort();

	/**
	 * Returns the value of this instance.
	 */
	JACE_API operator jshort() const;

	/**
	 * Compares this instance to another.
	 */
	JACE_API bool operator==(const JShort& value) const;

	/**
	 * Compares this instance to another.
	 */
	JACE_API bool operator!=(const JShort& value) const;

	/**
	 * Compares this instance to another.
	 */
	JACE_API bool operator==(jshort value) const;

	/**
	 * Compares this instance to another.
	 */
	JACE_API bool operator!=(jshort value) const;

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

#endif // #ifndef JACE_TYPES_JSHORT_H
