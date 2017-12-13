#ifndef JACE_TYPES_JBYTE_H
#define JACE_TYPES_JBYTE_H

#include "jace/OsDep.h"
#include "jace/Namespace.h"
#include "jace/Jace.h"
#include "jace/JClass.h"
#include "jace/proxy/JValue.h"
#include "jace/JNIException.h"


BEGIN_NAMESPACE_3(jace, proxy, types)

/**
 * A representation of the java primitive byte.
 *
 * @author Toby Reyelts
 */
class JByte : public ::jace::proxy::JValue
{
public:
	/**
	 * Creates a new JByte with the given value.
	 */
	JACE_API JByte(jvalue value);

	/**
	 * Creates a new JByte with the given value.
	 */
	JACE_API JByte(jbyte byte);

	/**
	 * Destroys the existing java object.
	 */
	JACE_API virtual ~JByte();

	/**
	 * Returns the byte value of this java byte.
	 */
	JACE_API operator jbyte() const;

	/**
	 * Compares this JByte to another.
	 */
	JACE_API bool operator==(const JByte& _byte) const;

	/**
	 * Compares this JByte to another.
	 */
	JACE_API bool operator!=(const JByte& _byte) const;


	/**
	 * Compares this JByte to a jbyte.
	 */
	JACE_API bool operator==(jbyte val) const;

	/**
	 * Compares this JByte to a jbyte.
	 */
	JACE_API bool operator!=(jbyte val) const;

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

#endif // #ifndef JACE_TYPES_JBYTE_H

