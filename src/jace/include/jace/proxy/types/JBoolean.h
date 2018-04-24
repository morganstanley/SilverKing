#ifndef JACE_TYPES_JBOOLEAN_H
#define JACE_TYPES_JBOOLEAN_H

#include "jace/OsDep.h"
#include "jace/Namespace.h"
#include "jace/Jace.h"
#include "jace/JClass.h"
#include "jace/proxy/JValue.h"
#include "jace/JNIException.h"

BEGIN_NAMESPACE_1(jace)
template <class T> class ElementProxy;
template <class T> class JFieldProxy;
template <class T> class JField;
END_NAMESPACE_1(jace)

BEGIN_NAMESPACE_3(jace, proxy, types)


/**
 * A representation of the java primitive boolean.
 *
 * @author Toby Reyelts
 */
class JBoolean: public ::jace::proxy::JValue
{
public:
	/**
	 * Creates a new instance with the given value.
	 */
	JACE_API JBoolean(jvalue value);

	/**
	 * Creates a new instance with the given value.
	 */
	JACE_API JBoolean(jboolean value);

	/**
	 * Destroys the existing java object.
	 */
	JACE_API virtual ~JBoolean();

	/**
	 * Returns the value of this instance.
	 */
	JACE_API operator jboolean() const;

	/**
	 * Compares this JBoolean to another.
	 */
	JACE_API bool operator==(const JBoolean& _boolean) const;

	/**
	 * Compares this JBoolean to another.
	 */
	JACE_API bool operator!=(const JBoolean& _boolean) const;

	/**
	 * Compares this JBoolean to a jboolean.
	 */
	JACE_API bool operator==(jboolean val) const;

	/**
	 * Compares this JBoolean to a jboolean.
	 */
	JACE_API bool operator!=(jboolean val) const;

	/**
	 * Returns the JClass for this class.
	 */
	JACE_API static const ::jace::JClass& staticGetJavaJniClass() throw (::jace::JNIException);

	/**
	 * Returns the JClass for this instance.
	 */
	JACE_API virtual const ::jace::JClass& getJavaJniClass() const throw (::jace::JNIException);

	friend class ::jace::ElementProxy<JBoolean>;
	friend class ::jace::JFieldProxy<JBoolean>;
	friend class ::jace::JField<JBoolean>;
};


END_NAMESPACE_3(jace, proxy, types)

#endif // #ifndef JACE_TYPES_JBOOLEAN_H

