#ifndef JACE_JNI_EXCEPTION_H
#define JACE_JNI_EXCEPTION_H

#include "jace/OsDep.h"
#include "jace/Namespace.h"
#include "jace/BaseException.h"

#include <string>


BEGIN_NAMESPACE(jace)


/**
 * An exception which is caused by invoking a call to the
 * java virtual machine through JNI.
 *
 * @author Toby Reyelts
 */
class JNIException : public BaseException
{
public:
	/**
	 * Creates a new JNIException with the given mesage.
	 */
	JACE_API JNIException(const std::string& value) throw ();

	/**
	 * Creates a new JNIException from an existing JNIException.
	 *
	 */
	JACE_API JNIException(const JNIException& rhs) throw ();

	/**
	 * Creates a new JNIException from an existing JNIException.
	 *
	 */
	JACE_API JNIException& operator=(const JNIException& rhs) throw ();
};

END_NAMESPACE(jace)

#endif // #ifndef JACE_JNI_EXCEPTION_H
