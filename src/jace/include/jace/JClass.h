#ifndef JACE_JCLASS_H
#define JACE_JCLASS_H

#include "jace/OsDep.h"
#include "jace/Namespace.h"
#include "jace/JNIException.h"

#include <jni.h>

#include <string>
#include <memory>

BEGIN_NAMESPACE(jace)


/**
 * An interface that represents a java class.
 *
 * @author Toby Reyelts
 */
class JClass
{
public:
	/**
	 * Destroys this JClass.
	 */
	JACE_API virtual ~JClass() {}

	/**
	 * Returns the internal name of the class. such as "java/lang/Object".
	 *   The internal name of a class is its fully qualified name, as returned by Class.getName(),
	 *   where '.' is replaced by '/'.
	 */
	JACE_API virtual const std::string& getInternalName() const = 0;

	/**
	 * Returns the class type signature, such as "Ljava/lang/Object;"
	 */
	JACE_API virtual const std::string& getSignature() const = 0;

	/**
	 * Returns the JNI representation of this class.
	 */
	JACE_API virtual jclass getClass() const throw (jace::JNIException) = 0;
};


END_NAMESPACE(jace)

#endif
