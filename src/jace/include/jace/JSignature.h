#ifndef JACE_JSIGNATURE_H
#define JACE_JSIGNATURE_H

#include "jace/OsDep.h"
#include "jace/Namespace.h"
#include "jace/JClass.h"

#include <string>
#include <list>

#include "jace/BoostWarningOff.h"
#include <boost/ref.hpp>
#include "jace/BoostWarningOn.h"

BEGIN_NAMESPACE(jace)


/**
 * A representation of the signature for a java method.
 *
 * A JSignature consists of the argument types for a method,
 * followed by the return type for the method.
 *
 * @author Toby Reyelts
 */
class JSignature
{
public:
	/**
	 * Constructs a new JSignature with the given return type.
	 */
	JACE_API JSignature(const JClass& returnType);


	/**
	 * Returns a string representation of this JSignature.
	 *
	 * The string representation is formatted as a fully qualified method signature.
	 *
	 * For example, the following JSignature:
	 *
	 *   JSignature(Void::staticGetJavaJniClass()) signature
	 *     << String::staticGetJavaJniClass()
	 *     << Url::staticGetJavaJniClass();
	 *
	 * returns the following string from a call toString:
	 *
	 *   "(Ljava/lang/String;Ljava/net/URL;)V"
	 */
	JACE_API std::string toString() const;

	/**
	 * Adds a new argument type to the method signature.
	 *
	 * A JSignature may have any arbitrary number of argument types,
	 */
	JACE_API JSignature& add(const JClass& argumentType);


	/**
	 * A convenience operator for JSignature.add.
	 */
	JACE_API JSignature& operator<<(const JClass& argumentType);

private:
	/**
	 * Prevent assignment.
	 */
	JSignature& operator=(JSignature&);
	std::list< ::boost::reference_wrapper<const JClass> > mTypes;
	const JClass& mResultType;
};


END_NAMESPACE(jace)

#endif // #ifndef JACE_JSIGNATURE_H
