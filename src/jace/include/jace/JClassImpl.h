#ifndef JACE_JCLASS_IMPL_H
#define JACE_JCLASS_IMPL_H

#include "jace/OsDep.h"
#include "jace/Namespace.h"
#include "jace/JClass.h"
#include "jace/JNIException.h"

/*
BEGIN_NAMESPACE(boost)
	class mutex;
END_NAMESPACE(boost)
*/

/*
#ifndef BOOST_NAMESPACE_OVERRIDE 
 #define BOOST_NAMESPACE boost 
#else 
  #define BOOST_NAMESPACE BOOST_NAMESPACE_OVERRIDE 
#endif //BOOST_NAMESPACE_OVERRIDE 

BEGIN_NAMESPACE(BOOST_NAMESPACE)
	class mutex;
END_NAMESPACE(BOOST_NAMESPACE)
#ifdef BOOST_NAMESPACE_OVERRIDE
namespace boost = BOOST_NAMESPACE_OVERRIDE;  //namespace aliasing
#endif //BOOST_NAMESPACE_OVERRIDE
*/
#include <boost/thread/mutex.hpp>

#include <string>

BEGIN_NAMESPACE(jace)


/**
 * The implementation of the JClass interface.
 *
 * @author Toby Reyelts
 */
class JClassImpl: public ::jace::JClass
{
public:
	/**
	 * Creates a new JClassImpl with the given name, and
	 * type name.
	 *
	 * @param internalName the internal name of the class. such as "java/lang/Object".
	 *   The internal name of a class is its fully qualified name, as returned by Class.getName(),
	 *   where '.' is replaced by '/'.
	 *
	 * @param signature the class type signature, such as "Ljava/lang/Object;".
	 *   For more information, see: http://download.oracle.com/javase/6/docs/technotes/guides/jni/spec/types.html#wp16432
	 */
	JACE_API JClassImpl(const std::string& internalName, const std::string& signature);

	/**
	 * Creates a new JClassImpl with the given name.
	 *
	 * @param internalName the internal name of the class. such as "java/lang/Object".
	 *   The internal name of a class is its fully qualified name, as returned by Class.getName(),
	 *   where '.' is replaced by '/'.
	 *
	 *
	 * The signature for the class is created by prepending
	 * "L" and appending ";" to name.
	 *
	 * For example,
	 *
	 *  JClassImpl("java/lang/String");
	 *
	 * is equivalent to
	 *
	 *  JClassImpl("java/lang/String", "Ljava/lang/String;");
	 */
	JACE_API JClassImpl(const std::string& internalName);

	/**
	 * Destroys this JClassImpl.
	 */
	JACE_API virtual ~JClassImpl() throw ();

	/**
	 * Returns the internal name of the class. such as "java/lang/Object".
	 *   The internal name of a class is its fully qualified name, as returned by Class.getName(),
	 *   where '.' is replaced by '/'.
	 */
	JACE_API virtual const std::string& getInternalName() const;

	/**
	 * Returns the class type signature, such as "Ljava/lang/Object;"
	 */
	JACE_API virtual const std::string& getSignature() const;

	/**
	 * Returns the JNI representation of this class.
	 */
	JACE_API virtual jclass getClass() const throw (::jace::JNIException);

private:
	/**
	 * Prevent copying.
	 */
	JClassImpl(JClassImpl&);
	/**
	 * Prevent assignment.
	 */
	JClassImpl& operator=(JClassImpl&);
	std::string internalName;
	std::string signature;
	mutable jclass theClass;
	boost::mutex* mutex;
};


END_NAMESPACE(jace)

#endif
