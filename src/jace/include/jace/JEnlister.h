#ifndef JACE_JENLISTER_H
#define JACE_JENLISTER_H

#include "jace/OsDep.h"
#include "jace/Namespace.h"
#include "jace/Jace.h"
#include "jace/JFactory.h"

#include "jace/BoostWarningOff.h"
#include <boost/shared_ptr.hpp>
#include "jace/BoostWarningOn.h"


BEGIN_NAMESPACE(jace)

/**
 * An implementation of a JFactory that creates new instances
 * of JValues.
 *
 * This template class works with any subclass of JValue. It provides
 * enlistment capabilities for Jace generated proxies.
 *
 * @author Toby Reyelts
 */
template <class T> class JEnlister : public ::jace::JFactory
{
public:
	/**
	 * Constructs this JEnlister and registers with Jace.
	 */
	JEnlister()
	{
		enlist(this);
	}

	/**
	 * Creates a new instance of T.
	 */
	virtual boost::shared_ptr<jace::proxy::JValue> create(jvalue val)
	{
		return boost::shared_ptr<T>(new T(val));
	}

	/**
	 * Creates a new instance of the value type for this JFactory
	 * and throws that instance.
	 *
	 * This method is equivalent to
	 *
	 *   throw *(JFactory::create(aValue)).get();
	 *
	 * except that the return value's real type is preserved and
	 * not sliced to a JValue upon being thrown.
	 */
	virtual void throwInstance(jvalue val)
	{
		T t(val);
		JNIEnv* env = attach();

		// We know that val is a jobject, because you can only throw exceptions.
		deleteLocalRef(env, val.l);

		throw t;
	}

	/**
	 * Returns the the class of which this factory
	 * creates instances.
	 */
	virtual const ::jace::JClass& getClass()
	{
		return T::staticGetJavaJniClass();
	}
};

END_NAMESPACE(jace)

#endif // #ifndef JACE_JENLISTER_H
