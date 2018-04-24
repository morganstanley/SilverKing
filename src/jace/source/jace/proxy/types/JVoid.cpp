#include "jace/proxy/types/JVoid.h"

#include "jace/JClassImpl.h"

#include "jace/BoostWarningOff.h"
#include <boost/thread/mutex.hpp>
#include "jace/BoostWarningOn.h"

BEGIN_NAMESPACE_3(jace, proxy, types)


static boost::mutex javaClassMutex;
const JClass& JVoid::staticGetJavaJniClass() throw (JNIException)
{
	static boost::shared_ptr<JClassImpl> result;
	boost::mutex::scoped_lock lock(javaClassMutex);
	if (result == 0)
		result = boost::shared_ptr<JClassImpl>(new JClassImpl("void", "V"));
	return *result;
}

const JClass& JVoid::getJavaJniClass() const throw (JNIException)
{
  return JVoid::staticGetJavaJniClass();
}

END_NAMESPACE_3(jace, proxy, types)
