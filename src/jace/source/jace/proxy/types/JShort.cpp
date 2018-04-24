#include "jace/proxy/types/JShort.h"

#include "jace/JClassImpl.h"

#include "jace/BoostWarningOff.h"
#include <boost/thread/mutex.hpp>
#include "jace/BoostWarningOn.h"

BEGIN_NAMESPACE_3(jace, proxy, types)


JShort::JShort(jvalue value)
{
  setJavaJniValue(value);
}

JShort::JShort(jshort _short)
{
  jvalue value;
  value.s = _short;
  setJavaJniValue(value);
}

JShort::~JShort()
{}

JShort::operator jshort() const
{
  return static_cast<jvalue>(*this).s;
}

bool JShort::operator==(const JShort& _short) const
{
  return static_cast<jshort>(_short) == static_cast<jshort>(*this);
}

bool JShort::operator!=(const JShort& _short) const
{
  return !(*this == _short);
}

bool JShort::operator==(jshort val) const
{
  return val == static_cast<jshort>(*this);
}

bool JShort::operator!=(jshort val) const
{
  return !(*this == val);
}

static boost::mutex javaClassMutex;
const JClass& JShort::staticGetJavaJniClass() throw (JNIException)
{
	static boost::shared_ptr<JClassImpl> result;
	boost::mutex::scoped_lock lock(javaClassMutex);
	if (result == 0)
		result = boost::shared_ptr<JClassImpl>(new JClassImpl("short", "S"));
	return *result;
}

const JClass& JShort::getJavaJniClass() const throw (JNIException)
{
  return JShort::staticGetJavaJniClass();
}

END_NAMESPACE_3(jace, proxy, types)
