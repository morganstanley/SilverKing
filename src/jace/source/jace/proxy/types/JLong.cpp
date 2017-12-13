#include "jace/proxy/types/JLong.h"

#include "jace/JClassImpl.h"

#include "jace/BoostWarningOff.h"
#include <boost/thread/mutex.hpp>
#include "jace/BoostWarningOn.h"

BEGIN_NAMESPACE_3(jace, proxy, types)


JLong::JLong(jvalue value)
{
  setJavaJniValue(value);
}

JLong::JLong(jlong _long)
{
  jvalue value;
  value.j = _long;
  setJavaJniValue(value);
}

JLong::JLong(const JInt& _int)
{
  jvalue value;
  value.j = static_cast<jint>(_int);
  setJavaJniValue(value);
}


JLong::~JLong()
{}

JLong::operator jlong() const
{
  return static_cast<jvalue>(*this).j;
}

bool JLong::operator==(const JLong& _long) const
{
  return static_cast<jlong>(_long) == static_cast<jlong>(*this);
}

bool JLong::operator!=(const JLong& _long) const
{
  return !(*this == _long);
}

bool JLong::operator==(jlong val) const
{
  return val == static_cast<jlong>(*this);
}

bool JLong::operator!=(jlong val) const
{
  return !(*this == val);
}

static boost::mutex javaClassMutex;
const JClass& JLong::staticGetJavaJniClass() throw (JNIException)
{
	static boost::shared_ptr<JClassImpl> result;
	boost::mutex::scoped_lock lock(javaClassMutex);
	if (result == 0)
		result = boost::shared_ptr<JClassImpl>(new JClassImpl("long", "J"));
	return *result;
}

const JClass& JLong::getJavaJniClass() const throw (JNIException)
{
  return JLong::staticGetJavaJniClass();
}

END_NAMESPACE_3(jace, proxy, types)
