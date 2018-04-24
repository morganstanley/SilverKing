#include "jace/proxy/types/JInt.h"

#include "jace/JClassImpl.h"

#include <iostream>
using std::ostream;

#include "jace/BoostWarningOff.h"
#include <boost/thread/mutex.hpp>
#include "jace/BoostWarningOn.h"

BEGIN_NAMESPACE_3(jace, proxy, types)


JInt::JInt(jvalue value)
{
  setJavaJniValue(value);
}

JInt::JInt(const jint _int)
{
  jvalue value;
  value.i = _int;
  setJavaJniValue(value);
}

JInt::JInt(const JByte& _byte)
{
  jvalue value;
  value.i = static_cast<jbyte>(_byte);
  setJavaJniValue(value);
}


JInt::~JInt()
{}

JInt::operator jint() const
{ 
  return static_cast<jvalue>(*this).i; 
}

bool JInt::operator==(const JInt& _int) const
{
  return static_cast<jint>(_int) == static_cast<jint>(*this);
}

bool JInt::operator!=(const JInt& _int) const
{
  return !(*this == _int);
}

bool JInt::operator==(jint val) const
{
  return val == static_cast<jint>(*this);
}

bool JInt::operator!=(jint val) const
{
  return !(*this == val);
}

static boost::mutex javaClassMutex;
const JClass& JInt::staticGetJavaJniClass() throw (JNIException)
{
	static boost::shared_ptr<JClassImpl> result;
	boost::mutex::scoped_lock lock(javaClassMutex);
	if (result == 0)
		result = boost::shared_ptr<JClassImpl>(new JClassImpl("int", "I"));
	return *result;
}

const JClass& JInt::getJavaJniClass() const throw (JNIException)
{
  return JInt::staticGetJavaJniClass();
}

ostream& operator<<(ostream& stream, const JInt& val)
{
  return stream << static_cast<jint>(val);
}

END_NAMESPACE_3(jace, proxy, types)
