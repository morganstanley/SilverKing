#ifndef JACE_JFIELD_HELPER_H
#define JACE_JFIELD_HELPER_H

#include "OsDep.h"
#include "Namespace.h"
#include "jace/proxy/JObject.h"
#include "jace/JClass.h"

#include "jni.h"
#include <string>

BEGIN_NAMESPACE(jace)

class JFieldHelper
{
public:
  JACE_API JFieldHelper(const std::string& name, const jace::JClass& typeClass);

  JACE_API jvalue getField(jace::proxy::JObject& object);
  JACE_API jvalue getField(const jace::JClass& jClass);
  JACE_API jfieldID getFieldID(const jace::JClass& parentClass, bool isStatic);
  JACE_API jfieldID getFieldID();

private:
	/**
	 * Prevent copying.
	 */
	JFieldHelper& operator=(JFieldHelper&);

  jfieldID mFieldID;
  const std::string mName;
  const JClass& mTypeClass;
};

END_NAMESPACE(jace)

#endif
