#include "jace/JFieldProxyHelper.h"

using jace::proxy::JObject;
using jace::JClass;

#ifndef JACE_JACE_H
#include "jace/Jace.h"
#endif

BEGIN_NAMESPACE_2(jace, JFieldProxyHelper)


jobject assign(const JObject& field, jobject parent, jfieldID fieldID)
{
  JNIEnv* env = attach();
  jobject object = static_cast<jobject>(field);
  env->SetObjectField(parent, fieldID, object);
  return object;
}

jobject assign(const JObject& field, jclass parentClass, jfieldID fieldID)
{
  JNIEnv* env = attach();
  jobject object = static_cast<jobject>(field);
  env->SetStaticObjectField(parentClass, fieldID, object);
  return object;
}


END_NAMESPACE_2(jace, JFieldProxyHelper)
