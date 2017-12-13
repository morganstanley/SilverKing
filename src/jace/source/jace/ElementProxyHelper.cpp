#include "jace/ElementProxyHelper.h"

#include "jace/Jace.h"
using ::jace::proxy::JObject;

BEGIN_NAMESPACE_2(jace, ElementProxyHelper)

void assign(const JObject& element, int index, jarray parent)
{
  JNIEnv* env = attach();
  jobjectArray array = static_cast<jobjectArray>(parent);
  env->SetObjectArrayElement(array, index, element);
}

END_NAMESPACE_2(jace, ElementProxyHelper)
