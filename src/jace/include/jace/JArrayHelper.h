#ifndef JACE_JARRAY_HELPER
#define JACE_JARRAY_HELPER

#include "OsDep.h"
#include "Namespace.h"
#include "jace/proxy/JObject.h"
#include "jace/JClass.h"
#include "jace/proxy/types/JInt.h"

#include "jni.h"

BEGIN_NAMESPACE_2(jace, JArrayHelper)

JACE_API jobjectArray newArray(int size, const jace::JClass& elementClass);
JACE_API jace::proxy::types::JInt getLength(jobject obj);
JACE_API jvalue getElement(jobject obj, int index);

END_NAMESPACE_2(jace, JArrayHelper)

#endif
