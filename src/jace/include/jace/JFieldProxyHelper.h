#ifndef JACE_JFIELD_PROXY_HELPER_H
#define JACE_JFIELD_PROXY_HELPER_H

#include "OsDep.h"
#include "Namespace.h"
#include "jace/proxy/JObject.h"
#include "jace/JClass.h"

#include "jni.h"

BEGIN_NAMESPACE_2(jace, JFieldProxyHelper)

JACE_API jobject assign(const jace::proxy::JObject& field, jobject parent, jfieldID fieldID);
JACE_API jobject assign(const jace::proxy::JObject& field, jclass parentClass, jfieldID fieldID);

END_NAMESPACE_2(jace, JFieldProxyHelper)

#endif
