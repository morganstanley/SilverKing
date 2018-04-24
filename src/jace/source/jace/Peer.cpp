#include "jace/Peer.h"

#include <string>
using std::string;


BEGIN_NAMESPACE(jace)

Peer::Peer(jobject obj)
{
  JNIEnv* env = attach();
  weakRef = env->NewWeakGlobalRef(obj);

  if (!weakRef)
	{
    string msg = "Unable to allocate a new weak reference for a Peer.";
    throw JNIException(msg);
  }
}

Peer::Peer(const Peer& other)
{
  JNIEnv* env = attach();
  weakRef = env->NewWeakGlobalRef(other.weakRef);

  if (!weakRef)
	{
    string msg = "Unable to allocate a new weak reference for a Peer.";
    throw JNIException(msg);
  }
}

Peer& Peer::operator=(const Peer& other)
{
	if (this == &other)
		return *this;
  JNIEnv* env = attach();

	jweak newReference = env->NewWeakGlobalRef(other.weakRef);
  if (!newReference)
	{
    string msg = "Unable to allocate a new weak reference for a Peer.";
    throw JNIException(msg);
  }
	deleteGlobalRef(env, weakRef);
	weakRef = newReference;
	return *this;
}

Peer::~Peer()
{
  JNIEnv* env = attach();
  env->DeleteWeakGlobalRef(weakRef);
}

void Peer::initialize()
{
}
  
void Peer::destroy()
{
}

jobject Peer::getGlobalRef()
{
  JNIEnv* env = attach();
  jobject ref = env->NewGlobalRef(weakRef);

  if (!ref)
	{
    throw JNIException("Unable to allocate a new global reference from a weak reference.\n"
      "It is likely that the weak reference is no longer valid.");
  }
  return ref;
}

void Peer::releaseGlobalRef(jobject ref)
{
  JNIEnv* env = attach();
  deleteGlobalRef(env, ref);
}
 
END_NAMESPACE(jace)
