
#include "jace/UnixVmLoader.h"

#ifdef JACE_GENERIC_UNIX

using ::jace::UnixVmLoader;
using ::jace::VmLoader;
using ::jace::JNIException;

using std::string;

#include <dlfcn.h>

BEGIN_NAMESPACE(jace)

UnixVmLoader::UnixVmLoader(std::string _path, jint jniVersion) throw (JNIException): 
  VmLoader(jniVersion), path(_path), lib(0)
{
  lib = dlopen(path.c_str(), RTLD_NOW | RTLD_GLOBAL);
  if (!lib)
	{
    string msg = "Unable to load the library at " + path;
    throw JNIException(msg);
  }

  createJavaVMPtr = (CreateJavaVM_t) dlsym(lib, "JNI_CreateJavaVM");

  if (!createJavaVMPtr)
	{
    string msg = "Unable to resolve the function, JNI_CreateJavaVM from library " + path;
    throw JNIException(msg);
  }

  getCreatedJavaVMsPtr = (GetCreatedJavaVMs_t) dlsym(lib, "JNI_GetCreatedJavaVMs");

  if (!getCreatedJavaVMsPtr)
	{
    string msg = "Unable to resolve the function, JNI_GetCreatedJavaVMs from library " 
      + path;
    throw JNIException(msg);
  }
}

jint UnixVmLoader::createJavaVM(JavaVM **pvm, void **env, void *args) const
{
  return createJavaVMPtr(pvm, env, args);
}

jint UnixVmLoader::getCreatedJavaVMs(JavaVM **vmBuf, jsize bufLen, jsize *nVMs) const
{
  return getCreatedJavaVMsPtr(vmBuf, bufLen, nVMs);
}

UnixVmLoader::~UnixVmLoader()
{
  if (lib)
	{
    dlclose(lib);
    lib = 0;
  }
}

END_NAMESPACE(jace)

#endif // JACE_GENERIC_UNIX
