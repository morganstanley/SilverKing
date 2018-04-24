#ifndef JACE_STATIC_VM_LOADER
#define JACE_STATIC_VM_LOADER

#include "jace/OsDep.h"
#include "jace/Namespace.h"
#include "jace/JNIException.h"
#include "jace/VmLoader.h"

#include <jni.h>

BEGIN_NAMESPACE(jace)

/**
 * The default VmLoader which works by statically linking to the JVM.
 * To turn off the StaticVmLoader, you must #define JACE_WANT_DYNAMIC_LOAD
 *
 * StaticVmLoader is totally inlined so that the Jace library can be built
 * orthogonally in reference to dynamic or static VM linking. If StaticVmLoader
 * wasn't defined within the header file, two different binaries of the Jace library
 * would be required.
 *
 * NOTE: jvm.dll must be in the system PATH at runtime. You cannot simply copy the library
 *       into the application directory because it locates dependencies relative to its location.
 *       See http://java.sun.com/products/jdk/faq/jni-j2sdk-faq.html#move for more information.
 *
 * @author Toby Reyelts
 */
class StaticVmLoader: public ::jace::VmLoader
{
public:
  StaticVmLoader(jint jniVersion):
		VmLoader(jniVersion)
	{}

  virtual jint getCreatedJavaVMs(JavaVM **vmBuf, jsize bufLen, jsize *nVMs) const
	{
    // Prevent static linking if the user intends on dynamically loading
    #ifndef JACE_WANT_DYNAMIC_LOAD
      return JNI_GetCreatedJavaVMs(vmBuf, bufLen, nVMs);
    #else
      return -1;
    #endif
  }

  virtual jint createJavaVM(JavaVM **pvm, void **env, void *args) const
	{
    // Prevent static linking if the user intends on dynamically loading
    #ifndef JACE_WANT_DYNAMIC_LOAD
      return JNI_CreateJavaVM(pvm, env, args);
    #else
      return -1;
    #endif
  }
};

END_NAMESPACE(jace)

#endif // JACE_STATIC_VM_LOADER
