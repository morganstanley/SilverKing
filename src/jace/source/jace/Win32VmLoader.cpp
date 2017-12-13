
#ifdef _WIN32

#include "jace/OsDep.h"

#include "jace/Win32VmLoader.h"
using ::jace::VmLoader;
using ::jace::Win32VmLoader;
using ::jace::JNIException;

#include "jace/Jace.h"

#define WIN32_LEAN_AND_MEAN
#include <windows.h>

#include <string>
using std::string;

namespace
{
  /**
   * Windows helper for querying the registry values related to the various JVM installations
   */
  bool getRegistryValue(const std::string& regPath, std::string& value)
	{
    // Split into key and value name
    size_t lastSlash = regPath.rfind('\\');
    std::string keyName = regPath.substr(0, lastSlash);
    std::string valueName = regPath.substr(lastSlash+1, regPath.length() - lastSlash);

    // Open the registry and get the data
    HKEY regKey;

    if (RegOpenKeyEx(HKEY_LOCAL_MACHINE, keyName.c_str(), 0, KEY_READ, &regKey) == ERROR_SUCCESS)
		{
		  BYTE retBuf[ 1000 ];
		  DWORD retBufSize = sizeof(retBuf);
		  DWORD retType;
      LONG apiRetVal = RegQueryValueEx(regKey, valueName.c_str(), 0, &retType, retBuf, &retBufSize);

      RegCloseKey(regKey);

		  if (apiRetVal == ERROR_SUCCESS)
			{
			  if (retType == REG_SZ)
				{
				  value = (char*) retBuf;
				  return true;
			  }
		  }
    }
	  return false;
  }
}


Win32VmLoader::Win32VmLoader(Win32VmLoader::JVMVendor jvmVendor, Win32VmLoader::JVMType jvmType,
														 std::string jvmVersion, jint _jniVersion) throw (JNIException):
  VmLoader(_jniVersion), path(), handle(0)
{
  getCreatedJavaVMsPtr = 0;
  createJavaVMPtr = 0;

  specifyVm(jvmVendor, jvmType, jvmVersion);
	loadVm(path);
}

Win32VmLoader::Win32VmLoader(std::string _path, jint _jniVersion) throw (JNIException):
	VmLoader(_jniVersion), path(_path), handle(0)
{
  getCreatedJavaVMsPtr = 0;
  createJavaVMPtr = 0;
	loadVm(path);
}

void Win32VmLoader::specifyVm(Win32VmLoader::JVMVendor jvmVendor, Win32VmLoader::JVMType jvmType,
															std::string version)
{
	switch (jvmVendor)
	{
		case JVMV_SUN:
		{
			// Get current version
			if (version.empty())
			{
				if (!getRegistryValue("Software\\JavaSoft\\Java Runtime Environment\\CurrentVersion", version))
					throw JNIException("No default Sun JRE found");
			}

#ifdef JACE_AMD64
			if (jvmType == JVMT_DEFAULT)
			{
				// WORKAROUND: 64-bit JVM only ships with <jre>/bin/server/jvm.dll yet the registry points to
				// <jre>/bin/client/jvm.dll
				jvmType = JVMT_SERVER;
			}
#endif

			// Search Registry for PATH to Sun runtime
			switch (jvmType)
			{
				case JVMT_DEFAULT:
				{
					if (!getRegistryValue(string("Software\\JavaSoft\\Java Runtime Environment\\") +
																version + "\\RuntimeLib", path))
					{
						throw JNIException(string("Sun JRE ") + version + " not found");
					}
					break;
				}
				case JVMT_CLASSIC:
				{
					if (version.find("1.3") != 0)
						throw JNIException(string("Classic VM not available in Sun JRE ") + version);
					if (!getRegistryValue(string("Software\\JavaSoft\\Java Runtime Environment\\") +
																version + "\\JavaHome", path))
					{
						throw JNIException(string("Classic Sun JRE ") + version + " not found");
					}
					path += "\\bin\\classic\\jvm.dll";
					break;
				}
				case JVMT_DEBUG:
					throw JNIException("Debug VM not available in Sun JRE");
				case JVMT_HOTSPOT:
				{
					if (version.find("1.3") != 0)
						throw JNIException(string("Hotspot VM not available in Sun JRE ") + version + "\nUse Client VM instead");
					if (!getRegistryValue(string("Software\\JavaSoft\\Java Runtime Environment\\") +
																version + "\\JavaHome", path))
					{
						throw JNIException(string("Hotspot Sun JRE ") + version + " not found");
					}
					path += "\\bin\\hotspot\\jvm.dll";
					break;
				}
				case JVMT_SERVER:
				{
					if (!getRegistryValue(string("Software\\JavaSoft\\Java Development Kit\\") +
																version + "\\JavaHome", path))
					{
						throw JNIException(string("Sun JDK ") + version + " not found");
					}
					path += "\\jre\\bin\\server\\jvm.dll";
					break;
				}
				case JVMT_CLIENT:
				{
					if (!version.find("1.4") == 0)
						throw JNIException(string("Client Sun JRE ") + version + " not found");
					if (!getRegistryValue(string("Software\\JavaSoft\\Java Runtime Environment\\") +
																version + "\\JavaHome", path))
					{
						throw JNIException(string("Client VM not available in Sun JRE ") + version +
															 "\nUse Classic or Hotspot VM instead");
					}
					path += "\\bin\\client\\jvm.dll";
					break;
				}
			}
		break;
	}
		case JVMV_IBM:
		{
			// Search Registry for PATH to IBM runtime
			switch (jvmType)
			{
 				case JVMT_DEFAULT:
				case JVMT_CLASSIC:
					if (!getRegistryValue("Software\\IBM\\Java2 Runtime Environment\\1.3\\RuntimeLib", path))
						throw JNIException("IBM JRE 1.3 not found");
					break;

				case JVMT_DEBUG:
					if (!getRegistryValue("Software\\IBM\\Java Development Kit\\1.3\\JavaHome", path))
					{
						throw JNIException("IBM JDK 1.3 not found");
					}
					path += "\\jre\\bin\\classic\\jvm_g.dll";
					break;

				case JVMT_HOTSPOT:
					throw JNIException("Hotspot VM not available in IBM JRE");
				case JVMT_SERVER:
					throw JNIException("Server VM not available in IBM JRE");
				case JVMT_CLIENT:
					throw JNIException("Client VM not available in IBM JRE");
			}
		}
		break;
	}
}

void Win32VmLoader::loadVm(const std::string &jvmPath) throw (JNIException)
{
  // Load the Java VM DLL
  if ((handle = LoadLibrary(jvmPath.c_str())) == 0)
    throw JNIException(string("Can't load JVM from ") + jvmPath);

  // Now get the function addresses
  getCreatedJavaVMsPtr = (GetCreatedJavaVMs_t) GetProcAddress(handle, "JNI_GetCreatedJavaVMs");
	if (!getCreatedJavaVMsPtr)
		throw JNIException(string("Can't find JNI_GetCreatedJavaVMs in ") + jvmPath);

  createJavaVMPtr = (CreateJavaVM_t) GetProcAddress(handle, "JNI_CreateJavaVM");
	if (!createJavaVMPtr)
		throw JNIException(string("Can't find JNI_CreateJavaVM in ") + jvmPath);
}

jint Win32VmLoader::createJavaVM(JavaVM **pvm, void **env, void *args) const
{
  return createJavaVMPtr(pvm, env, args);
}

jint Win32VmLoader::getCreatedJavaVMs(JavaVM **vmBuf, jsize bufLen, jsize *nVMs) const
{
  return getCreatedJavaVMsPtr(vmBuf, bufLen, nVMs);
}

Win32VmLoader::~Win32VmLoader()
{
  if (handle)
	{
    FreeLibrary(handle);
    handle = 0;
  }
}

#endif // _WIN32

