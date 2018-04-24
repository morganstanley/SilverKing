
#include "jace/VmLoader.h"

jace::VmLoader::VmLoader(jint _jniVersion):
	jniVersion(_jniVersion)
{}

jint jace::VmLoader::getJniVersion() const
{
	return jniVersion;
}
