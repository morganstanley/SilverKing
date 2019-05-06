#include "SKSystemTimeSource.h"

#include "jace/Jace.h"
using jace::java_new;
using jace::java_cast;
using namespace jace;
#include "jace/proxy/com/ms/silverking/time/SystemTimeSource.h"
using jace::proxy::com::ms::silverking::time::SystemTimeSource;


SKSystemTimeSource::SKSystemTimeSource() : SKRelNanosAbsMillisTimeSource(NULL) {
	pImpl = new SystemTimeSource(java_new<SystemTimeSource>());
}

SKSystemTimeSource::SKSystemTimeSource(int64_t nanosOriginTime) : SKRelNanosAbsMillisTimeSource(NULL) {
	pImpl = new SystemTimeSource(java_cast<SystemTimeSource>(SystemTimeSource::createWithMillisOrigin(nanosOriginTime)));
}

SKSystemTimeSource::SKSystemTimeSource(SystemTimeSource * pSystemTimeSource) : SKRelNanosAbsMillisTimeSource(NULL) { //FIXME: ?
	if(pSystemTimeSource)
		pImpl = pSystemTimeSource;
}

SKSystemTimeSource::~SKSystemTimeSource() {
	if(pImpl) {
		SystemTimeSource* pTs = dynamic_cast<SystemTimeSource*>(pImpl);
		delete pTs;
		pImpl = NULL;
	}
};

// Ideally, we would just inherit. For now, just implement directly
// until the new generation code makes all of this obsolete.
int64_t SKSystemTimeSource::absTimeNanos() 
{
	if(pImpl) {
		SystemTimeSource* pTs = dynamic_cast<SystemTimeSource*>(pImpl);  
		int64_t time = (int64_t) pTs->absTimeNanos();
		return time;
	}
	throw std::exception();
}
