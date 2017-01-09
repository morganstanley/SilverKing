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
