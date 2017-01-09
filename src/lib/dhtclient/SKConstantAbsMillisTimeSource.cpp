#include "SKConstantAbsMillisTimeSource.h"

#include "jace/Jace.h"
using jace::java_new;
using namespace jace;
#include "jace/proxy/com/ms/silverking/time/ConstantAbsMillisTimeSource.h"
using jace::proxy::com::ms::silverking::time::ConstantAbsMillisTimeSource;


SKConstantAbsMillisTimeSource::SKConstantAbsMillisTimeSource(int64_t absMillisTime){
	pImpl = new ConstantAbsMillisTimeSource(java_new<ConstantAbsMillisTimeSource>(absMillisTime));
}


SKConstantAbsMillisTimeSource::SKConstantAbsMillisTimeSource(ConstantAbsMillisTimeSource * pConstantAbsMillisTimeSource) { //FIXME: ?
		pImpl = pConstantAbsMillisTimeSource;
}

SKConstantAbsMillisTimeSource::~SKConstantAbsMillisTimeSource() {
	if(pImpl) {
		delete pImpl;
		pImpl = NULL;
	}
};
