#include "SKRelNanosTimeSource.h"
#include "jace/proxy/com/ms/silverking/time/RelNanosTimeSource.h"
using jace::proxy::com::ms::silverking::time::RelNanosTimeSource;


int64_t SKRelNanosTimeSource::relTimeNanos(){
	int64_t reltime = (int64_t)((RelNanosTimeSource*)this->getPImpl())->relTimeNanos();
	return reltime;
}


SKRelNanosTimeSource::SKRelNanosTimeSource(void * pRelNanosTimeSource) { //FIXME: ?
	if(pRelNanosTimeSource)
		pImpl = pRelNanosTimeSource;
}

SKRelNanosTimeSource::~SKRelNanosTimeSource() {
	if(pImpl) {
		RelNanosTimeSource* pTs = (RelNanosTimeSource*) pImpl;
		delete pTs;
		pImpl = NULL;
	}
};

void * SKRelNanosTimeSource::getPImpl(){
	return pImpl;
}
