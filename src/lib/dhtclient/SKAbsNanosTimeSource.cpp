#include "SKAbsNanosTimeSource.h"
#include "jace/proxy/com/ms/silverking/time/AbsNanosTimeSource.h"
using jace::proxy::com::ms::silverking::time::AbsNanosTimeSource;


int64_t SKAbsNanosTimeSource::getNanosOriginTime(){
	int64_t otime = (int64_t)((AbsNanosTimeSource*)this->getPImpl())->getNanosOriginTime();
	return otime;
}

int64_t SKAbsNanosTimeSource::absTimeNanos(){
	int64_t abstime = (int64_t)((AbsNanosTimeSource*)this->getPImpl())->absTimeNanos();
	return abstime;
}

int64_t SKAbsNanosTimeSource::relNanosRemaining(int64_t absDeadlineNanos){
	int64_t nanos = (int64_t)((AbsNanosTimeSource*)this->getPImpl())->relNanosRemaining(absDeadlineNanos);
	return nanos;
}

SKAbsNanosTimeSource::SKAbsNanosTimeSource(void * pAbsNanosTimeSource) { //FIXME: ?
	if(pAbsNanosTimeSource)
		_pImpl = pAbsNanosTimeSource;
}

SKAbsNanosTimeSource::~SKAbsNanosTimeSource() {
	if(_pImpl) {
		AbsNanosTimeSource* pTs = (AbsNanosTimeSource*) _pImpl;
		delete pTs;
		_pImpl = NULL;
	}
};

void * SKAbsNanosTimeSource::getPImpl(){
	return _pImpl;
}
