#include "SKAbsMillisTimeSource.h"

#include "jace/proxy/com/ms/silverking/time/AbsMillisTimeSource.h"
using jace::proxy::com::ms::silverking::time::AbsMillisTimeSource;


int64_t SKAbsMillisTimeSource::absTimeMillis(){
	int64_t abstime = (int64_t)((AbsMillisTimeSource*)this->getPImpl())->absTimeMillis();
	return abstime;
}

int SKAbsMillisTimeSource::relMillisRemaining(int64_t absDeadlineMillis){
	int remainingTime = (int)((AbsMillisTimeSource*)this->getPImpl())->relMillisRemaining(absDeadlineMillis);
	return remainingTime;
}


SKAbsMillisTimeSource::SKAbsMillisTimeSource(AbsMillisTimeSource * pAbsMillisTimeSource) { //FIXME: ?
	if(pAbsMillisTimeSource)
		pImpl = pAbsMillisTimeSource;
}

SKAbsMillisTimeSource::~SKAbsMillisTimeSource() {
	if(pImpl) {
		delete pImpl;
		pImpl = NULL;
	}
};

AbsMillisTimeSource * SKAbsMillisTimeSource::getPImpl(){
	return pImpl;
}
