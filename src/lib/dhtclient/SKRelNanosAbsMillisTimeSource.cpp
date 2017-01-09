#include "SKRelNanosAbsMillisTimeSource.h"
#include <exception>

#include "jace/proxy/com/ms/silverking/time/RelNanosAbsMillisTimeSource.h"
using jace::proxy::com::ms::silverking::time::RelNanosAbsMillisTimeSource;


SKRelNanosAbsMillisTimeSource::SKRelNanosAbsMillisTimeSource() {};

SKRelNanosAbsMillisTimeSource::SKRelNanosAbsMillisTimeSource(RelNanosAbsMillisTimeSource * pRelNanosAbsMillisTimeSource) 
	: SKAbsMillisTimeSource(NULL)
{ //FIXME: ?
	if(pRelNanosAbsMillisTimeSource)
		pImpl = pRelNanosAbsMillisTimeSource;
}

SKRelNanosAbsMillisTimeSource::~SKRelNanosAbsMillisTimeSource() {
	if(pImpl) {
		RelNanosAbsMillisTimeSource* pTs = dynamic_cast<RelNanosAbsMillisTimeSource*>(pImpl);  //FIXME:
		delete pTs;
		pImpl = NULL;
	}
};

int64_t SKRelNanosAbsMillisTimeSource::relTimeNanos() 
{
	if(pImpl) {
		RelNanosAbsMillisTimeSource* pTs = dynamic_cast<RelNanosAbsMillisTimeSource*>(pImpl);  
		int64_t reltime = (int64_t) pTs->relTimeNanos();
		return reltime;
	}
	throw std::exception();
}
