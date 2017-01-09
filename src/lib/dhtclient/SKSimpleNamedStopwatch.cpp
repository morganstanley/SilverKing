#include "SKSimpleNamedStopwatch.h"
#include "SKRelNanosTimeSource.h"
#include "SKRelNanosAbsMillisTimeSource.h"
#include <string>
using std::string;

#include "jace/Jace.h"
using jace::java_new;
using jace::java_cast;
using namespace jace;
#include "jace/proxy/java/lang/String.h"
using jace::proxy::java::lang::String;
#include "jace/proxy/com/ms/silverking/time/SimpleNamedStopwatch.h"
using jace::proxy::com::ms::silverking::time::SimpleNamedStopwatch;
#include "jace/proxy/com/ms/silverking/time/RelNanosTimeSource.h"
using jace::proxy::com::ms::silverking::time::RelNanosTimeSource;
#include "jace/proxy/com/ms/silverking/time/RelNanosAbsMillisTimeSource.h"
using jace::proxy::com::ms::silverking::time::RelNanosAbsMillisTimeSource;

SKSimpleNamedStopwatch::SKSimpleNamedStopwatch(): SKSimpleStopwatch((SimpleStopwatch*)NULL) {
	pImpl = new SimpleNamedStopwatch(java_new<SimpleNamedStopwatch>());
}

SKSimpleNamedStopwatch::SKSimpleNamedStopwatch(const char * name){
	pImpl = new SimpleNamedStopwatch(java_new<SimpleNamedStopwatch>(java_new<String>((char*)name)));
}

SKSimpleNamedStopwatch::SKSimpleNamedStopwatch(SKRelNanosAbsMillisTimeSource * relNanosTimeSource, const char * name){
	RelNanosAbsMillisTimeSource * pRelNanosTimeSource =  dynamic_cast<RelNanosAbsMillisTimeSource *>(relNanosTimeSource->getPImpl());
	RelNanosTimeSource ts = java_cast<RelNanosTimeSource>(*pRelNanosTimeSource);
	pImpl = new SimpleNamedStopwatch(java_new<SimpleNamedStopwatch>(ts, java_new<String>((char*)name)));
}

//impl
SKSimpleNamedStopwatch::SKSimpleNamedStopwatch(SimpleNamedStopwatch * pSimpleNamedStopwatch) {
	if(pSimpleNamedStopwatch)
		pImpl = pSimpleNamedStopwatch;
} 
	
SKSimpleNamedStopwatch::~SKSimpleNamedStopwatch()
{
	if(pImpl!=NULL) {
		SimpleNamedStopwatch * pSimpleNamedStopwatch = dynamic_cast<SimpleNamedStopwatch*>(pImpl);
		delete pSimpleNamedStopwatch; 
		pImpl = NULL;
	}
}

string SKSimpleNamedStopwatch::getName() {
	return (string)(dynamic_cast<SimpleNamedStopwatch*>(pImpl)->getName());
}
