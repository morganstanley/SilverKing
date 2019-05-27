#include "SKSimpleStopwatch.h"
#include "SKRelNanosTimeSource.h"
#include "SKRelNanosAbsMillisTimeSource.h"

#include <string>
using std::string;

#include "jace/Jace.h"
using jace::java_new;
using jace::java_cast;
using namespace jace;
#include "jace/proxy/com/ms/silverking/time/SimpleStopwatch.h"
using jace::proxy::com::ms::silverking::time::SimpleStopwatch;
#include "jace/proxy/com/ms/silverking/time/RelNanosTimeSource.h"
using jace::proxy::com::ms::silverking::time::RelNanosTimeSource;
#include "jace/proxy/com/ms/silverking/time/RelNanosAbsMillisTimeSource.h"
using jace::proxy::com::ms::silverking::time::RelNanosAbsMillisTimeSource;

SKSimpleStopwatch::SKSimpleStopwatch() : SKStopwatchBase(NULL) {
    pImpl = new SimpleStopwatch(java_new<SimpleStopwatch>());
}

SKSimpleStopwatch::SKSimpleStopwatch(SKRelNanosAbsMillisTimeSource * relNanosTimeSource) {
    RelNanosAbsMillisTimeSource * pRelNanosTimeSource =  dynamic_cast<RelNanosAbsMillisTimeSource *>(relNanosTimeSource->getPImpl());
    //RelNanosTimeSource ts = java_cast<RelNanosTimeSource>(*pRelNanosTimeSource);
    pImpl = new SimpleStopwatch(java_new<SimpleStopwatch>(*pRelNanosTimeSource));
}


//impl
SKSimpleStopwatch::SKSimpleStopwatch(SimpleStopwatch * pSimpleStopwatch) {
    if(pSimpleStopwatch)
        pImpl = pSimpleStopwatch;
} 
    
SKSimpleStopwatch::~SKSimpleStopwatch()
{
    if(pImpl!=NULL) {
        SimpleStopwatch * pSimpleStopwatch = dynamic_cast<SimpleStopwatch*>(pImpl);
        delete pSimpleStopwatch; 
        pImpl = NULL;
    }
}
