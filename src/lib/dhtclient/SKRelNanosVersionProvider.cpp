#include "SKRelNanosVersionProvider.h"
#include "SKRelNanosTimeSource.h"
#include "SKRelNanosAbsMillisTimeSource.h"

#include "jace/Jace.h"
using jace::java_new;
using jace::java_cast;
using namespace jace;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/RelNanosVersionProvider.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::RelNanosVersionProvider;
#include "jace/proxy/com/ms/silverking/time/RelNanosTimeSource.h"
using jace::proxy::com::ms::silverking::time::RelNanosTimeSource;
#include "jace/proxy/com/ms/silverking/time/RelNanosAbsMillisTimeSource.h"
using jace::proxy::com::ms::silverking::time::RelNanosAbsMillisTimeSource;


SKRelNanosVersionProvider::SKRelNanosVersionProvider(RelNanosVersionProvider * pRelNanosVersionProvider) 
	: SKVersionProvider(pRelNanosVersionProvider) {}; 
	
SKRelNanosVersionProvider::~SKRelNanosVersionProvider()
{
	if(pImpl!=NULL) {
		delete pImpl; 
		pImpl = NULL;
	}
}

SKRelNanosVersionProvider::SKRelNanosVersionProvider(SKRelNanosAbsMillisTimeSource * relNanosTimeSource){
	RelNanosAbsMillisTimeSource* pSource = dynamic_cast<RelNanosAbsMillisTimeSource *>(relNanosTimeSource->getPImpl());

	//pImpl = new RelNanosVersionProvider(java_new<RelNanosVersionProvider>(*pSource)); 
	RelNanosTimeSource ts = java_cast<RelNanosTimeSource>(*pSource);
	pImpl = new RelNanosVersionProvider(java_new<RelNanosVersionProvider>(ts)); 
}
