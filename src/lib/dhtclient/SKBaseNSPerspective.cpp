#include "SKBaseNSPerspective.h"
#include "SKNamespace.h"
#include "SKNamespacePerspectiveOptions.h"
#include "SKVersionConstraint.h"
#include "SKVersionProvider.h"

#include "jace/Jace.h"
using namespace jace;
#include "jace/proxy/types/JLong.h"
using jace::proxy::types::JLong;

#include "jace/proxy/java/lang/String.h"
using jace::proxy::java::lang::String;
#include "jace/proxy/com/ms/silverking/cloud/dht/NamespacePerspectiveOptions.h"
using jace::proxy::com::ms::silverking::cloud::dht::NamespacePerspectiveOptions;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/BaseNamespacePerspective.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::BaseNamespacePerspective;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/Namespace.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::Namespace;
#include "jace/proxy/com/ms/silverking/cloud/dht/VersionConstraint.h"
using jace::proxy::com::ms::silverking::cloud::dht::VersionConstraint;
#include "jace/proxy/com/ms/silverking/cloud/dht/VersionConstraint_Mode.h"
using jace::proxy::com::ms::silverking::cloud::dht::VersionConstraint_Mode;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/VersionProvider.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::VersionProvider;

SKBaseNSPerspective::~SKBaseNSPerspective(){};

string SKBaseNSPerspective::getName(){
	BaseNamespacePerspective * pBnsp = (BaseNamespacePerspective*)getPImpl();
	string name = (string)pBnsp->getName();
	return name;
}
SKNamespace * SKBaseNSPerspective::getNamespace(){
	BaseNamespacePerspective * pBnsp = (BaseNamespacePerspective*)getPImpl();
	SKNamespace * ns = new SKNamespace(new Namespace(pBnsp->getNamespace()));
	return ns;
	
}
SKNamespacePerspectiveOptions * SKBaseNSPerspective::getOptions(){
	BaseNamespacePerspective * pBnsp = (BaseNamespacePerspective*)getPImpl();
	SKNamespacePerspectiveOptions * nspo = 
		new SKNamespacePerspectiveOptions(new NamespacePerspectiveOptions(
				java_cast<NamespacePerspectiveOptions>(pBnsp->getOptions())));
	return nspo;
}

void SKBaseNSPerspective::setOptions(SKNamespacePerspectiveOptions * nspOptions) {
	NamespacePerspectiveOptions* pNspo = (NamespacePerspectiveOptions*)(nspOptions->getPImpl());
	((BaseNamespacePerspective*)this->getPImpl())->setOptions(*pNspo);
}

void SKBaseNSPerspective::setDefaultRetrievalVersionConstraint(SKVersionConstraint * vc) {
	VersionConstraint* pVc = (VersionConstraint*)(vc->getPImpl());
	((BaseNamespacePerspective*)this->getPImpl())->setDefaultRetrievalVersionConstraint(*pVc);
}

void SKBaseNSPerspective::setDefaultVersionProvider(SKVersionProvider * versionProvider) {
	VersionProvider* pVp = (VersionProvider*)(versionProvider->getPImpl());
	((BaseNamespacePerspective*)this->getPImpl())->setDefaultVersionProvider(*pVp);
}

void SKBaseNSPerspective::setDefaultVersion(int64_t version) {
	((BaseNamespacePerspective*)this->getPImpl())->setDefaultVersion( JLong(version));
}

void SKBaseNSPerspective::close() {
	((BaseNamespacePerspective*)this->getPImpl())->close();
}

