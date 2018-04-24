#include "NamespaceHandling.h"

#include "PutOpts.h"
#include "SyncNSP.h"
#include "GetOpts.h"

#include "SKAbsMillisVersionProvider.h"
#include "SKSystemTimeSource.h"

NamespaceHandling::NamespaceHandling(const string& gc, const string& h, const string& n, const string& log, int verb, const string& nsOpts, const string& jvmOpts, SKCompression::SKCompression compress, long long int version, SKRetrievalType retrieval)
 : DhtAction(gc, h, n, log, verb, nsOpts, jvmOpts, compress, version, retrieval)
 , pChildNamespace()
 {
 }

NamespaceHandling::~NamespaceHandling() {
  if(pChildNamespace)
    delete pChildNamespace;
}

SKNamespace*
NamespaceHandling::cloneNamespace(SKSession* ssn, const string& parent) {
  SKNamespace* pParentNs = ssn->getNamespace(parent.data());
  SKNamespaceOptions* parentNsOpts = pParentNs->getOptions();

  try {
    SKAbsMillisVersionProvider* versionProvider = new SKAbsMillisVersionProvider(new SKSystemTimeSource);
    long childVersion = versionProvider->getVersion();
    string child = parent + "_" + "child" + "_" + to_string(childVersion);
    if(parentNsOpts->getVersionMode() == CLIENT_SPECIFIED) {
      pChildNamespace = pParentNs->clone(child.data(), childVersion);
    } else {
      pChildNamespace = pParentNs->clone(child.data());
    }
  } catch ( SKNamespaceLinkException& e ){
    cout << "caught SKNamespaceLinkException in cloneNamespace() in " << __FILE__ << ":" << __LINE__ << " exception: " << e.what() << endl;
  } catch (SKClientException& ce ) {
    exhandler("SKClientException in setnso", __FILE__, __LINE__, ns.data());
    cout << "caught SKClientException in cloneNamespace() in " << __FILE__ << ":" << __LINE__ << " exception: " << ce.what() << endl;
  } catch(std::exception& e ){
    cout << "caught exception in cloneNamespace() in " << __FILE__ << ":" << __LINE__ << " exception: " << e.what() << endl;
  }
  return pChildNamespace;
}


SKNamespaceOptions*
NamespaceHandling::setNamespaceOptions(SKNamespaceOptions* nsOpt, SKStorageType::SKStorageType storageType, SKConsistency consistencyProtocol, SKVersionMode versionMode, int segmentSize) {
  try {
    nsOpt->storageType(storageType); // or e.g. SKStorageType::RAM
    nsOpt->consistencyProtocol(consistencyProtocol);
    nsOpt->versionMode(versionMode);
    nsOpt->segmentSize(segmentSize);
    //nsOpt->allowLinks(link);
    string pOptStr = nsOpt->toString();
  } catch (SKClientException & ce ) {
    exhandler("SKClientException in setnso", __FILE__, __LINE__, ns.data());
  }
  return nsOpt;
}

SKNamespace*
NamespaceHandling::getChildNamespace() {
  return pChildNamespace;
}
