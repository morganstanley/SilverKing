
#include "DhtAction.h"
#include "PutOpts.h"

DhtAction::DhtAction(const string& gc, const string& h, const string& n, const string& log, int verb, const string& nsOpts, const string& jvmOpts, SKCompression::SKCompression compress, long long int version, SKRetrievalType retrieval)
  : gcName(gc)
  , host(h)
  , action()
  , ns(n)
  , logfile(log)
  , verbose(verb)
  , nsOptions(nsOpts)
  , jvmOptions(jvmOpts)
  , compressType(compress)
  , valueVersion(version)
  , retrievalType(retrieval)
  , clientInited(false)
  , client()
  , pGC()
  , pCdc()
  , sessionOption()
  , session()
  , pNSO()
  , pNamespace()
  , pPutOpt()
  , compress()
  , pNspOptions()
  , userData()
  {
    if(!logfile.empty()) {
      SKClient::setLogFile(logfile.data());
    }
    logLevel = (verbose)? LVL_ALL : LVL_OFF;
   
    if(nsOptions.empty()) {
      // from "testdht.cpp"
      //nsOptions = "versionMode=SINGLE_VERSION,storageType=FILE,consistencyProtocol=TWO_PHASE_COMMIT";
      // from "exampledht.cpp"
      nsOptions = "versionMode=CLIENT_SPECIFIED,storageType=FILE,consistencyProtocol=TWO_PHASE_COMMIT";
    }
    userData = sk_create_val();
  }

DhtAction::~DhtAction() {

}

bool
DhtAction::isCompressSet() {
  return compressType != SKCompression::NONE;
}

bool
DhtAction::isValueVersionSet() {
  return valueVersion != 0;
}

bool
DhtAction::initClient() {
  clientInited = SKClient::init(logLevel, jvmOptions.c_str());
  return clientInited;
}

SKClient*
DhtAction::getClient() {
  client = SKClient::getClient(logLevel, jvmOptions.c_str());
  return client;
}

SKGridConfiguration*
DhtAction::getGridConfiguration() {
  pGC = SKGridConfiguration::parseFile(gcName.c_str());
  return pGC;
}

SKClientDHTConfiguration*
DhtAction::getClientDHTConfiguration()
{
  pCdc = pGC->getClientDHTConfiguration();
  return pCdc;
}

SKSessionOptions*
DhtAction::getSessionOptions() {
  sessionOption = new SKSessionOptions(pCdc, host.c_str());
  return sessionOption;
}

SKSession*
DhtAction::openSession() {
  session = client->openSession(sessionOption);
  return session;
}

SKSession*
DhtAction::getSession() {
  return session;
}

SKNamespaceOptions*
DhtAction::getNamespaceOpts() {
  return pNSO;
}

SKNamespaceOptions*
DhtAction::getNamespaceOptions() {
  if(!nsOptions.empty() && nsOptions.length() > 1) {
    pNSO = SKNamespaceOptions::parse( nsOptions.c_str() );
  }
 
  return pNSO;
}

SKNamespaceOptions*
DhtAction::getNamespaceOptions(const string& optStr) {
  SKNamespaceOptions* nmspOpts = NULL;
  if(!optStr.empty() && optStr.length() > 1) {
    nmspOpts = SKNamespaceOptions::parse(optStr.c_str());
  }
 
  return nmspOpts;
}

string
DhtAction::getNamespaceOptStr() {
  return nsOptions;
}

string
DhtAction::getNamespaceName() {
  return ns;
}

SKNamespace*
DhtAction::getNamespace() {
  try {
    pNamespace = session->getNamespace(ns.c_str());
  } catch(std::exception &e ){
    try {
      if(getNamespaceOptions()) { 
        // if pNSO is not NULL, i.e. string nsOptions is not empty
        pNamespace = session->createNamespace( ns.c_str(), pNSO );
      } else {
        pNamespace = session->createNamespace( ns.c_str() );
      }
    } catch ( SKNamespaceCreationException & e ){
      fprintf( stderr,  "caught SKNamespaceCreationException in %s:%d\n%s\n", __FILE__, __LINE__, e.what() );
    } catch (...){
      exhandler( "caught in createNamespace", __FILE__, __LINE__, ns.c_str() );
    }
  }

  return pNamespace;
}

SKNamespace*
DhtAction::getNamespace(const string& nmspName, SKNamespaceOptions* nsOpt) {
  SKNamespace* nmsp = NULL;
  try {
    nmsp = session->getNamespace(nmspName.c_str());
  } catch(std::exception &e){
    try {
      if(nsOpt) { 
        // if pNSO is not NULL, i.e. string nsOptions is not empty
        nmsp = session->createNamespace(nmspName.c_str(), nsOpt);
      } else {
        nmsp = session->createNamespace(nmspName.c_str());
      }
    } catch ( SKNamespaceCreationException & e ){
      fprintf( stderr,  "caught SKNamespaceCreationException in %s:%d\n%s\n", __FILE__, __LINE__, e.what() );
    } catch (...){
      exhandler( "caught in createNamespace", __FILE__, __LINE__, ns.c_str() );
    }
  }

  return nmsp;
}

SKNamespacePerspectiveOptions*
DhtAction::getNSPOptions() {
  try {
    pNspOptions = pNamespace->getDefaultNSPOptions();
  } catch (SKClientException & e) {
    exhandler("SKClientException in getNSPOptions ", __FILE__, __LINE__, ns.c_str() );
  }
  return pNspOptions;
}

// apply SKPutOptions on SKNamespacePerspectiveOptions with new value
// to be called before calling getSyncNSPerspective()
SKNamespacePerspectiveOptions*
DhtAction::applyPutOptsOnNSPOptions(SKPutOptions* pOpts) {
  pNspOptions = pNspOptions->defaultPutOptions(pOpts);
  return pNspOptions;
}

SKPutOptions*
DhtAction::getPutOptions() {
  pPutOpt = PutOpts::getPutOptions(pNspOptions, ns, compressType, valueVersion, userData);
  applyPutOptsOnNSPOptions(pPutOpt);
  return pPutOpt;
}

long long int
DhtAction::getValueVersion() {
  return valueVersion;
}

SKPutOptions*
DhtAction::getPutOpt() {
  return pPutOpt;
}
