#include "PutOpts.h"

SKPutOptions*
PutOpts::getPutOptions(SKNamespacePerspectiveOptions* pNspOptions, string namesp, SKCompression::SKCompression compressType, int valueVersion, SKVal* userData) {
  set<SKSecondaryTarget*> * pTargets = new set<SKSecondaryTarget*>;
  SKPutOptions* pPutOpt = NULL;
  try {
    bool cksumCompressedValues = false;
    pPutOpt = pNspOptions->getDefaultPutOptions()->compression(compressType)->checksumType(SKChecksumType::NONE)->checksumCompressedValues(cksumCompressedValues)->version(valueVersion)->secondaryTargets(pTargets)->userData(userData);
    sk_destroy_val(&userData);
  } catch (SKClientException & e) {
    exhandler( "SKClientException in openSyncNamespacePerspective ", __FILE__, __LINE__, namesp.c_str() );
    return NULL;
  }
  return pPutOpt;
}

