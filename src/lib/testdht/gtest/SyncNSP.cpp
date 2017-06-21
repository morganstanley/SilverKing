#include "SyncNSP.h"

SKSyncNSPerspective*
SyncNSP::getSyncNSPerspective(SKSession* session, string namesp, SKNamespacePerspectiveOptions* pNspOpts) {
  SKSyncNSPerspective* snsp = NULL;
  try {
    snsp = session->openSyncNamespacePerspective(namesp.c_str(), pNspOpts);
  } catch (SKClientException & e) {
    exhandler( "SKClientException in openSyncNamespacePerspective ", __FILE__, __LINE__, namesp.c_str() );
    return NULL;
  }
  return snsp;
}

