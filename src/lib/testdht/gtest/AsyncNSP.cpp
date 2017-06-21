#include "AsyncNSP.h"

SKAsyncNSPerspective*
AsyncNSP::getAsyncNSPerspective(SKSession* session, string namesp, SKNamespacePerspectiveOptions* pNspOpts) {
  SKAsyncNSPerspective* ansp = NULL;
  try {
    ansp = session->openAsyncNamespacePerspective(namesp.c_str(), pNspOpts);
  } catch (SKClientException & e) {
    exhandler( "SKClientException in openAsyncNamespacePerspective ", __FILE__, __LINE__, namesp.c_str() );
    return NULL;
  }
  return ansp;
}

