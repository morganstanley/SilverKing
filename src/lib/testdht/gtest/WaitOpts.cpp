
#include "WaitOpts.h"

SKWaitOptions*
WaitOpts::getWaitOptions(SKSyncNSPerspective* snsp, SKRetrievalType retrievalType, int valueVersion, int timeOut, int threshold) {
  SKWaitOptions* pWaitOpt = NULL;
  SKVersionConstraint* pVersionConstraint = NULL;
  if(valueVersion)
    pVersionConstraint = SKVersionConstraint::maxBelowOrEqual(valueVersion);
  else
    pVersionConstraint = new SKVersionConstraint(LLONG_MIN, LLONG_MAX, GREATEST);

  SKTimeoutResponse::SKTimeoutResponse timeoutResponse = SKTimeoutResponse::EXCEPTION;

  pWaitOpt = snsp->getOptions()->getDefaultWaitOptions()->retrievalType(retrievalType)->versionConstraint(pVersionConstraint)->timeoutSeconds(timeOut)->threshold(threshold)->timeoutResponse(timeoutResponse);

  return pWaitOpt;
}

SKWaitOptions*
WaitOpts::getWaitOptions(SKAsyncNSPerspective* ansp, SKRetrievalType retrievalType, int valueVersion, int timeOut, int threshold) {
  SKWaitOptions* pWaitOpt = NULL;
  SKVersionConstraint* pVersionConstraint = NULL;
  if(valueVersion)
    pVersionConstraint = SKVersionConstraint::maxBelowOrEqual(valueVersion);
  else
    pVersionConstraint = new SKVersionConstraint(LLONG_MIN, LLONG_MAX, GREATEST);

  SKTimeoutResponse::SKTimeoutResponse timeoutResponse = SKTimeoutResponse::EXCEPTION;

  pWaitOpt = ansp->getOptions()->getDefaultWaitOptions()->retrievalType(retrievalType)->versionConstraint(pVersionConstraint)->timeoutSeconds(timeOut)->threshold(threshold)->timeoutResponse(timeoutResponse);

  return pWaitOpt;
}

