
#include "GetOpts.h"

SKVersionConstraint*
GetOpts::getVersionConstraint(int valueVersion) {
  SKVersionConstraint* pVc = NULL;
  // valueVersion is initialized to 0 or overwriten by cmd line opt '-i' in main()
  if ( valueVersion ) {
    pVc = SKVersionConstraint::maxBelowOrEqual(valueVersion);
  } else {
    pVc = new SKVersionConstraint(LLONG_MIN, LLONG_MAX, GREATEST);
  }
  return pVc;
}

SKGetOptions*
GetOpts::getGetOptions(SKSyncNSPerspective* snsp, SKRetrievalType retrievalType, int valueVersion) {
  SKGetOptions* pGetOpt = NULL;
  // valueVersion is initialized to 0 or overwriten by cmd line opt '-i' in main()
  SKVersionConstraint* pVc = SKVersionConstraint::maxBelowOrEqual(valueVersion);
  // retrievalType is initialized to VALUE_AND_META_DATA or overwriten by cmd line opt '-m' in main()
  pGetOpt = snsp->getOptions()->getDefaultGetOptions()->retrievalType(retrievalType)->versionConstraint(pVc);
  return pGetOpt;
}

SKGetOptions*
GetOpts::getMetaGetOptions(SKSyncNSPerspective* snsp, SKRetrievalType retrievalType, int valueVersion) {
  SKGetOptions* pGetOpt = NULL;
  SKVersionConstraint* pVc = getVersionConstraint(valueVersion);

  // retrievalType is initialized to VALUE_AND_META_DATA or overwriten by cmd line opt '-m' in main()
  pGetOpt = snsp->getOptions()->getDefaultGetOptions()->retrievalType(retrievalType)->versionConstraint(pVc);
  return pGetOpt;
}

SKGetOptions*
GetOpts::getGetOptions(SKAsyncNSPerspective* ansp, SKRetrievalType retrievalType, int valueVersion) {
  SKGetOptions* pGetOpt = NULL;
  // valueVersion is initialized to 0 or overwriten by cmd line opt '-i' in main()
  SKVersionConstraint* pVc = getVersionConstraint(valueVersion);
  // retrievalType is initialized to VALUE_AND_META_DATA or overwriten by cmd line opt '-m' in main()
  pGetOpt = ansp->getOptions()->getDefaultGetOptions()->retrievalType(retrievalType)->versionConstraint(pVc);
  return pGetOpt;
}

SKGetOptions*
GetOpts::getMetaGetOptions(SKAsyncNSPerspective* ansp, SKRetrievalType retrievalType, int valueVersion) {
  SKGetOptions* pGetOpt = NULL;
  SKVersionConstraint* pVc = getVersionConstraint(valueVersion);

  // retrievalType is initialized to VALUE_AND_META_DATA or overwriten by cmd line opt '-m' in main()
  pGetOpt = ansp->getOptions()->getDefaultGetOptions()->retrievalType(retrievalType)->versionConstraint(pVc);
  return pGetOpt;
}

