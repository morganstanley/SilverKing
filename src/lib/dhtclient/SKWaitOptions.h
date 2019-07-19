#ifndef SKWAITOPTIONS_H
#define SKWAITOPTIONS_H

#include <set>
#include "skconstants.h"
#include "skbasictypes.h"
#include "SKRetrievalOptions.h"

class SKVersionConstraint;
class SKWaitForTimeoutController;
class SKSecondaryTarget;

class SKWaitOptions : public SKRetrievalOptions
{
public:
    SKAPI SKWaitOptions * opTimeoutController(SKOpTimeoutController * opTimeoutController);
    SKAPI SKWaitOptions * secondaryTargets(std::set<SKSecondaryTarget*> * secondaryTargets);
    SKAPI SKWaitOptions * retrievalType(SKRetrievalType retrievalType); 
    SKAPI SKWaitOptions * versionConstraint(SKVersionConstraint * versionConstraint);
    SKAPI SKWaitOptions * nonExistenceResponse(SKNonExistenceResponse::SKNonExistenceResponse nonExistenceResponse);
    SKAPI SKWaitOptions * verifyChecksums(bool verifyChecksums);
    SKAPI SKWaitOptions * returnInvalidations(bool returnInvalidations);
    SKAPI SKWaitOptions * updateSecondariesOnMiss(bool updateSecondariesOnMiss);
    SKAPI SKWaitOptions * timeoutSeconds(int timeoutSeconds);
    SKAPI SKWaitOptions * threshold(int threshold);
    SKAPI SKWaitOptions * timeoutResponse(SKTimeoutResponse::SKTimeoutResponse timeoutResponse);
    
    SKAPI int getTimeoutSeconds();
    SKAPI int getThreshold();
    SKAPI SKTimeoutResponse::SKTimeoutResponse getTimeoutResponse();
    SKAPI bool hasTimeout();
    
    SKAPI static SKWaitOptions * parse(const char * def);
    SKAPI virtual bool equals(SKWaitOptions * other) const;
    SKAPI virtual string toString();

    SKAPI virtual ~SKWaitOptions();
    SKAPI SKWaitOptions(SKOpTimeoutController * opTimeoutController, 
            std::set<SKSecondaryTarget*> * secondaryTargets,
            SKRetrievalType retrievalType, SKVersionConstraint *versionConstraint, 
            SKNonExistenceResponse::SKNonExistenceResponse nonExistenceResponse, 
            bool verifyChecksums, 
            bool returnInvalidations,
            bool updateSecondariesOnMiss,
            int timeoutSeconds, int threshold, 
            SKTimeoutResponse::SKTimeoutResponse timeoutResponse);

    SKWaitOptions(void * pOpt);
    void * getPImpl();
protected:
    
};

#endif // SKWAITOPTIONS_H
