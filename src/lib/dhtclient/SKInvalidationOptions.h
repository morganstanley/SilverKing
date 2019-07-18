////#pragma once
#ifndef SKINVALIDATIONOPTIONS_H
#define SKINVALIDATIONOPTIONS_H

#include <stdint.h>
#include <string>
using std::string;
#include <set>

#include "skconstants.h"
#include "skbasictypes.h"
#include "SKVersionConstraint.h"
#include "SKPutOptions.h"


class SKSecondaryTarget;
class SKOpTimeoutController;
class SKSecondaryTarget;

class SKInvalidationOptions : public SKPutOptions
{
public:
    SKAPI SKInvalidationOptions * opTimeoutController(SKOpTimeoutController * opTimeoutController);
    SKAPI SKInvalidationOptions * secondaryTargets(std::set<SKSecondaryTarget*> * secondaryTargets);
    SKAPI SKInvalidationOptions * secondaryTargets(SKSecondaryTarget * secondaryTarget);
    SKAPI SKInvalidationOptions * version(int64_t version);
    SKAPI SKInvalidationOptions * requiredPreviousVersion(int64_t requiredPreviousVersion);
    SKAPI SKInvalidationOptions * lockSeconds(int16_t lockSeconds);

    SKAPI static SKInvalidationOptions * parse(const char * def);
    SKAPI virtual bool equals(SKInvalidationOptions * other) const;
    SKAPI virtual string toString() const;    
                
    SKAPI virtual ~SKInvalidationOptions();
    SKAPI SKInvalidationOptions(SKOpTimeoutController * opTimeoutController, 
        std::set<SKSecondaryTarget*> * secondaryTargets,
        int64_t version);
    SKAPI SKInvalidationOptions(SKOpTimeoutController * opTimeoutController, 
        std::set<SKSecondaryTarget*> * secondaryTargets,
        int64_t version, int64_t requiredPreviousVersion, int16_t lockSeconds);

    SKInvalidationOptions(void * pOptsImpl);  //FIXME:
    void * getPImpl();  //FIXME:

//private:
    void * pImpl;
};

#endif // SKINVALIDATIONOPTIONS_H
