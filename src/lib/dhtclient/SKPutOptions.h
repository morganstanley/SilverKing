////#pragma once
#ifndef SKPUTOPTIONS_H
#define SKPUTOPTIONS_H

#include <stdint.h>
#include <string>
using std::string;
#include <set>

#include "skconstants.h"
#include "skbasictypes.h"
#include "SKOperationOptions.h"
#include "SKVersionConstraint.h"


//class PutOptions;
class SKSecondaryTarget;
class SKOpTimeoutController;
class SKSecondaryTarget;

class SKPutOptions : public SKOperationOptions
{
public:
    SKAPI SKPutOptions * opTimeoutController(SKOpTimeoutController * opTimeoutController);
    SKAPI SKPutOptions * secondaryTargets(std::set<SKSecondaryTarget*> * secondaryTargets);
    SKAPI SKPutOptions * secondaryTargets(SKSecondaryTarget * secondaryTarget);
    SKAPI SKPutOptions * compression(SKCompression::SKCompression compression);
    SKAPI SKPutOptions * checksumType(SKChecksumType::SKChecksumType checksumType);
    SKAPI SKPutOptions * checksumCompressedValues(bool checksumCompressedValues );
    SKAPI SKPutOptions * version(int64_t version);
    SKAPI SKPutOptions * requiredPreviousVersion(int64_t requiredPreviousVersion);
    SKAPI SKPutOptions * fragmentationThreshold(int64_t fragmentationThreshold);
    SKAPI SKPutOptions * userData(SKVal * userData);

    SKAPI SKCompression::SKCompression getCompression() const;
    SKAPI SKChecksumType::SKChecksumType getChecksumType() const;
    SKAPI bool getChecksumCompressedValues() const;
    SKAPI int64_t getVersion() const;
    SKAPI int64_t getRequiredPreviousVersion() const;
    SKAPI int64_t getFragmentationThreshold() const;
    SKAPI SKVal * getUserData() const;
    
    SKAPI static SKPutOptions * parse(const char * def);
    SKAPI virtual bool equals(SKPutOptions * other) const;
    SKAPI virtual string toString() const;    
                
    SKAPI virtual ~SKPutOptions();
    SKAPI SKPutOptions(SKOpTimeoutController * opTimeoutController, 
        std::set<SKSecondaryTarget*> * secondaryTargets, 
        SKCompression::SKCompression compression, SKChecksumType::SKChecksumType checksumType,
        bool checksumCompressedValues, int64_t version, int64_t requiredPreviousVersion,
        int64_t fragmentationThreshold,
        SKVal * userData );

    SKPutOptions();
    SKPutOptions(void * pPutOptsImpl);  //FIXME:
    void * getPImpl();  //FIXME:

//private:
    void * pImpl;
};

#endif // SKPUTOPTIONS_H
