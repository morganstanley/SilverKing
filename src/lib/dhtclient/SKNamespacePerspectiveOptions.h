////#pragma once
#ifndef SKNAMESPACEPERSPECTIVEOPTIONS_H
#define SKNAMESPACEPERSPECTIVEOPTIONS_H

#include <cstddef>
#include <string>
using std::string;

#include "skconstants.h"
#include "SKPutOptions.h"
#include "SKInvalidationOptions.h"
#include "SKGetOptions.h"
#include "SKWaitOptions.h"
#include "SKVersionProvider.h"


//class NamespacePerspectiveOptions;

class SKNamespacePerspectiveOptions
{
public:
    
/* what do SK return : jclass / Class / Java class name/ something else ?
    Class<K> getKeyClass();
    Class<V> getValueClass();
*/

    SKAPI SKKeyDigestType::SKKeyDigestType getKeyDigestType();
    SKAPI SKPutOptions * getDefaultPutOptions();
    SKAPI SKInvalidationOptions * getDefaultInvalidationOptions();
    SKAPI SKGetOptions * getDefaultGetOptions();
    SKAPI SKWaitOptions * getDefaultWaitOptions();
    SKAPI SKVersionProvider * getDefaultVersionProvider();
    
    SKAPI SKNamespacePerspectiveOptions * keyDigestType(SKKeyDigestType::SKKeyDigestType keyDigestType);
    SKAPI SKNamespacePerspectiveOptions * defaultPutOptions(SKPutOptions * defaultPutOptions);
    SKAPI SKNamespacePerspectiveOptions * defaultInvalidationOptions(SKInvalidationOptions * defaultInvalidationOptions);
    SKAPI SKNamespacePerspectiveOptions * defaultGetOptions(SKGetOptions * defaultGetOptions);
    SKAPI SKNamespacePerspectiveOptions * defaultWaitOptions(SKWaitOptions * defaultWaitOptions);
    SKAPI SKNamespacePerspectiveOptions * defaultVersionProvider(SKVersionProvider * defaultVersionProvider);

    SKAPI SKNamespacePerspectiveOptions * parse(const char * def); 
    SKAPI string toString();

    SKAPI ~SKNamespacePerspectiveOptions();
    SKAPI SKNamespacePerspectiveOptions(/* KeyClass k, ValueClass v, */ SKKeyDigestType::SKKeyDigestType keyDigestType, 
                                        SKPutOptions * defaultPutOptions, 
                                        SKInvalidationOptions * defaultInvalidationOptions,
                                        SKGetOptions * defaultGetOptions, 
                                        SKWaitOptions * defaultWaitOptions, SKVersionProvider * defaultVersionProvider );
    SKNamespacePerspectiveOptions(void * pOpt);
    void * getPImpl();
private:
    void * pImpl;
};

#endif // SKNAMESPACEPERSPECTIVEOPTIONS_H
