#ifndef SKNAMESPACEOPTIONS_H
#define SKNAMESPACEOPTIONS_H

#include "skconstants.h"

class SKPutOptions;
class SKInvalidationOptions;
class SKGetOptions;
class SKWaitOptions;

class SKNamespaceOptions 
{
public:
	SKAPI SKNamespaceOptions(SKStorageType::SKStorageType storageType, SKConsistency consistencyProtocol, 
						SKVersionMode versionMode, SKRevisionMode revisionMode, 
                        SKPutOptions * defaultPutOptions, SKInvalidationOptions * defaultInvalidationOptions,
						SKGetOptions * defaultGetOptions, SKWaitOptions * defaultWaitOptions,
						int secondarySyncIntervalSeconds, int segmentSize, int maxValueSize, bool allowLinks );

    SKAPI SKNamespaceOptions * storageType(SKStorageType::SKStorageType storageType);
    SKAPI SKNamespaceOptions * consistencyProtocol(SKConsistency consistencyProtocol);
    SKAPI SKNamespaceOptions * versionMode(SKVersionMode versionMode);
	SKAPI SKNamespaceOptions * revisionMode(SKRevisionMode revisionMode);
    SKAPI SKNamespaceOptions * defaultPutOptions(SKPutOptions * defaultPutOptions);
    SKAPI SKNamespaceOptions * defaultInvalidationOptions(SKInvalidationOptions * defaultInvalidationOptions);
    SKAPI SKNamespaceOptions * defaultGetOptions(SKGetOptions * defaultGetOptions);
    SKAPI SKNamespaceOptions * defaultWaitOptions(SKWaitOptions * defaultWaitOptions);
	SKAPI SKNamespaceOptions * secondarySyncIntervalSeconds(int secondarySyncIntervalSeconds);
    SKAPI SKNamespaceOptions * segmentSize(int segmentSize);
    SKAPI SKNamespaceOptions * allowLinks(bool allowLinks);

	SKAPI SKStorageType::SKStorageType getStorageType();
	SKAPI SKConsistency getConsistencyProtocol();
	SKAPI SKVersionMode getVersionMode();
	SKAPI SKRevisionMode getRevisionMode();
	SKAPI SKPutOptions * getDefaultPutOptions();
	SKAPI SKInvalidationOptions * getDefaultInvalidationOptions();
	SKAPI SKGetOptions * getDefaultGetOptions();
	SKAPI SKWaitOptions * getDefaultWaitOptions();
	SKAPI int getSecondarySyncIntervalSeconds();
    SKAPI int getSegmentSize();
    SKAPI bool getAllowLinks();
    
	SKAPI static SKNamespaceOptions * parse(const char * def);
	SKAPI virtual char * toString() const;
	SKAPI virtual bool equals(SKNamespaceOptions * pOther) const;
    
    SKAPI ~SKNamespaceOptions();
	SKAPI bool isWriteOnce() const;
	// creates a copy with VersionMode of SINGLE_VERSION and a RevisionMode of NO_REVISIONS.
	SKAPI SKNamespaceOptions * asWriteOnce() const; 

	//impl
	SKNamespaceOptions(void * pNamespaceOptions);
	void * getPImpl();
private:
	void * pImpl;
};

#endif // SKNAMESPACEOPTIONS_H
