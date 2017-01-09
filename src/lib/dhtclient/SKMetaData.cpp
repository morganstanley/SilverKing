/**
*
* $Header: $
* $Change: $
* $DateTime: $
*/

#include "SKMetaData.h"
#include "SKValueCreator.h"
#include "skbasictypes.h"
#include "jenumutil.h"
#include <string>
using std::string;
#include <iostream>
using namespace std;

#include "jace/Jace.h"
using jace::java_new;
using jace::java_cast;
using namespace jace;
#include "jace/JArray.h"
using jace::JArray;
#include "jace/proxy/JObject.h"
using jace::proxy::JObject;
#include "jace/proxy/types/JByte.h"
using jace::proxy::types::JByte;
#include "jace/proxy/java/lang/String.h"
using jace::proxy::java::lang::String;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/MetaData.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::MetaData;
#include "jace/proxy/com/ms/silverking/cloud/dht/ValueCreator.h"
using jace::proxy::com::ms::silverking::cloud::dht::ValueCreator;
#include "jace/proxy/com/ms/silverking/cloud/dht/CreationTime.h"
using jace::proxy::com::ms::silverking::cloud::dht::CreationTime;

typedef JArray< jace::proxy::types::JByte > ByteArray;


SKMetaData::SKMetaData(MetaData * pMdImpl) { 
	pImpl = pMdImpl ;
}

SKMetaData::~SKMetaData() {
	if(pImpl) {
		delete pImpl;
		pImpl = NULL;
	}
};

MetaData * SKMetaData::getPImpl(){
	return pImpl;
}

SKMetaData::SKMetaData() { };


int SKMetaData::getStoredLength() const {
	return (int)(pImpl->getStoredLength());
}

int SKMetaData::getUncompressedLength() const {
	return (int)(pImpl->getUncompressedLength());
}

int64_t SKMetaData::getVersion() const {
	return (int64_t)(pImpl->getVersion());
}

int64_t SKMetaData::getCreationTime() const {
	return (int64_t)(pImpl->getCreationTime().inNanos());
}

SKValueCreator * SKMetaData::getCreator() const {
	ValueCreator * pvc = new ValueCreator(java_cast<ValueCreator>(pImpl->getCreator()));
	return new SKValueCreator(pvc);
}

SKVal * SKMetaData::getUserData() const {
    JObject o = pImpl->getUserData();
    if(o.isNull())
        return NULL;
    //cout <<"SKMetaData::getUserData static class " << o.getJavaJniClass().getInternalName() <<endl;
    ByteArray obj = java_cast<ByteArray>(o);
    return ::convertToDhtVal(&obj);
}

char * SKMetaData::toString(bool labeled) const {
	string str = (string)(pImpl->toString(labeled));
	return skStrDup(str.c_str(), __FILE__, __LINE__);
}

SKVal * SKMetaData::getChecksum() const {
	ByteArray obj = java_cast<ByteArray>(pImpl->getChecksum());
	return convertToDhtVal(&obj);
}

SKCompression::SKCompression SKMetaData::getCompression() const {
	int compr = (int)(pImpl->getCompression().ordinal());
	return static_cast<SKCompression::SKCompression>(compr);
}

SKChecksumType::SKChecksumType SKMetaData::getChecksumType() const {
	int chktype = (int)(pImpl->getChecksumType().ordinal());
    return static_cast<SKChecksumType::SKChecksumType>(chktype);
}

