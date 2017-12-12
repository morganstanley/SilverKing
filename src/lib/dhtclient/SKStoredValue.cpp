/**
*
* $Header: $
* $Change: $
* $DateTime: $
*/

#include "SKStoredValue.h"
#include "SKClientException.h"
#include "SKValueCreator.h"
#include "skbasictypes.h"
#include "jenumutil.h"
#include <iostream>
using namespace std;

#include "jace/Jace.h"
using jace::java_new;
using jace::java_cast;
using namespace jace;
#include "jace/JArray.h"
using jace::JArray;
#include "jace/proxy/types/JByte.h"
using jace::proxy::types::JByte;
#include "jace/proxy/java/lang/Object.h"
using jace::proxy::java::lang::Object;
#include "jace/proxy/java/lang/Throwable.h"
using jace::proxy::java::lang::Throwable;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/StoredValue.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::StoredValue;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/MetaData.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::MetaData;

typedef JArray< jace::proxy::types::JByte > ByteArray;


SKStoredValue::SKStoredValue(StoredValue * pSvImpl) : SKMetaData() {
	pImpl = pSvImpl ;
}

SKStoredValue::~SKStoredValue() {
	if(pImpl) {
		StoredValue* pVc = dynamic_cast<StoredValue*>(pImpl);
		delete pVc;
		pImpl = NULL;
	}
};

SKMetaData * SKStoredValue::getMetaData() const {
    if (pImpl == NULL) {
        return NULL;
    } else {
        SKMetaData  *skmd = NULL;
        
        try {
            StoredValue *_sv;
            
            _sv = dynamic_cast<StoredValue*>(pImpl);
            if (_sv == NULL || _sv->isNull()) {
                skmd = NULL;
            } else {
                MetaData md0;
                
                md0 = java_cast<MetaData>(_sv->getMetaData());
                if (md0.isNull()) {
                    skmd = NULL;
                } else {
                    skmd = new SKMetaData(new MetaData(md0));
                }
            }
        } catch(Throwable &t) {
            t.printStackTrace();
            throw SKClientException( &t, __FILE__, __LINE__ );
        }
        return skmd;

        //MetaData * pmd = new MetaData(java_cast<MetaData>(dynamic_cast<StoredValue*>(pImpl)->getMetaData()));
        //return new SKMetaData(pmd);
    }
}

SKVal * SKStoredValue::getValue() const {
	try {
		Object obj = dynamic_cast<StoredValue*>(pImpl)->getValue();
		if(obj.isNull()) return NULL;
		ByteArray byteArr = java_cast<ByteArray>(obj);
		if(byteArr.isNull())  return NULL;
		return ::convertToDhtVal(&byteArr);
    } catch(Throwable& t){
        t.printStackTrace();
        throw SKClientException( &t, __FILE__, __LINE__ );
    }
}

SKStoredValue * SKStoredValue::next() const {
	StoredValue * psv = new StoredValue(java_cast<StoredValue>(dynamic_cast<StoredValue*>(pImpl)->next()));
	return new SKStoredValue(psv);
}


