#include "SKValueCreator.h"

#include "jace/Jace.h"
using jace::java_new;
using jace::java_cast;
using namespace jace;
#include "jace/JArray.h"
using jace::JArray;
#include "jace/proxy/types/JByte.h"
using jace::proxy::types::JByte;
#include "jace/proxy/com/ms/silverking/cloud/dht/ValueCreator.h"
using jace::proxy::com::ms::silverking::cloud::dht::ValueCreator;

typedef JArray< jace::proxy::types::JByte > ByteArray;



SKValueCreator::SKValueCreator(ValueCreator * pVCImpl) {
	pImpl = pVCImpl ;
}

SKValueCreator::~SKValueCreator() {
	if(pImpl) {
		delete pImpl;
		pImpl = NULL;
	}
};

ValueCreator * SKValueCreator::getPImpl(){
	return pImpl;
}


SKVal * SKValueCreator::getIP() const {
	SKVal* pVal = sk_create_val();
	ByteArray obj = java_cast<ByteArray>(((ValueCreator*)pImpl)->getIP());
	
	if(obj.isNull()) {
		return pVal;  //empty value
	}
		
	size_t valLength = obj.length();
	if(valLength == 0) {
		return pVal;  //empty value
	}

	JNIEnv* env = attach();
	jbyte * carr = (jbyte *) skMemAlloc(valLength, sizeof(jbyte), __FILE__, __LINE__);
	env->GetByteArrayRegion(static_cast<jbyteArray>(obj.getJavaJniArray()), 0, valLength, carr );
	sk_set_val_zero_copy(pVal, valLength, (void*) carr);
	return pVal;  //non-empty value
}

int SKValueCreator::getID()  const {
	return (int)((ValueCreator*)pImpl)->getID();
}

SKVal * SKValueCreator::getBytes() const {
	SKVal* pVal = sk_create_val();
	ByteArray obj = java_cast<ByteArray>(((ValueCreator*)pImpl)->getBytes());
	
	if(obj.isNull()) {
		return pVal;  //empty value
	}
		
	size_t valLength = obj.length();
	if(valLength == 0) {
		return pVal;  //empty value
	}

	JNIEnv* env = attach();
	jbyte * carr = (jbyte *) skMemAlloc(valLength, sizeof(jbyte), __FILE__, __LINE__);
	env->GetByteArrayRegion(static_cast<jbyteArray>(obj.getJavaJniArray()), 0, valLength, carr );
	sk_set_val_zero_copy(pVal, valLength, (void*) carr);
	return pVal;  //non-empty value
}



