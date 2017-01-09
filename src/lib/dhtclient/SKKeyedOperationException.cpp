//#include "jccommon.h"
#include "SKKeyedOperationException.h"

SKKeyedOperationException::SKKeyedOperationException(ClientException * pe, const char * fileName, int lineNum) : SKOperationException(pe, fileName, lineNum) {};

SKKeyedOperationException::~SKKeyedOperationException() throw () { }

/*
SKKeyedOperationException::SKKeyedOperationException(bool doNewPImpl) : SKOperationException(doNewPImpl) {};

//public
SKKeyedOperationException::~SKKeyedOperationException() throw () {
    if(pOpStates) { delete pOpStates; pOpStates = NULL; }
    if(pFailureCauses) { delete pFailureCauses; pFailureCauses = NULL; }
    if(pFailedKeys) { delete pFailedKeys; pFailedKeys = NULL; }
}

SKOperationState::SKOperationState SKKeyedOperationException::getOperationState(string key) const {
    //return * pOpStates->get(key);
	KeyedOperationException * pKoe = dynamic_cast<KeyedOperationException *>(pImpl);
	int opState = (jint)  pKoe->getOperationState(java_new<String>((char*)key.c_str())).ordinal();
	return static_cast<SKOperationState::SKOperationState>(opState);
}

SKFailureCause::SKFailureCause SKKeyedOperationException::getFailureCause(string key) const {
    //return * pFailureCauses->get(key);
	KeyedOperationException * pKoe =  dynamic_cast<KeyedOperationException *>(pImpl);
	int failureCause = (jint)  pKoe->getFailureCause(java_new<String>((char*)key.c_str())).ordinal();
	return static_cast<SKFailureCause::SKFailureCause>(failureCause);
}

SKVector<string> * SKKeyedOperationException::getFailedKeys() const {
    //return pFailedKeys;
	KeyedOperationException * pKoe = dynamic_cast<KeyedOperationException *>(pImpl);
	SKVector<string> * pFailedKeys = new SKVector<string>();
	Set failedSet( pKoe->getFailedKeys() );
	for (Iterator it(failedSet.iterator()); it.hasNext();) {
        String akey = java_cast<String>(it.next());
		pFailedKeys->push_back( (string)(akey) );
    }
	return pFailedKeys;
}

string SKKeyedOperationException::getDetailedFailureMessage() const {
    //return message;
	KeyedOperationException * pKoe = dynamic_cast<KeyedOperationException *>(pImpl);
	string failureMsg =  (string)(pKoe->getDetailedFailureMessage());
	return failureMsg;
}
*/
