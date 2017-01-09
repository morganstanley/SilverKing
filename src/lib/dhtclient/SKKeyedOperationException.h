/**
*
* $Header: $
* $Change: $
* $DateTime: $
*/

#ifndef SKKEYEDOPERATIONEXCEPTION_H
#define SKKEYEDOPERATIONEXCEPTION_H


#include "SKOperationException.h"
#include "skconstants.h"
#include "skcontainers.h"

class SKKeyedOperationException : public SKOperationException
{ 
public:
    SKAPI ~SKKeyedOperationException() throw ();
/*
	SKAPI virtual SKOperationState::SKOperationState getOperationState(string key) const ;
	SKAPI virtual SKFailureCause::SKFailureCause getFailureCause(string key) const ;
	SKAPI virtual SKVector<string> * getFailedKeys() const ;
    SKAPI virtual string getDetailedFailureMessage() const ;
protected:
	SKKeyedOperationException(bool doNewPImpl);

    SKMap<string,SKOperationState::SKOperationState> * pOpStates;
    SKMap<string,SKFailureCause::SKFailureCause> * pFailureCauses;
    SKVector<string> * pFailedKeys;
    string message; 
*/
protected:
	SKKeyedOperationException(ClientException * cause, const char * fileName, int lineNum) ;
};


#endif //SKKEYEDOPERATIONEXCEPTION_H
