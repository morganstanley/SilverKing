/**
*
* $Header: $
* $Change: $
* $DateTime: $
*/

#ifndef SKOPERATIONEXCEPTION_H
#define SKOPERATIONEXCEPTION_H

#include "skconstants.h"
#include "SKClientException.h"

class SKOperationException : public SKClientException
{
/*
public:
	SKAPI SKOperationException(ClientException * cause, const char * fileName, int lineNum) throw ();
    SKAPI virtual string getDetailedFailureMessage() const = 0;

protected:
	SKOperationException(bool doNewPImpl) : 	SKClientException(doNewPImpl) {};
    friend class SKAsyncOperation;

    SKOperationException(std::string const & message, const SKClientException & cause );
    SKOperationException(std::string const & message);
    SKOperationException(const SKOperationException & other );
	
    SKOperationException(bool doNewPImpl);
    SKOperationException(const SKOperationException & other );
    SKOperationException & operator=(SKOperationException const & other);
*/
protected:
	SKOperationException(ClientException * cause, const char * fileName, int lineNum) : SKClientException(cause, fileName, lineNum) {};
};


#endif //SKOPERATIONEXCEPTION_H
