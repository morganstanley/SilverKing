/**
*
* $Header: $
* $Change: $
* $DateTime: $
*/

#ifndef SKCLIENTEXCEPTION_H
#define SKCLIENTEXCEPTION_H


#include <string>
using std::string;
#include <exception>
#include "skconstants.h"

namespace jace { namespace proxy { namespace com { namespace ms { 
	namespace silverking {namespace cloud { namespace dht { namespace client {
		class ClientException;
} } } } } } } }
typedef jace::proxy::com::ms::silverking::cloud::dht::client::ClientException ClientException;
namespace jace { namespace proxy { namespace java { namespace lang { 
		class Throwable;
} } } } 
typedef jace::proxy::java::lang::Throwable Throwable;

class SKClientException : public std::exception
{
public:
    SKAPI virtual ~SKClientException(void) throw ();
	SKAPI SKClientException(Throwable * cause, const char * fileName, int lineNum) throw ();
	SKAPI SKClientException(ClientException * cause, const char * fileName, int lineNum) throw ();
	SKAPI virtual const char *what() const throw ();
    SKAPI void printStackTrace();
    SKAPI string getStackTrace();
    SKAPI virtual string getMessage() const;
	SKAPI string toString() const throw();
	SKAPI virtual const char * what();

protected:
	std::string msg;
	std::string mStack;
	std::string mFileName;
	int mLineNum;
	std::string mAll;
};

#endif //SKCLIENTEXCEPTION_H
