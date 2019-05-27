#include <exception>
#include <string.h>
#include <string>
using std::string;
#include "SKClientException.h"
#include "jenumutil.h"

#include "jace/Jace.h"
using jace::java_new;
using jace::java_cast;
using jace::instanceof;
using namespace jace;
#include "jace/proxy/java/lang/String.h"
using jace::proxy::java::lang::String;
#include "jace/proxy/java/lang/Throwable.h"
using jace::proxy::java::lang::Throwable;
#include "jace/proxy/java/util/logging/Level.h"
using jace::proxy::java::util::logging::Level;
#include "jace/proxy/com/google/common/base/Throwables.h"
using jace::proxy::com::google::common::base::Throwables;

#include "jace/proxy/com/ms/silverking/log/Log.h"
using jace::proxy::com::ms::silverking::log::Log;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/ClientException.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::ClientException;

#include <iostream>
using namespace std;


SKClientException::~SKClientException(void) throw () 
{
    //if(pImpl) delete pImpl; pImpl = NULL;
}

const char* SKClientException::what() const throw ()
{
    return  mAll.c_str();
}

SKClientException::SKClientException(Throwable * cause, const char * fileName, int lineNum) throw (){
    //Throwable ce = java_cast<Throwable>(*cause);
    String strCause = cause->toString();
    if(!strCause.isNull()) {
        msg  = (std::string)(strCause);
    }
    else {
        msg  = cause->getMessage();
    }
    mStack    = (std::string)(Throwables::getStackTraceAsString(*cause));
    mFileName = fileName;
    mLineNum  = lineNum;
    std::ostringstream str ;
    str << msg << "\n" << mStack << "\ncaught in " << mFileName << " : " << mLineNum << " \n" << mStack;
    mAll = str.str();
}
SKClientException::SKClientException(ClientException * cause, const char * fileName, int lineNum) throw (){
    //ClientException ce = java_cast<ClientException>(*cause);
    String strCause = cause->toString();
    if(!strCause.isNull()) {
        msg  = (std::string)(strCause);
    }
    else {
        msg  = cause->getMessage();
    }
    mStack    = (std::string)(Throwables::getStackTraceAsString(*cause));
    mFileName = fileName;
    mLineNum  = lineNum;
    std::ostringstream str ;
    str << msg << mStack << " in " << mFileName << " : " << mLineNum <<"\n" ;
    mAll = str.str();
}

void SKClientException::printStackTrace(){
    Log::warning(java_new<String>((char *)mStack.c_str()));
    //pImpl->printStackTrace();
}

string SKClientException::getStackTrace(){
    return mStack ;
}

string SKClientException::getMessage() const {
    return msg;
}

string SKClientException::toString() const throw (){
    return  mAll;
}

const char * SKClientException::what() {
    return mAll.c_str();
}

/*
string SKClientException::getLocalizedMessage() const{
    return (string)pImpl->getLocalizedMessage();
}
*/

