#include <exception>
#include <sstream>

#include "jenumutil.h"
#include "SKAsyncOperation.h"
#include "SKRetrievalException.h"
#include "SKPutException.h"
#include "SKSyncRequestException.h"
#include "SKSnapshotException.h"
#include "SKWaitForCompletionException.h"
#include "SKNamespaceCreationException.h"
#include "SKClientException.h"

#include "jace/Jace.h"
using namespace jace;
#include "jace/proxy/types/JLong.h"
using jace::proxy::types::JLong;
#include "jace/proxy/types/JBoolean.h"
using jace::proxy::types::JBoolean;

#include "jace/proxy/java/lang/Throwable.h"
using jace::proxy::java::lang::Throwable;
#include "jace/proxy/java/util/concurrent/TimeUnit.h"
using jace::proxy::java::util::concurrent::TimeUnit;
#include "jace/proxy/java/util/logging/Level.h"
using jace::proxy::java::util::logging::Level;

#include "jace/proxy/com/ms/silverking/log/Log.h"
using jace::proxy::com::ms::silverking::log::Log;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/AsyncOperation.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::AsyncOperation;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/FailureCause.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::FailureCause;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/OperationState.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::OperationState;

#include "jace/proxy/com/ms/silverking/cloud/dht/client/ClientException.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::ClientException;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/OperationException.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::OperationException;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/KeyedOperationException.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::KeyedOperationException;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/NamespaceCreationException.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::NamespaceCreationException;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/PutException.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::PutException;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/RetrievalException.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::RetrievalException;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/SnapshotException.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::SnapshotException;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/SyncRequestException.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::SyncRequestException;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/WaitForCompletionException.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::WaitForCompletionException;


/* protected */
SKAsyncOperation::SKAsyncOperation() : pImpl(NULL) {};

/* public implementation */
SKAsyncOperation::SKAsyncOperation(AsyncOperation * pAsyncOperation){
    pImpl = pAsyncOperation;
}
void * SKAsyncOperation::getPImpl() {
    return pImpl;
}

SKAsyncOperation::~SKAsyncOperation() { 
    if(pImpl) {
        delete pImpl;
        pImpl = NULL;
    }
};

/* public Java methods */
SKOperationState::SKOperationState SKAsyncOperation::getState(){
	AsyncOperation * pAsyncOp = (AsyncOperation*)getPImpl();
	int opState = (jint)  pAsyncOp->getState().ordinal();
	return static_cast<SKOperationState::SKOperationState>(opState);
	
}

SKFailureCause::SKFailureCause SKAsyncOperation::getFailureCause(){
	AsyncOperation * pAsyncOp = (AsyncOperation*)getPImpl();
	int failureCause = (jint)  pAsyncOp->getFailureCause().ordinal();
	return static_cast<SKFailureCause::SKFailureCause>(failureCause);
}

bool SKAsyncOperation::waitForCompletion(long timeout, SKTimeUnit unit){
    AsyncOperation * pAsyncOp = (AsyncOperation*)getPImpl();
    JBoolean bdone = false;
    try {
	    switch (unit) {
		    case NANOSECONDS: 
			    bdone =  pAsyncOp->waitForCompletion( JLong(timeout), TimeUnit(TimeUnit::valueOf("NANOSECONDS")) );
			    break;
		    case MICROSECONDS: 
			    bdone =  pAsyncOp->waitForCompletion( JLong(timeout), TimeUnit(TimeUnit::valueOf("MICROSECONDS")) );
			    break;
		    case MILLISECONDS: 
			    bdone =  pAsyncOp->waitForCompletion( JLong(timeout), TimeUnit(TimeUnit::valueOf("MILLISECONDS")) );
			    break;
		    case SECONDS: 
			    bdone =  pAsyncOp->waitForCompletion( JLong(timeout), TimeUnit(TimeUnit::valueOf("SECONDS")) );
			    break;
		    case MINUTES: 
			    bdone =  pAsyncOp->waitForCompletion( JLong(timeout), TimeUnit(TimeUnit::valueOf("MINUTES")) );
			    break;
		    case HOURS: 
			    bdone =  pAsyncOp->waitForCompletion( JLong(timeout), TimeUnit(TimeUnit::valueOf("HOURS")) );
			    break;
		    case DAYS:
			    bdone =  pAsyncOp->waitForCompletion( JLong(timeout), TimeUnit(TimeUnit::valueOf("DAYS")) );
			    break;
		    default:
			    throw std::exception();
        }
    }  catch( Throwable &t ) {
		//throw SKClientException( &t, __FILE__, __LINE__ );
		repackException(__FILE__, __LINE__ );
    }
  	return (bool) bdone;
}

void SKAsyncOperation::waitForCompletion(){
    try {
	    AsyncOperation * pAsyncOp = (AsyncOperation*)getPImpl();
	    pAsyncOp->waitForCompletion();
    }  catch( Throwable &t ) {
		repackException(__FILE__, __LINE__ );
		//throw SKClientException( &t, __FILE__, __LINE__ );
    }
}

void SKAsyncOperation::close(){
	try {
		AsyncOperation * pAsync = (AsyncOperation*)getPImpl();
		pAsync->close();
    }  catch( Throwable &t ) {
		repackException(__FILE__, __LINE__ );
		//throw SKClientException( &t, __FILE__, __LINE__ );
	}
}

void SKAsyncOperation::repackException(const char * fileName , int lineNum ){

	try {
		throw;
    } catch(RetrievalException & pe) {
        throw SKRetrievalException( &pe, fileName, lineNum );
    } catch (PutException & pe) {
        throw SKPutException( &pe, fileName, lineNum );
    } catch (NamespaceCreationException & pe) {
        throw SKNamespaceCreationException( &pe, fileName, lineNum );
    } catch (SyncRequestException & pe) {
        throw SKSyncRequestException( &pe, fileName, lineNum );
    } catch (SnapshotException & pe) {
        throw SKSnapshotException(&pe, fileName, lineNum );
    } catch (WaitForCompletionException & pe) {
        throw SKWaitForCompletionException( &pe, fileName, lineNum );
    } catch (ClientException & pe) {
        throw SKClientException(&pe, fileName, lineNum );
    } catch (Throwable & pe) {
        throw SKClientException(&pe, fileName, lineNum );
    }

}
