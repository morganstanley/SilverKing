%include "perl5/exception.i"
%exception {
	try {
		$action
	} catch (const std::exception &e) {
		SWIG_exception_fail(SWIG_RuntimeError, e.what());
	/*} catch (const std::exception &e) {  */
	/*	SWIG_exception_fail(SWIG_RuntimeError, e.what()); */
	}
}

%module SKClientImpl

%{
/* Put all declarations here */
#include <stdint.h>
#include <set>
#include <boost/functional/hash.hpp>
#include <boost/unordered_map.hpp>
#undef seed
#include "skconstants.h"
#include "skcommon.h"
#include "skbasictypes.h"
#include "skcontainers.h"

#include "SKSessionEstablishmentTimeoutController.h"
#include "SKSimpleSessionEstablishmentTimeoutController.h"
#include "SKOpTimeoutController.h"
#include "SKSimpleTimeoutController.h"
#include "SKOpSizeBasedTimeoutController.h"
#include "SKWaitForTimeoutController.h"
#include "SKSecondaryTarget.h"
#include "SKAddrAndPort.h"
#include "SKAbsMillisTimeSource.h"
#include "SKConstantAbsMillisTimeSource.h"
#include "SKRelNanosTimeSource.h"
#include "SKRelNanosAbsMillisTimeSource.h"
#include "SKSystemTimeSource.h"
#include "SKTimerDrivenTimeSource.h"
#include "SKVersionConstraint.h"
#include "SKVersionProvider.h"
#include "SKRelNanosVersionProvider.h"
#include "SKAbsMillisVersionProvider.h"
#include "SKValueCreator.h"

#include "SKAsyncOperation.h"
#include "SKAsyncSnapshot.h"
#include "SKAsyncSyncRequest.h"
#include "SKAsyncKeyedOperation.h"
#include "SKAsyncPut.h"
#include "SKAsyncRetrieval.h"
#include "SKAsyncValueRetrieval.h"
#include "SKAsyncSingleValueRetrieval.h"

#include "SKClientDHTConfigurationProvider.h"
#include "SKGridConfiguration.h"
#include "SKClientDHTConfiguration.h"

#include "SKSessionOptions.h"
#include "SKNamespaceCreationOptions.h"
#include "SKNamespaceOptions.h"
#include "SKNamespacePerspectiveOptions.h"
#include "SKPutOptions.h"
#include "SKRetrievalOptions.h"
#include "SKGetOptions.h"
#include "SKWaitOptions.h"

#include "SKStopwatchBase.h"
#include "SKStopwatch.h"
#include "SKSimpleStopwatch.h"
#include "SKSimpleNamedStopwatch.h"

#include "SKClientException.h"
#include "SKNamespaceCreationException.h"
#include "SKNamespaceLinkException.h"
#include "SKSnapshotException.h"
#include "SKSyncRequestException.h"
#include "SKPutException.h"
#include "SKRetrievalException.h"
#include "SKWaitForCompletionException.h"
#include "SKMetaData.h"
#include "SKStoredValue.h"

#include "SKBaseNSPerspective.h"
#include "SKAsyncWritableNSPerspective.h"
#include "SKAsyncReadableNSPerspective.h"
#include "SKAsyncNSPerspective.h"
#include "SKSyncWritableNSPerspective.h"
#include "SKSyncReadableNSPerspective.h"
#include "SKSyncNSPerspective.h"

#include "SKNamespace.h"
#include "SKSession.h"
#include "SKClient.h"

%}

%inline %{
/* include all kind of additional code in c/c++ */
/* #include <iostream> */
#include <stdint.h>
#include <set>
#include <exception>

//------------------------------------------------------------------------------------------
// This function takes a hash of form:
// {  KEY1 => VALUE1 , KEY2 => VALUE2, ...  }
// The VALUEs can be either undef or string scalars.
// It returns a SKMap<string,SKVal*> object representing same data.
// It is the caller's responsibility to delete the object.

StrValMap * createMapFromHash(SV *keyValues)
{
  if (!SvROK(keyValues)) croak("keyValues is not a reference.");
  const SV* sv_nhash = SvRV(keyValues);
  if (SvTYPE(sv_nhash) != SVt_PVHV) croak("keyValues is not a hash");
  HV *nhv = (HV*)sv_nhash;
  hv_iterinit(nhv);
  StrValMap * kvMap = new StrValMap();
  char* keyStr;
  I32  keyLen;

  for (SV* ksv; (ksv = hv_iternextsv(nhv, &keyStr, &keyLen)) != 0; )
  {
    if (SvOK(ksv))  // is not undef
	{
	  if (!SvPOK(ksv))  // is string
	  {
	      delete kvMap;
	      croak("inner value is not a string scalar.");
	  }
	  STRLEN len;
	  char *s = SvPV(ksv, len);
	  SKVal * pval = sk_create_val();
	  sk_set_val(pval, len, (void*)s);
	  kvMap->insert( StrValMap::value_type(keyStr, pval ));
	}
    else  // value is undef
	{
	  SKVal * pval = sk_create_val();
	  kvMap->insert( StrValMap::value_type(keyStr, pval ));
	}
  }
  return kvMap;
}

%}

%rename(Assign) *::operator=;

%include "perl5/std_vector.i"
%include "perl5/std_string.i"
%include "perl5/std_map.i"
%include "stdint.i"
%include "perl5/std_except.i"
%include "perl5/exception.i"
%include "boost_unordered_map.i"

%rename(NO_CHECK)        SKChecksumType::NONE;
%rename(DIGEST_NONE)     SKKeyDigestType::NONE;
%rename(DIGEST_MD5)      SKKeyDigestType::MD5;
%rename(DIGEST_SHA_1)    SKKeyDigestType::SHA_1;
%rename(NONEXIST_NULL_VALUE)  SKNonExistenceResponse::NULL_VALUE;
%rename(NONEXIST_EXCEPTION)   SKNonExistenceResponse::EXCEPTION;
%rename(TIMEOUT_EXCEPTION) SKTimeoutResponse::EXCEPTION;
%rename(TIMEOUT_IGNORE)    SKTimeoutResponse::TIGNORE;
%rename(NOCOMPRESSION)         SKCompression::NONE;
%rename(FC_ERROR)             SKFailureCause::ERROR;
%rename(FC_TIMEOUT)           SKFailureCause::TIMEOUT;
%rename(FC_MUTATION)          SKFailureCause::MUTATION;
%rename(FC_MULTIPLE)          SKFailureCause::MULTIPLE;
%rename(FC_INVALID_VERSION)   SKFailureCause::INVALID_VERSION;
%rename(FC_SIMULTANEOUS_PUT)  SKFailureCause::SIMULTANEOUS_PUT;
%rename(FC_NO_SUCH_VALUE)     SKFailureCause::NO_SUCH_VALUE;
%rename(FC_NO_SUCH_NAMESPACE) SKFailureCause::NO_SUCH_NAMESPACE;
%rename(FC_CORRUPT)           SKFailureCause::CORRUPT;
%rename(OP_INCOMPLETE)         SKOpResult::INCOMPLETE;
%rename(OP_SUCCEEDED)          SKOpResult::SUCCEEDED;
%rename(OP_ERROR)              SKOpResult::ERROR;
%rename(OP_TIMEOUT)            SKOpResult::TIMEOUT;
%rename(OP_MUTATION)           SKOpResult::MUTATION;
%rename(OP_NO_SUCH_VALUE)      SKOpResult::NO_SUCH_VALUE;
%rename(OP_SIMULTANEOUS_PUT)   SKOpResult::SIMULTANEOUS_PUT;
%rename(OP_MULTIPLE)           SKOpResult::MULTIPLE;
%rename(OP_INVALID_VERSION)    SKOpResult::INVALID_VERSION;
%rename(OP_NO_SUCH_NAMESPACE)  SKOpResult::NO_SUCH_NAMESPACE;
%rename(OP_CORRUPT)            SKOpResult::CORRUPT;
%rename(OPST_INCOMPLETE)   SKOperationState::INCOMPLETE;
%rename(OPST_SUCCEEDED)    SKOperationState::SUCCEEDED;
%rename(OPST_FAILED)       SKOperationState::FAILED;
%rename(TIMEOUT_EXCEPTION)    SKTimeoutResponse::EXCEPTION;
%rename(TIMEOUT_IGNORE)       SKTimeoutResponse::TIGNORE;
%rename(NONEXIST_EXCEPTION)    SKNonExistenceResponse::EXCEPTION;

%include "skconstants.h"
%include "skcontainers.h"

%include "skbasictypes.h"
/** my $self=shift; SKVal* v = $self; **/
%extend SKVal 
{
	SKVal() { SKVal* v = sk_create_val(); /* printf("SKVal [%d, %s]\n", v->m_len,v->m_pVal); */ return v; }
	SKVal(const char * str_) { SKVal* v = sk_create_val();  sk_set_val(v, strlen(str_), (void*)str_); /* printf("SKVal [%d, %s]\n", v->m_len,v->m_pVal); */ return v; }
	SKVal(int strlen_, const char * str_) { SKVal* v = sk_create_val();  sk_set_val(v, strlen_, (void*)str_); return v; }
	~SKVal() { sk_destroy_val(&($self)); }
	std::string toString() { return std::string ((char*)$self->m_pVal, $self->m_len ); }
	int toInt() {return *((int*)$self->m_pVal); }
	int64_t toInt64() {return *((int64_t*)$self->m_pVal); }
	double toDouble() {return *((double*)$self->m_pVal); }
};

%typemap(in) ( int size_, void *src_ ) { 
 	STRLEN sz; 
    $2 = (void*) SvPV( $input, sz ); 
	$1 = (int)sz; 
} 

/** %typemap(in)  std::string const & key  { $1 = new string (SvPV_nolen( $input )); } **/
%typemap(in)      std::string * key  { $1 = new string (SvPV_nolen( $input )); } 
%typemap(freearg) std::string * key  { delete $1; }

%typemap(out) SKStoredValue ** {
	SKStoredValue ** psv = $1;
	/* printf("StoredValue %p, %s \n", *psv, (char*)((*psv)->getValue()->m_pVal) ); */
    $result = SWIG_NewPointerObj(SWIG_as_voidptr(*psv), SWIGTYPE_p_SKStoredValue, SWIG_OWNER | SWIG_SHADOW) ; argvi++ ;
} 


%template(StrVector) SKVector<std::string>;
%template(KeyVector) SKVector<const std::string*> ;
%template(StrStrMap) boost::unordered_map<std::string,std::string>;     /* SKMap<std::string, std::string>; */
%template(StrValMap) boost::unordered_map<std::string,SKVal*>;          /* SKMap<std::string, SKVal*> ; */
%template(StrSVMap)  boost::unordered_map<std::string,SKStoredValue*>;  /* SKMap<std::string, SKStoredValue*> ; */
%template(FailureCauseMap) boost::unordered_map<std::string,SKFailureCause::SKFailureCause>; /* SKMap<string,SKFailureCause::SKFailureCause> ; */
%template(OpStateMap) boost::unordered_map<std::string,SKOperationState::SKOperationState>;  /* SKMap<string,SKOperationState::SKOperationState> ; */

%typecheck(SWIG_TYPECHECK_FLOAT_ARRAY) 
	 boost::unordered_map<std::string,SKVal*> const * ,  boost::unordered_map<std::string,SKVal*> *,
	StrStrMap const * , StrStrMap *,
	StrSVMap const *, StrSVMap *,
	OpStateMap const *, OpStateMap *, 
	FailureCauseMap const *, FailureCauseMap *
{
  $1 = ( SvROK($input) && SvTYPE(SvRV($input)) == SVt_PVHV ) ?  1 : 0;
}

%typecheck(SWIG_TYPECHECK_DOUBLE_ARRAY) 
	SKVector<std::string> const *, SKVector<std::string> *,
	SKVector<const std::string*> const *, SKVector<const std::string*> *
{
  $1 = ( SvROK($input) && SvTYPE(SvRV($input)) == SVt_PVAV ) ?  1 : 0;
}

%typecheck(SWIG_TYPECHECK_CHAR_ARRAY) 
	const SKVal*, SKVal*
{
  $1 = ( SvPOKp($input) ) ?  1 : 0;
}

%typemap(out)  boost::unordered_map<std::string,SKVal*> *  {
  HV* hv = newHV();
   boost::unordered_map<std::string,SKVal*>::const_iterator cit ;
  for(cit = $1->begin(); cit!=$1->end(); cit++ ){
    SKVal* pval = cit->second;
	if( pval != NULL ){
		/* printf("typemap(out)StrValMap  key %s -> SKVal %s \n", cit->first.c_str(), (char*) pval->m_pVal ); */
		hv_store(hv, cit->first.c_str(), cit->first.size(), newSVpv((const char *)pval->m_pVal, pval->m_len), 0);
		sk_destroy_val( &pval );
	}
	else {
		hv_store(hv, cit->first.c_str(), cit->first.size(), newSVpv("",0), 0);
	}
  }
  
  $result = newRV_noinc((SV*)hv);
  sv_2mortal($result);
  ++argvi;
}

%typemap(in)  boost::unordered_map<std::string,SKVal*> const * dhtValues {

  if (!SvROK($input)) croak("$input is not a reference.");
  const SV* sv_hash = SvRV($input);
  if (SvTYPE(sv_hash) != SVt_PVHV) croak("$input is not a hash.");

  HV *hv = (HV*)sv_hash;
  hv_iterinit(hv);
   boost::unordered_map<std::string,SKVal*> * kvMap = new  boost::unordered_map<std::string,SKVal*>();

  char* keyStr;
  I32   keyLen;

  for (SV* ksv; (ksv = hv_iternextsv(hv, &keyStr, &keyLen)) != 0; )
  {
    if (SvOK(ksv))  // is not undef
	{
	  // strings 
	  if (!SvPOK(ksv))  // is string
	  {
	      delete kvMap;
	      croak("inner value is not a string scalar.");
	  }
	  STRLEN len;
	  char *s = SvPV(ksv, len);
	  SKVal * pval = sk_create_val();
	  sk_set_val(pval, len, (void*)s);
	  kvMap->insert(  boost::unordered_map<std::string,SKVal*>::value_type( keyStr, pval ) );
	}
    else  // value is undef
	{
	  SKVal * pval = sk_create_val();
	  kvMap->insert(  boost::unordered_map<std::string,SKVal*>::value_type( keyStr, pval ) );
	}
  }
  $1 = kvMap;

}
%typemap(freearg)   boost::unordered_map<std::string,SKVal*> const * dhtValues {
  boost::unordered_map<std::string,SKVal*>::const_iterator cit ;
  for(cit = $1->begin(); cit!=$1->end(); cit++ ){
    SKVal* pval = cit->second;
	if( pval != NULL ){
		sk_destroy_val( &pval );
	}
  }
  delete $1;
}

%typemap(out) OpStateMap *  {
  HV* hv = newHV();
  OpStateMap::const_iterator cit ;
  for(cit = $1->begin(); cit!=$1->end(); cit++ ){
    SKOperationState val = cit->second;
	hv_store(hv, cit->first.c_str(), cit->first.size(),  newSViv((int)(val)), 0);
  }
  
  $result = newRV_noinc((SV*)hv);
  sv_2mortal($result);
  ++argvi;
}

%typemap(out) FailureCauseMap *  {
  HV* hv = newHV();
  FailureCauseMap::const_iterator cit ;
  for(cit = $1->begin(); cit!=$1->end(); cit++ ){
    SKFailureCause val = cit->second;
	hv_store(hv, cit->first.c_str(), cit->first.size(),  newSViv((int)(val)), 0);
  }
  
  $result = newRV_noinc((SV*)hv);
  sv_2mortal($result);
  ++argvi;
}

%typemap(in) SKVector<std::string> const *
{
  if (!SvROK($input)) croak("$input is not an array reference.");
  const SV* sv_outer_array = SvRV($input);
  if (SvTYPE(sv_outer_array) != SVt_PVAV) croak("$input is not an array.");

  AV* av_outer = (AV*)sv_outer_array;
  const I32 len_outer = av_len(av_outer) + 1;
  $1 = new SKVector<std::string>();

  for (I32 idx_outer = 0; idx_outer < len_outer; ++idx_outer)
  {
      $1->push_back(SvPV(*av_fetch(av_outer, idx_outer, 0), PL_na));
  }
}

%typemap(freearg) SKVector<std::string> const *
{
  delete $1;
}

%typemap(out) SKVector<string> *  {
  AV* av = newAV();
  for(unsigned int i = 0 ; i<$1->size(); i++ ){
    const string & aResultKey= $1->at(i);
	av_store(av, i, newSVpv(aResultKey.c_str(), aResultKey.size()));
  }
  
  $result = newRV_noinc((SV*)av);
  sv_2mortal($result);
  ++argvi;
}

%typemap(freearg) SKVector<const std::string *> const *
{
  delete $1;
}

%typemap(out) SKVector<const std::string *> *  {
  AV* av = newAV();
  for(unsigned int i = 0 ; i<$1->size(); i++ ){
    const string & aResultKey= *($1->at(i));
	av_store(av, i, newSVpv(aResultKey.c_str(), aResultKey.size()));
  }
  
  $result = newRV_noinc((SV*)av);
  sv_2mortal($result);
  ++argvi;
}

%typemap(in) SKVal*   
{
  if(!SvPOKp($input)) croak("$input is not a string."); 
  STRLEN  keyLen;
  char* keyStr = SvPV( $input, keyLen );
  SKVal * pval = sk_create_val();
  sk_set_val(pval, keyLen, (void*)keyStr ); 
  $1 = pval;
}

%typemap(out) SKVal*   
{
	if($1==NULL || ($1->m_pVal)==NULL || $1->m_len == 0 ) {
		$result = newSV(0);
	} 
	else {
		$result = newSVpv((const char *)($1->m_pVal), $1->m_len);
	}
	sv_2mortal($result); 
	++argvi;
}

%typemap(freearg)  SKVal* {
	if( $1 != NULL ){
		sk_destroy_val( &($1) );
		$1 = NULL;
	}
}

%ignore *::getPImpl;

%newobject SKMetaData::getCreator;
%newobject SKMetaData::getUserData;
%newobject SKMetaData::getChecksum;
%include "SKMetaData.h"

%newobject SKStoredValue::getCreator;
%newobject SKStoredValue::getUserData;
%newobject SKStoredValue::getChecksum;
%newobject SKStoredValue::getMetaData;
%rename(nextValue) SKStoredValue::next;
%newobject SKStoredValue::nextValue;
%include "SKStoredValue.h"

%include "SKSecondaryTarget.h"

%import "SKClientDHTConfigurationProvider.h"
%newobject SKClient::getClient;
%newobject SKClient::openSession;
%include "SKClient.h"

/* SKSession, all namespace perspectives*/
%newobject *::getNamespace;  
%newobject SKSession::createNamespace;
%newobject SKSession::openAsyncNamespacePerspective;
%newobject SKSession::openSyncNamespacePerspective;
%newobject SKSession::getNamespaceCreationOptions;
%newobject *::getDefaultNamespaceOptions;
%newobject SKSession::getDefaultPutOptions;
%newobject SKSession::getDefaultGetOptions;
%newobject SKSession::getDefaultWaitOptions;
%include "SKSession.h"


%import "SKBaseNSPerspective.h"
%import "SKAsyncWritableNSPerspective.h"
%import "SKAsyncReadableNSPerspective.h"
%import "SKSyncWritableNSPerspective.h"
%import "SKSyncReadableNSPerspective.h"

/*all namespace perspectives*/
%newobject *::getOptions;
%newobject SKSyncNSPerspective::get;
%newobject SKSyncNSPerspective::waitFor;
%include "SKSyncNSPerspective.h"

%newobject SKAsyncNSPerspective::put;
%newobject SKAsyncNSPerspective::get;
%newobject SKAsyncNSPerspective::waitFor;
%newobject SKAsyncNSPerspective::snapshot;
%newobject SKAsyncNSPerspective::syncRequest;
%include "SKAsyncNSPerspective.h"
%include "SKAsyncWritableNSPerspective.h"

%newobject *::getIncompleteKeys;
%newobject *::getKeys;
%newobject *::getOperationStateMap;
%include "SKAsyncOperation.h"
%include "SKAsyncSnapshot.h"
%include "SKAsyncSyncRequest.h"
%include "SKAsyncKeyedOperation.h"
%include "SKAsyncPut.h"

%newobject *::getStoredValue;
%newobject *::getStoredValues;
%newobject *::getLatestStoredValues;
%include "SKAsyncRetrieval.h"

%newobject SKAsyncValueRetrieval::getValues;
%newobject SKAsyncValueRetrieval::getLatestValues;
%newobject SKAsyncValueRetrieval::getValue;
%include "SKAsyncValueRetrieval.h"
%include "SKAsyncSingleValueRetrieval.h"

%newobject *::parse;

%newobject SKClientDHTConfiguration::getClientDHTConfiguration;
%newobject SKClientDHTConfiguration::getZkLocs;
%newobject SKClientDHTConfiguration::create;
%include "SKClientDHTConfiguration.h"

%newobject SKGridConfiguration::parseFile;
%newobject SKGridConfiguration::readEnvFile;
%newobject SKGridConfiguration::getEnvMap;
%newobject SKGridConfiguration::getClientDHTConfiguration;
%include "SKGridConfiguration.h"

%newobject SKNamespace::getDefaultNSPOptions;
%newobject SKNamespace::getOptions;
%newobject SKNamespace::openAsyncPerspective;
%newobject SKNamespace::openSyncPerspective;
%include "SKNamespace.h"

%newobject SKNamespaceCreationOptions::defaultOptions;
%include "SKNamespaceCreationOptions.h"

%typemap(in) bool   
{
  if(!SvIOK($input)) croak("$input is not an integer."); 
  $1 = static_cast<bool>( (int)SvIV( $input ) );
}

%typemap(out) bool   
{
	$result = newSViv( (int) $1 );
	sv_2mortal($result); 
	++argvi;
}

%newobject SKNamespaceOptions::getDefaultGetOptions;
%newobject SKNamespaceOptions::getDefaultWaitOptions;
%newobject SKNamespaceOptions::getDefaultPutOptions;
%include "SKNamespaceOptions.h"
%typemap(in) bool ;
%typemap(out) bool ;

%include "SKSessionEstablishmentTimeoutController.h"
%include "SKSimpleSessionEstablishmentTimeoutController.h"
%include "SKOpTimeoutController.h"
%include "SKSimpleTimeoutController.h"
%include "SKOpSizeBasedTimeoutController.h"
%include "SKWaitForTimeoutController.h"

%newobject SKNamespacePerspectiveOptions::getDefaultPutOptions;
%newobject SKNamespacePerspectiveOptions::getDefaultGetOptions;
%newobject SKNamespacePerspectiveOptions::getDefaultWaitOptions;
%newobject SKNamespacePerspectiveOptions::getDefaultVersionProvider;
%newobject SKNamespacePerspectiveOptions::parse;
%include "SKNamespacePerspectiveOptions.h"

%newobject SKPutOptions::parse;
%newobject SKPutOptions::getUserData;
%newobject SKPutOptions::getSecondaryTargets;
/* %newobject SKPutOptions::compression;    */
/* %newobject SKPutOptions::version;        */
/* %newobject SKPutOptions::userData;       */
%include "SKPutOptions.h" 

%newobject SKRetrievalOptions::getVersionConstraint;
/* %newobject SKRetrievalOptions::retrievalType;        */
/* %newobject SKRetrievalOptions::waitMode;             */
/* %newobject SKRetrievalOptions::versionConstraint;    */
/* %newobject SKRetrievalOptions::nonExistenceResponse; */
%include "SKRetrievalOptions.h"

%newobject SKSessionOptions::getDHTConfig;
%include "SKSessionOptions.h"

%newobject SKValueCreator::getBytes;
%newobject SKValueCreator::getIP;
%include "SKValueCreator.h"

%newobject SKVersionConstraint::exactMatch;
%newobject SKVersionConstraint::allBelowOrEqual;
%newobject SKVersionConstraint::maxAboveOrEqual;
%newobject SKVersionConstraint::maxBelowOrEqual;
%newobject SKVersionConstraint::minAboveOrEqual;
%include "SKVersionConstraint.h"

%import "SKClientException.h"
%newobject *::getCause;
%newobject SKKeyedOperationException::getFailedKeys;
%include "SKPutException.h"
%newobject SKRetrievalException::getStoredValue;
%include "SKRetrievalException.h"
/* from SKClientException to all exceptions */
%newobject SKWaitForCompletionException::getFailedOperations;
%include "SKWaitForCompletionException.h"
%include "SKNamespaceCreationException.h"
%include "SKNamespaceLinkException.h"
%include "SKGetOptions.h"
%include "SKWaitOptions.h"

