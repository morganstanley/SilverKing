
/** ==[ qdht2.c ]==============================================================
 *  (New) Q Interface to DHT:
 *  Function entry points for Q access to DHT facilities (New API)
 *  ===========================================================================
 */


#include <string>
#include <map>
#include <set>
#include <iostream>
#include <cstdio>
#include <sstream>
using std::set;

#include "sk.h"
#include "k.h"
#include "KTypes.h"
//#include "DHTLog.h"


#define STDSTRING(s)     KString(s).string() 
#define NSTRING(nsq)     nsq == K(0) ? 0 : (STDSTRING(nsq)).c_str()


namespace { // internal "static" functions and "global" data

    std::string                 _defaultNs ;
    K                         _kNULL ;
    
    SKClient * _client = NULL;

    double zu(int u){ return (double)u/8.64e4-(double)10957; }   // kdb+ from unix
    int uz(double f){ return (int)(86400.0*(f+10957.0)); }  // unix from kdb+

    K internal_nullterm_list( K keylistq )
    {
        if (keylistq->t==0 || keylistq->t==-KC ) 
        {
            for (int ii = 0; ii < keylistq->n; ++ii)
            {
                K dkey = kK(keylistq)[ii];
                if (dkey->t == KC )
                {
                    if( kC(dkey)[dkey->n] != 0) 
                    {
                        kC(dkey)[dkey->n] = 0;
                    }
                }
            }
        }
        return K(0);
    }

    K internal_nullterm_dict( K dictq )
    {
        K keys = kK(dictq)[0];
        K values = kK(dictq)[1];
        int keysize=keys->n;
        int valsize=values->n;
        if(keys->t == -KC || keys->t == 0) 
        {
            for(int ii = 0; ii < keysize; ++ii)
            {
                K dkey = kK(keys)[ii];
                int kstrsize=dkey->n;
                if(kC(dkey)[kstrsize] != 0){
                   kC(dkey)[kstrsize] = 0;
                }
            }
        }
        if(values->t == KC) {
            for(int ii = 0; ii < valsize; ++ii)
            {
                K dval = kK(values)[ii];
                int vstrsize = dval->n;
                if(kC(dval)[vstrsize] != 0){
                   kC(dval)[vstrsize] = 0;
                }
            }
        }
        
        return K(0);
    }
    
    /** =========================================================== */
    
    K make_symbol( void* pointer )
    {
        return kj( (J)pointer ) ;
    }
    
    K ptr_to_printable( void* pointer ) {
        char buff[19]; //64bit => 8bytes => "0x" + 16 chars + \0
        sprintf(buff, "0x%llx", pointer);
        return kpn(buff, 19);
    }

    void* printable_to_ptr( K keyq ) {
        if(kC(keyq)[keyq->n] != 0){
           kC(keyq)[keyq->n] = 0;
        }
        unsigned long addr = strtoul((char *)kC(keyq), 0, 0);
        return (void *)addr;
    }

    K dhtval_to_hexk( const SKVal* pVal ) {
        char * buff = (char *) malloc( pVal->m_len*2 );
        for ( int i=0; i<pVal->m_len; i++) {
            sprintf(buff+2*i, "%02x", ((unsigned char*)pVal->m_pVal)[i] );
        }
        K retval = kpn(buff, pVal->m_len*2);
        free( buff );
        return retval;
    }
    
    //exhandler is called from within catch(){ ... } block
    K exhandler( const char * msg, const char * filename, const int lineNum) {
        std::ostringstream str ;
        str << string(msg) << " " << filename << ":" << lineNum << " ";
        try {
            throw;
        } catch(SKRetrievalException const& ex) {
            std::cout << "Retrieval Excption: " << ex.what() << "\n";
        } catch(SKPutException const& ex) {
            std::cout << "Put Excption: " << ex.what() << "\n";
        } catch (SKWaitForCompletionException const& ex) {
            std::cout << "WaitForCompletionException: " << ex.what() << "\n";
        } catch (SKSyncRequestException const& ex) {
            std::cout << "SyncRequestException: " << ex.what() << "\n";
        } catch (SKSnapshotException const& ex) {
            std::cout << "SnapshotException: " << ex.what() << "\n";
        } catch (SKNamespaceCreationException const& ex) {
            std::cout << "NamespaceCreationException: " << ex.what() << "\n";
        } catch (SKNamespaceLinkException const& ex) {
            std::cout << "NamespaceLinkException: " << ex.what() << "\n";
        } catch (SKClientException const& ex) {
            std::cout << "Exception: " << ex.what() << "\n";
        } catch (std::exception const& ex) {
            std::cout << "std::exception: " << ex.what() << "\n";
        } catch (...) {
            std::cout << "unknown exception\n";
        }
        krr( const_cast<char*>(str.str().c_str()) ) ;
        return K(0);
    }
    
    K get_client(LoggingLevel logLevel, std::string const& jvmOptions) {
        /* 
        //TODO: add/init logging  
        */
        _kNULL = K(0) ;
        if(_client == NULL) {
            _client = SKClient::getClient(logLevel, jvmOptions.c_str());
        }
        return make_symbol( _client ) ;
    }

    K get_session( SKClient * client, std::string const& gcname, std::string const& server ) {
        if(client != NULL) {
            SKGridConfiguration * pGC = NULL;
            pGC = SKGridConfiguration::parseFile(gcname.c_str());

            SKClientDHTConfiguration * pCdc = NULL;
            SKSessionOptions * sessOption = NULL;
            pCdc = pGC->getClientDHTConfiguration();
            sessOption = new SKSessionOptions( pCdc, server.c_str()) ;
            SKSession * session = client->openSession(sessOption);
            delete pCdc;
            delete sessOption;

            //SKSession * session = client->openSession(pGC, server.c_str());
            delete pGC;

            if(session != NULL)
                return make_symbol( session ) ;
        }
        return _kNULL ;
    }

    //------------------------ SKStoredValue --------------------------------
    K internal_dhtval_pval(SKVal * pVal) 
    {
        K rv = K( 0 );
        if( !pVal )
            return rv ;
        if(pVal->m_pVal)
            rv = kpn( (char*)pVal->m_pVal, pVal->m_len ); 
        else
            rv = kp( "" ); 
        return rv;
    }

    K internal_storedvalue_get_value(SKStoredValue * pStoredVal) 
    {
        if( !pStoredVal ) return _kNULL ;
        SKVal * pVal  = pStoredVal->getValue();
        K rv = internal_dhtval_pval(pVal);
        sk_destroy_val(&pVal);
        return rv;
    }

    K internal_storedvalue_get_userdata(SKStoredValue * pStoredVal) 
    {
        if( !pStoredVal ) return _kNULL ;
        SKVal * pVal  = pStoredVal->getUserData();
        K rv = internal_dhtval_pval(pVal);
        sk_destroy_val(&pVal);
        return rv;
    }

    K internal_storedvalue_get_checksum(SKStoredValue * pStoredVal) 
    {
        if( !pStoredVal ) return _kNULL ;
        SKVal * pVal  = pStoredVal->getChecksum();
        K rv = dhtval_to_hexk( pVal ); 
        sk_destroy_val(&pVal);
        return rv;
    }

    K internal_storedvalue_get_creatorip(SKStoredValue * pStoredVal) 
    {
        K rv  = _kNULL;
        if( !pStoredVal ) return rv ;
        SKValueCreator * pCreator = pStoredVal->getCreator();
        if( !pCreator ) return rv ;
        SKVal * pVal  = pCreator->getIP();
        if( !pVal ) return rv ;
        
        if(!pVal->m_len )
            rv =  kp( "" ); 
        else {
            ostringstream str;
            unsigned char ipp1 = (unsigned char) ((char*)pVal->m_pVal)[0] ;
            unsigned char ipp2 = (unsigned char) ((char*)pVal->m_pVal)[1] ;
            unsigned char ipp3 = (unsigned char) ((char*)pVal->m_pVal)[2] ;
            unsigned char ipp4 = (unsigned char) ((char*)pVal->m_pVal)[3] ;
            str << (int) ipp1 << "." << (int) ipp2 << "." << (int) ipp3 << "." << (int) ipp4 ;
            rv  = kp( const_cast<char*>(str.str().c_str()) ); 
        }
        sk_destroy_val(&pVal);
        delete pCreator;
        return rv;
    }

    K internal_storedvalue_get_creatorid(SKStoredValue * pStoredVal) 
    {
        K rv  = _kNULL;
        if( !pStoredVal ) return rv ;
        SKValueCreator * pCreator = pStoredVal->getCreator();
        if( !pCreator ) return rv ;
        rv  = ki(pCreator->getID() ); //process ID
        delete pCreator;
        return rv;
    }
    
    K internal_storedvalue_todict(SKStoredValue * pStoredVal) 
    {
        K rv = K( 0 );
        if( !pStoredVal )
            return rv ;
        
        size_t nVals = 11;
        int i = 0;
        K vallist = ktn(0, nVals);
        K keylist = ktn(0, nVals);
        {
            kK(vallist)[i] = ki( pStoredVal->getStoredLength() ); // 
            kK(keylist)[i] = kp( "Length");
            ++i;
            kK(vallist)[i] = ki( pStoredVal->getUncompressedLength() ); // 
            kK(keylist)[i] = kp( "UncompressedLength");
            ++i;
            kK(vallist)[i] = kj( pStoredVal->getVersion() ); // 
            kK(keylist)[i] = kp( "Version");
            ++i;
            //CreationTime is in nanosecs, startipng from 2000/1/1
            int64_t timeNs = pStoredVal->getCreationTime();
            time_t tmpT = timeNs/(1000*1000*1000);
            int nsTime = (int) (timeNs % (1000*1000*1000));
            tm * tmTime = localtime(&tmpT);
            tmTime->tm_year += 30 ; //year start is 2000 instead of 1970
            tmpT = mktime ( tmTime );
            kK(vallist)[i] = kz(zu( tmpT )); // 
            kK(keylist)[i] = kp( "CreationTime");
            ++i;
            kK(vallist)[i] = ki( (int) pStoredVal->getChecksumType() ); // 
            kK(keylist)[i] = kp( "ChecksumType");
            ++i;
            kK(vallist)[i] = ki( (int) pStoredVal->getCompression() ); // 
            kK(keylist)[i] = kp( "Compression");
            ++i;
            SKVal * pVal  = pStoredVal->getValue();
            if(pVal != NULL) {
                kK(vallist)[i] = kpn( (char*)pVal->m_pVal, pVal->m_len ); 
                sk_destroy_val(&pVal);
            } else {
                kK(vallist)[i] = kp(",ERROR:bad result from DHT client"); 
            }
            kK(keylist)[i] = kp( "Value");
            ++i;
            
            pVal           = pStoredVal->getUserData();
            if(pVal != NULL) {
                kK(vallist)[i] = kpn( (char*)pVal->m_pVal, pVal->m_len ); 
                sk_destroy_val(&pVal);
            } else {
                kK(vallist)[i] = kp("<NULL>"); 
            }
            kK(keylist)[i] = kp( "UserData");
            ++i;

            pVal           = pStoredVal->getChecksum();
            if(pVal != NULL) {
            
                int * pi = (int *)(pVal->m_pVal);
                kK(vallist)[i] = dhtval_to_hexk( pVal ); 
                sk_destroy_val(&pVal);
            } else {
                kK(vallist)[i] = kp("<NULL>"); 
            }
            kK(keylist)[i] = kp( "Checksum");
            ++i;
            
            kK(vallist)[i] = internal_storedvalue_get_creatorid( pStoredVal) ; // creator Process ID
            kK(keylist)[i] = kp( "CreatorID");
            ++i;
            kK(vallist)[i] = internal_storedvalue_get_creatorip( pStoredVal) ; // creator IP address
            kK(keylist)[i] = kp( "CreatorIP");
            ++i;
        }
        
        rv = xD( keylist, vallist );
        return rv ;
    }
    
    //------------------------ SKSession --------------------------------
    K session_create_ns( SKSession * session, std::string const& ns, std::string const& nsOptions ) {
        if(session != NULL) {
            SKNamespaceOptions * pNSO = NULL;
            SKNamespace * pNs = NULL;
            if (!nsOptions.empty()) {
                pNSO = SKNamespaceOptions::parse(nsOptions.c_str());
                pNs = session->createNamespace( ns.c_str(), pNSO);
            } else {
                pNs = session->createNamespace( ns.c_str() );
            }
            delete pNSO;
            if ( pNs != NULL ) {
                return make_symbol( pNs ) ;
                //delete pNs;
                //return kb(1) ;
            }

        }
        return K(0) ;
    }

    K session_get_ns( SKSession * session, std::string const& ns ) {
        if(session != NULL) {
            SKNamespace * nameSpace = session->getNamespace( ns.c_str() );
            if(nameSpace != NULL)
                return make_symbol( nameSpace ) ;
        }
        return K(0) ;
    }

    /*
    SKNamespacePerspectiveOptions * get_nsp_options( SKChecksumType::SKChecksumType checksumType, SKCompression::SKCompression compression ){
        SKOpSizeBasedTimeoutController * pOsCtrl = new SKOpSizeBasedTimeoutController();
        std::set<SKSecondaryTarget*> * pTgts = new std::set<SKSecondaryTarget*>();
        SKVal * userData = sk_create_val();
        bool cksumComprVals = false;
        int64_t version = -1;
        SKPutOptions * pPutOpt =  new SKPutOptions( pOsCtrl, compression, checksumType,
                cksumComprVals, version, pTgts, userData);
        sk_destroy_val(&userData);
        
        SKNamespacePerspectiveOptions * pNspOptions = new SKNamespacePerspectiveOptions();
        pNspOptions->defaultPutOptions(pPutOpt);
        return pNspOptions;
    }
    */

    //FIXME: consider use of SKNamespacePerspectiveOptions::parse() ?
    K session_get_sync_nsp(SKSession * session, std::string const& ns, 
                           SKNamespacePerspectiveOptions * pNspOptions) {
        if (session != NULL) {
            SKSyncNSPerspective * snsp = NULL;
            if (pNspOptions) {
                snsp = session->openSyncNamespacePerspective(ns.c_str(), pNspOptions);
            } else {
                SKNamespacePerspectiveOptions *pDefNspOptions;
                SKNamespace *pNs;
                
                pNs = session->getNamespace(ns.c_str());
                pDefNspOptions = pNs->getDefaultNSPOptions();
                snsp = session->openSyncNamespacePerspective(ns.c_str(), pDefNspOptions);
                delete pNs;
                delete pDefNspOptions;
            }
            if (snsp != NULL) {
                return make_symbol( snsp ) ;
            }
        }
        return K(0) ;
    }

    K session_get_async_nsp(SKSession * session, std::string const& ns, 
                            SKNamespacePerspectiveOptions * pNspOptions) {
        if (session != NULL) {
            SKAsyncNSPerspective * ansp = NULL;
            if (pNspOptions) {
                ansp = session->openAsyncNamespacePerspective(ns.c_str(), pNspOptions);
            } else {
                SKNamespacePerspectiveOptions *pDefNspOptions;
                SKNamespace *pNs;
                
                pNs = session->getNamespace(ns.c_str());
                pDefNspOptions = pNs->getDefaultNSPOptions();
                ansp = session->openAsyncNamespacePerspective(ns.c_str(), pDefNspOptions);
                delete pNs;
                delete pDefNspOptions;
            }
            if (ansp != NULL) {
                return make_symbol( ansp ) ;
            }
        }
        return _kNULL ;
    }
    
    //------------------------ SKNamespace --------------------------------
    K namespace_get_options( SKNamespace * pNamespace ) {
        if(pNamespace != NULL) {
            SKNamespaceOptions * pNSO = pNamespace->getOptions();
            if ( pNSO != NULL ) {
                return make_symbol( pNSO ) ;
            }
        }
        return _kNULL ;
    }

    K namespace_open_sync_nsp( SKNamespace * pNamespace, SKNamespacePerspectiveOptions * nspOptions ) {
        if(pNamespace != NULL) {
            SKNamespaceOptions * pNSO = pNamespace->getOptions();
            if ( pNSO != NULL ) {
                return make_symbol( pNSO ) ;
            }
        }
        return K(0) ;
    }
    

    //------------------------ SKSyncWritableNSPerspective ------------------------
    K internal_put( SKSyncWritableNSPerspective * snsp, K keyq, K valueq, SKPutOptions * pPutOptions = NULL )
    {
        KBytes val(valueq);
        SKVal* pVal = sk_create_val();
        sk_set_val_zero_copy(pVal, val.size(), (char*)val );
        string key = KString(keyq).string();
        if(pPutOptions)
            snsp->put( &key, pVal, pPutOptions );
        else
            snsp->put( &key, pVal );
            
        pVal->m_len = 0; pVal->m_pVal = NULL; // m_pVal handled by external object
        sk_destroy_val(&pVal);
        
        return _kNULL ;
    }
    
    K internal_mput( SKSyncWritableNSPerspective * snsp, K keylistq, K valuelistq, SKPutOptions * pPutOptions = NULL )
    {
        SKMap<string,SKVal*>  valueMap;
        std::vector<SKVal*> pValVec ;
        for ( int ii = 0 ; ii < keylistq->n ; ++ii ) {
            SKVal* pVal = sk_create_val();
            KBytes val(kK(valuelistq)[ii]);
            sk_set_val_zero_copy(pVal, val.size(), (char*)val );
            valueMap.insert( StrValMap::value_type( STDSTRING( kK(keylistq)[ii] ).c_str() , pVal ) );
            pValVec.push_back(pVal);
        }
        if(pPutOptions)
            snsp->put( &valueMap, pPutOptions );
        else
            snsp->put( &valueMap );
        
        int sz = pValVec.size();
        for(int i=0; i<sz; ++i){
            SKVal* pVal = pValVec.at(i);
            pVal->m_len = 0; pVal->m_pVal = 0; // internal val handled by external object
            sk_destroy_val(& pVal );
        }
        return _kNULL ;
    }

    //------------------------ SKSyncReadableNSPerspective ------------------------
    K internal_get( SKSyncReadableNSPerspective * snsp, K keyq, bool isWaitFor )
    {
        if(kC(keyq)[keyq->n] != 0){
           kC(keyq)[keyq->n] = 0;
        }
        K result = _kNULL;
        SKVal* value = NULL;
        string key = KString(keyq).string();
        if(isWaitFor){
            value = snsp->waitFor( &key );
        } 
        else{
            value = snsp->get( &key );
        }

        if(value==NULL) 
            return kp(",ERROR:bad result from DHT client"); //error value

        if (value->m_len > 0)
            result =  kpn( (char*)value->m_pVal, value->m_len); // 
        else
            result =  kp(""); //empty value
        sk_destroy_val(&value);
        
        return result;
    }

    SKStoredValue* internal_getwo( SKSyncReadableNSPerspective * snsp, K keyq, bool isWaitFor, SKRetrievalOptions * retrieveOptions )
    {
        if(kC(keyq)[keyq->n] != 0){
           kC(keyq)[keyq->n] = 0;
        }
        SKStoredValue* value = NULL;
        string key = KString(keyq).string();
        if(isWaitFor){
            value = snsp->waitFor( &key, dynamic_cast<SKWaitOptions*>(retrieveOptions) );
        } 
        else{
            value = snsp->get( &key, dynamic_cast<SKGetOptions*>(retrieveOptions) );
        }
        return value;
    }

    K internal_getwo_asdict( SKSyncReadableNSPerspective * snsp, K keyq, bool isWaitFor, SKRetrievalOptions * retrieveOptions )
    {
        K result = _kNULL;
        SKStoredValue* value = internal_getwo( snsp, keyq, isWaitFor, retrieveOptions );
        return internal_storedvalue_todict(value);
    }
    
    //FIXME:: this function is almost identical to below one, but uses SKStoredValue and return dict of dict
    K internal_mget( SKSyncReadableNSPerspective * snsp, K keylistq, bool isWaitFor )
    {
        SKMap<string, SKVal*> * values = NULL;
        K rv = K(0);
        internal_nullterm_list(keylistq);
        SKVector<std::string> keys; 
        for(int i=0; i<keylistq->n; i++) 
        {
            std::string val( STDSTRING( kK(keylistq)[i] ).c_str() );
            keys.push_back(val);
        }

        //FIXME: add try/catch SKRetrievalException and get partial results
        try {
            if(isWaitFor) {
                values = snsp->waitFor( &keys );
            } else {
                values = snsp->get( &keys );
            }
        }
        catch ( SKRetrievalException & re ) {
            /*for (size_t ii = 0; ii < len; ++ii) {
                SKStoredValue * svalue = re.getStoredValue(keys.at(ii));
                ...
            } */
        }
        
        if(!values) return rv;  //return NULL, if no values found. Is there a null-dictionary ?
        
        size_t len = keys.size();
        K resultlist = ktn(0, len);
        for (size_t ii = 0; ii < len; ++ii)
        {
            //dhtLog(DHT_LOG_INFO, "Fetching key %d: %s", ii, keys[ii]);
            StrValMap::iterator it = values->find(keys.at(ii));
            if(it!=values->end()) {
                SKVal * pvalue = it->second;
                if(pvalue==NULL) 
                    kK(resultlist)[ii] = kp(",ERROR:bad result from DHT client"); //error value
                else {
                    if (pvalue->m_len > 0)
                        kK(resultlist)[ii] = kpn( (char*)pvalue->m_pVal, pvalue->m_len); // 
                    else
                        kK(resultlist)[ii] = kp(""); //empty value
                    sk_destroy_val(&pvalue);
                }
            }
            else 
                kK(resultlist)[ii] = kp(",ERROR:bad result from DHT client"); //error value
        }
        delete values;

        r1(keylistq);
        rv = xD( keylistq, resultlist );
        
        return rv ;
    }

    K internal_mgetwo_asdict( SKSyncReadableNSPerspective * snsp, K keylistq, bool isWaitFor, K retrieveopthq )
    {
        SKMap<string, SKStoredValue*> * values = NULL;
        K rv = K(0);
        internal_nullterm_list(keylistq);
        SKVector<std::string> keys; 
        for(int i=0; i<keylistq->n; i++) 
        {
            std::string val( STDSTRING(kK(keylistq)[i]).c_str() );
            keys.push_back(val);
        }

        //FIXME: add try/catch SKRetrievalException and get partial results
        try {
            if(isWaitFor) {
                SKWaitOptions * waitOpt = (SKWaitOptions *) retrieveopthq->j; //retrieveopthq ie really SKWaitOptions
                values = snsp->waitFor( &keys, waitOpt );
            }
            else {
                SKGetOptions * getOpt = (SKGetOptions *) retrieveopthq->j; //retrieveopthq ie really SKGetOptions
                values = snsp->get( &keys, getOpt );
            }
            
        }
        catch ( SKRetrievalException & re ) {
            std::cout << "RetrievalException in internal_mgetwo_asdict \n" ;
            /*for (size_t ii = 0; ii < len; ++ii) {
                SKStoredValue * svalue = re.getStoredValue(keys.at(ii));
                ...
            } */
        }
        
        if(!values) return rv;  //return NULL, if no values found. Is there a null-dictionary ?
        
        size_t len = keys.size();
        K resultlist = ktn(0, len);
        for (size_t ii = 0; ii < len; ++ii)
        {
            //dhtLog(DHT_LOG_INFO, "Fetching key %d: %s", ii, keys[ii]);
            StrSVMap::iterator it = values->find(keys.at(ii));
            if( it != values->end() ) {
                SKStoredValue * pvalue = it->second;
                if(pvalue == NULL) 
                    kK(resultlist)[ii] = kp(",ERROR:bad result from DHT client"); //error value
                else {
                    K metaDict = internal_storedvalue_todict(pvalue);
                    kK(resultlist)[ii] = metaDict;
                    delete pvalue;
                }
            } 
            else kK(resultlist)[ii] = kp(",ERROR:bad result from DHT client"); //error value
        }
        delete values;
        r1(keylistq);
        rv = xD( keylistq, resultlist );
        //std::cout << "internal_mgetwo_asdict done \n";
        return rv ;
    }
    
    K internal_mgetwo( SKSyncReadableNSPerspective * snsp, K keylistq, bool isWaitFor, K retrieveopthq )
    {
        SKMap<string, SKStoredValue*> * values = NULL;
        K rv = K(0);
        internal_nullterm_list(keylistq);
        SKVector<std::string> keys; 
        for(int i=0; i<keylistq->n; i++) 
        {
            std::string val( STDSTRING( kK(keylistq)[i] ).c_str()  );
            keys.push_back(val);
        }

        //FIXME: add try/catch SKRetrievalException and get partial results
        if(isWaitFor) {
            SKWaitOptions * waitOpt = (SKWaitOptions *) retrieveopthq->j; //retrieveopthq ie really SKWaitOptions
            values = snsp->waitFor( &keys, waitOpt );
        }
        else {
            SKGetOptions * getOpt = (SKGetOptions *) retrieveopthq->j; //retrieveopthq ie really SKGetOptions
            values = snsp->get( &keys, getOpt );
        }
        if(!values) return rv;  //return NULL, if no values found. Is there a null-dictionary ?

        size_t len = keys.size();
        K resultlist = ktn(0, len);
        for (size_t ii = 0; ii < len; ++ii)
        {
            //dhtLog(DHT_LOG_INFO, "Fetching key %d: %s", ii, keys[ii]);
            SKMap<string, SKStoredValue*>::iterator it = values->find(keys.at(ii));
            if( it != values->end() ) {
                SKStoredValue * value = it->second;
                if( value==NULL ) 
                    kK(resultlist)[ii] = kp(",ERROR:bad result from DHT client"); //error value
                else {
                    SKVal* pVal = value->getValue();
                    if(!pVal)
                        kK(resultlist)[ii] = kp(",ERROR:bad result from DHT client"); //error value
                    else {
                        if ( pVal->m_len > 0)
                            kK(resultlist)[ii] = kpn( (char*)pVal->m_pVal, pVal->m_len); // 
                        else
                            kK(resultlist)[ii] = kp(""); //empty value
                        sk_destroy_val(&pVal);
                    }
                    delete value;
                }
            }
            else kK(resultlist)[ii] = kp(",ERROR:bad result from DHT client"); //error value
        }
        delete values;
        r1(keylistq);
        rv = xD( keylistq, resultlist );
        return rv ;
    }
    
    //------------------------ SKAsyncWritableNSPerspective ------------------------
    K internal_awnsp_put( SKAsyncWritableNSPerspective * ansp, K keyq, K valueq, SKPutOptions * pPutOptions = NULL )
    {
        KBytes val(valueq);
        SKVal* pVal = sk_create_val();
        sk_set_val_zero_copy(pVal, val.size(), (char*)val );
        SKAsyncPut * pPut = NULL;
        if(pPutOptions)
            pPut = ansp->put( STDSTRING(keyq).c_str(), pVal, pPutOptions ) ;
        else
            pPut = ansp->put( STDSTRING(keyq).c_str(), pVal );
            
        pVal->m_len = 0; pVal->m_pVal = NULL; // m_pVal handled by external object
        sk_destroy_val(&pVal);
        
        return make_symbol(pPut) ;
    }

    K internal_awnsp_mput( SKAsyncWritableNSPerspective * ansp, K keylistq, K valuelistq, SKPutOptions * pPutOptions = NULL )
    {
        SKMap<string,SKVal*>  valueMap;
        std::vector<SKVal*> pValVec ;
        SKAsyncPut * pPut = NULL;
        for ( int ii = 0 ; ii < keylistq->n ; ++ii ) {
            SKVal* pVal = sk_create_val();
            KBytes val( kK(valuelistq)[ii]);
            sk_set_val_zero_copy(pVal, val.size(), (char*)val );
            valueMap.insert( SKMap<string,SKVal*>::value_type(STDSTRING(kK(keylistq)[ii]).c_str(), pVal ));
            pValVec.push_back(pVal);
        }
        if(pPutOptions)
            pPut = ansp->put( &valueMap, pPutOptions );
        else
            pPut = ansp->put( &valueMap );
        
        int sz = pValVec.size();
        for(int i=0; i<sz; ++i){
            SKVal* pVal = pValVec.at(i);
            pVal->m_len = 0; pVal->m_pVal = 0; // internal val handled by external object
            sk_destroy_val(& pVal );
        }
        return make_symbol(pPut) ;
    }
    
    //------------------------ SKAsyncReadableNSPerspective ------------------------
    K internal_arnsp_get( SKAsyncReadableNSPerspective * ansp, K keyq, bool isWaitFor )
    {
        if(kC(keyq)[keyq->n] != 0){
           kC(keyq)[keyq->n] = 0;
        }
        SKAsyncValueRetrieval * pAvr = NULL;
        if(isWaitFor){
            pAvr = ansp->waitFor( STDSTRING(keyq).c_str() );
        } 
        else{
            pAvr = ansp->get( STDSTRING(keyq).c_str() );
        }
        return (pAvr==NULL) ? K(0) : make_symbol(pAvr);
    }
    
    //Next 2 methods are very identical
    K internal_arnsp_mgetwo( SKAsyncReadableNSPerspective * ansp, K keylistq, bool isWaitFor, K retrieveopthq )
    {
        SKAsyncRetrieval * pAr = NULL;
        internal_nullterm_list(keylistq);
        SKVector<std::string> keys; 
        for(int i=0; i<keylistq->n; i++) 
        {
            std::string val( STDSTRING(kK(keylistq)[i]).c_str() );
            keys.push_back(val);
        }

        //FIXME: add try/catch SKRetrievalException and get partial results
        //try {
            if(isWaitFor) {
                SKWaitOptions * waitOpt = (SKWaitOptions *) retrieveopthq->j; //retrieveopthq ie really SKWaitOptions
                pAr = ansp->waitFor( &keys, waitOpt );
            }
            else {
                SKGetOptions * getOpt = (SKGetOptions *) retrieveopthq->j; //retrieveopthq ie really SKGetOptions
                pAr = ansp->get( &keys, getOpt );
            }
        //} catch ( SKRetrievalException & re ) {
        //}
        return (pAr==NULL) ? K(0) : make_symbol(pAr);
    }
    
    K internal_arnsp_mget( SKAsyncReadableNSPerspective * ansp, K keylistq, bool isWaitFor )
    {
        SKAsyncValueRetrieval * pAvr = NULL;
        K rv = K(0);
        internal_nullterm_list(keylistq);
        SKVector<std::string> keys; 
        for(int i=0; i<keylistq->n; i++) 
        {
            std::string val( STDSTRING(kK(keylistq)[i]).c_str() );
            keys.push_back(val);
        }

        //FIXME: add try/catch SKRetrievalException and get partial results
        if(isWaitFor) {
            pAvr = ansp->waitFor( &keys );
        }
        else {
            pAvr = ansp->get( &keys );
        }
        return (pAvr==NULL) ? K(0) : make_symbol(pAvr);
    }
    
    //------------------------ SKAsyncKeyedOperation ----------------------------------------------
    K internal_asynckeyedop_get_keys(SKAsyncKeyedOperation * asyncput, bool isIncomplete)
    {
        SKVector<string> * pKeys = NULL;
        if(isIncomplete) 
            pKeys = asyncput->getIncompleteKeys();
        else
            pKeys = asyncput->getKeys();
            
        size_t nVals = pKeys->size();
        K keylist = ktn(0, nVals);
        for( int ii = 0; ii < nVals; ++ii ) {
            kK(keylist)[ii] = kpn((char*)(pKeys->at(ii).data()), pKeys->at(ii).size());
        }
        return keylist;
    }
    
    
    K internal_asynckeyedop_get_opstatemap(SKAsyncKeyedOperation * asyncput)
    {
        K rv = K( 0 );
        SKMap<string,SKOperationState::SKOperationState> * pValues = asyncput->getOperationStateMap();
        if( !pValues || pValues->size() == 0 )
            return rv ;
        
        size_t nVals = pValues->size();
        K vallist = ktn(0, nVals);
        K keylist = ktn(0, nVals);
        int i = 0;
        SKMap<string,SKOperationState::SKOperationState>::const_iterator cit ;
        for(cit = pValues->begin(); cit!=pValues->end(); cit++ ){
            SKOperationState::SKOperationState opState = cit->second;
            kK(vallist)[i] = ki( (int)opState); // 
            kK(keylist)[i] = kpn( const_cast<char*>(cit->first.data()), cit->first.size());
            i++;
        }
        delete pValues;
        rv = xD( keylist, vallist );
        return rv ;
    }
    
    //------------------------ SKAsyncValueRetrieval ----------------------------------------------
    K internal_avalret_get_value(SKAsyncValueRetrieval * asyncret, K keyq)
    {
        if(kC(keyq)[keyq->n] != 0){
           kC(keyq)[keyq->n] = 0;
        }
        K result = _kNULL;
        SKVal* value = NULL;
        string key = KString(keyq).string();
        value = asyncret->getValue( &key );
        if(value==NULL) 
            return kp(",ERROR:bad result from DHT client"); //error value

        if (value->m_len > 0)
            result =  kpn( (char*)value->m_pVal, value->m_len); 
        else
            result =  kp(""); //empty value
        sk_destroy_val(&value);
        return result;
    }

    K internal_avalret_get_values(SKAsyncValueRetrieval * asyncret, bool getLatest)
    {
        K rv = _kNULL;
        SKMap<string,SKVal*> * values = NULL;
        SKOperationState::SKOperationState opstate = asyncret->getState();
        //FIXME: add try/catch SKRetrievalException and get partial results
        //try {
            if(getLatest)
                values = asyncret->getLatestValues();
            else
                values = asyncret->getValues();
        //} catch ( SKRetrievalException & re ) {
            /*
            for (size_t ii = 0; ii < len; ++ii) {
                SKStoredValue * svalue = re.getStoredValue(keys.at(ii));
                ...
            }
            */
        //}
        if(!values) return rv;  //return NULL, if no values found. Is there a null-dictionary ?
        
        size_t len = values->size();
        K resultlist = ktn(0, len);
        K keylist    = ktn(0, len);
        SKMap<string,SKVal*>::const_iterator cit ;
        int ii = 0;
        for(cit = values->begin(); cit!=values->end(); cit++ ){
            SKVal* pVal = cit->second;
            if(!pVal)
                kK(resultlist)[ii] = kp(",ERROR:bad result from DHT client"); //error value
            else {
                if ( pVal->m_len > 0)
                    kK(resultlist)[ii] = kpn( (char*)pVal->m_pVal, pVal->m_len); // 
                else
                    kK(resultlist)[ii] = kp(""); //empty value
                sk_destroy_val(&pVal);
            }
            kK(keylist)[ii] = kpn( const_cast<char*>(cit->first.data()), cit->first.size());
            ii++;
        }
        K rvd = xD(keylist, resultlist);
        K names = ktn(KS, 2);
        kS(names)[0] = ss("lastbatch");
        kS(names)[1] = ss("results");
        K kvalues = ktn(0, 2);
        kK(kvalues)[0] = ki((int)opstate);
        kK(kvalues)[1] = rvd;
        rv = xD(names, kvalues);    
        
        //rv = xD( keylist, resultlist );
        return rv;
    }

    //------------------------ SKAsyncRetrieval ----------------------------------------------
    K internal_asyncretrieval_get_storedvalue(SKAsyncRetrieval * asyncret, K keyq)
    {
        if(kC(keyq)[keyq->n] != 0){
           kC(keyq)[keyq->n] = 0;
        }
        K result = _kNULL;
        SKStoredValue * pvalue = NULL;
        SKVal* value = NULL;
        string key = KString(keyq).string();
        pvalue = asyncret->getStoredValue( key );
        if ( pvalue==NULL || (value = pvalue->getValue())==NULL ) 
            return kp(",ERROR:bad result from DHT client"); //error value
            
        if (value->m_len > 0)
            result =  kpn( (char*)value->m_pVal, value->m_len); 
        else
            result =  kp(""); //empty value
        
        sk_destroy_val(&value);
        delete pvalue;
        return result;
    }
    
    K internal_asyncretrieval_get_storedvalues(SKAsyncRetrieval * asyncret, bool getLatest)
    {
        K rv = _kNULL;
        SKMap<string,SKStoredValue*> * values = NULL;
        SKOperationState::SKOperationState opstate = asyncret->getState();
        if(getLatest)
            values = asyncret->getLatestStoredValues();
        else
            values = asyncret->getStoredValues();
            
        if(!values) return rv;  //return NULL, if no values found. Is there a null-dictionary ?
        
        size_t len = values->size();
        K resultlist = ktn(0, len);
        K keylist    = ktn(0, len);

        SKMap<string,SKStoredValue*>::const_iterator cit ;
        int ii = 0;
        for(cit = values->begin(); cit!=values->end(); cit++ ){
            SKStoredValue * pvalue = cit->second;
            if(pvalue) {
                SKVal* pVal = pvalue->getValue();
                if(!pVal)
                    kK(resultlist)[ii] = kp(",ERROR:bad result from DHT client"); //error value
                else {
                    if ( pVal->m_len > 0)
                        kK(resultlist)[ii] = kpn( (char*)pVal->m_pVal, pVal->m_len); // 
                    else
                        kK(resultlist)[ii] = kp(""); //empty value
                    sk_destroy_val(&pVal);
                }
            }
            else
                kK(resultlist)[ii] = kp(",ERROR:bad result from DHT client"); //error value
            kK(keylist)[ii] = kpn( const_cast<char*>(cit->first.data()), cit->first.size());
            ii++;
        }
        K rvd = xD(keylist, resultlist);
        K names = ktn(KS, 2);
        kS(names)[0] = ss("lastbatch");
        kS(names)[1] = ss("results");
        K kvalues = ktn(0, 2);
        kK(kvalues)[0] = ki((int)opstate);
        kK(kvalues)[1] = rvd;
        rv = xD(names, kvalues);    
        
        //rv = xD( keylist, resultlist );
        return rv;
    }
    
    //------------------------ SKPutOptions ----------------------------------------------
    K internal_putopts_get_userdata(SKPutOptions * pPutOpt)
    {
        K result = _kNULL;
        SKVal* value = NULL;
        if(!pPutOpt) return _kNULL;
        
        value = pPutOpt->getUserData();
        if(value==NULL) 
            return kp(",ERROR: no UserData from DHT client"); //error value
        if (value->m_len > 0)
            result =  kpn( (char*)value->m_pVal, value->m_len); 
        else
            result =  kp(""); //empty value
        sk_destroy_val(&value);
        return result;
    }
    
    
    //------------------------------------------------------------------------------------------------------
/*    K internal_submit_async( SKAsyncNSPerspective * ansp, K keylistq, int timeout, int threshold )
    {
        internal_nullterm_list(keylistq);
        SKVector<std::string> keys; 
        for(int i=0; i<keylistq->n; i++) 
        {
            std::string val( kC( kK(keylistq)[i] ) );
            keys.push_back(val);
        }
        SKAsyncValueRetrieval * asyncRet = NULL;  //// FIXME !!!!!!!!
        if( timeout > 0 ){
            asyncRet = ansp->waitFor( &keys, timeout, threshold );
        } 
        else{
            asyncRet = ansp->get( &keys );
        }
        
        K async_handle = kj( (J)asyncRet );
        return async_handle;
    }

    K internal_results_async( SKAsyncValueRetrieval * retrieval )
    {
        //retrieval->waitForCompletion();
        SKMap<string,string> * pValues = retrieval->getValues();
        size_t nVals = pValues->size();
        K vallist = ktn(0, nVals);
        K keylist = ktn(0, nVals);
        size_t ii = 0;
        SKMap<string,string>::iterator cit = pValues->begin();
        for(cit; cit != pValues->end(); cit++, ii++){
            if(cit->second.data())  //FIXME: check OpState
            {
                kK(vallist)[ii] = kpn((char*)(cit->second.data()), cit->second.size());
            } else {
                kK(vallist)[ii] = kp(",ERROR:bad result from DHT client");
            }
            
            kK(keylist)[ii] = kpn((char*)(cit->first.data()), cit->first.size());
        }
    
        // build result K structure 
        int lastbatch = 0;
        if (retrieval->getState() != SKOperationState::INCOMPLETE)    {
          lastbatch = 1;
          retrieval->close();
          delete retrieval;
          ////dhtLog(DHT_LOG_FINE, "got the last batch");
        }
        K rvd = xD(keylist, vallist);
        K names = ktn(KS, 2);
        kS(names)[0] = ss("lastbatch");
        kS(names)[1] = ss("results");
        K values = ktn(0, 2);
        kK(values)[0] = ki(lastbatch);
        kK(values)[1] = rvd;
        K rv = xD(names, values);
        
        return rv;

    }
    
    K internal_async_mput( SKAsyncNSPerspective * ansp, K keylistq, K valuelistq )
    {
        SKMap<string,string> * valueMap = new SKMap<string,string>();
        for ( int ii = 0 ; ii < keylistq->n ; ++ii ) {
            std::string aKey(kC(kK(keylistq)[ii]));
            KString val = KString(kK(valuelistq)[ii]);
            //SKValue aVal(val.size(), (char*)val);
            valueMap->insert(aKey, string(val, val.size()) );
        }
        SKAsyncPut * asyncPut = NULL;
        asyncPut = ansp->put( valueMap );
        delete valueMap;
        if ( asyncPut != NULL ) {
            K async_handle = kj( (J)asyncPut );
            return async_handle;
        }
        return _kNULL ;
    }


    K internal_amput_complete( SKAsyncPut * asyncPut )
    {
        //retrieval->waitForCompletion();
        int lastbatch = 0;
        if (asyncPut->getState() != SKOperationState::INCOMPLETE)    {
          lastbatch = 1;
          asyncPut->close();
          delete asyncPut;
        }
        K rv = kb(lastbatch);
        return rv;
    }
*/    
    
} // namespace (anonymous)

extern "C" {

/** ==[ lifecycle ]========================================================= */

K dht_client_getclient(K loglevelq, K jvmoptionsq )
{
    try {
        LoggingLevel level;
        if (loglevelq->t == -KI) level = (LoggingLevel)(loglevelq->i);
        return get_client(level, STDSTRING(jvmoptionsq)); 
    }
    catch ( std::exception& ke ) {
        krr( "K type mismatch" ) ;
    }
    catch (...) {
        krr( "dht init failed" ) ;
    }
    return K(0) ;
}

K dht_shutdown( K dummyq )
{
    // which resources to release?
    try {
        if(_client) {
            delete _client ;
            _client = NULL;
        }
        SKClient::shutdown();
    }
    catch (...) 
    {
       //just ignore if it throw on on client destructors/free()/de-allocations, when VERBOSE logging is used
       ;
    }
    return dummyq ;
}

K dht_client_getsession(K clienthq, K gcname, K hostq )
{
    try {
        return get_session((SKClient *) ((void *) KLong(clienthq)),  STDSTRING(gcname), STDSTRING(hostq));
    }
    catch (...) {
        exhandler("client get session failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}

K dht_client_setlogfile ( K loglevelq) 
{
    try 
    {
        if(_client) {
            LoggingLevel level;
            if (loglevelq->t == -KI) level = (LoggingLevel)(loglevelq->i);
            SKClient::setLogLevel(level); 
        }
    }
    catch ( std::exception& ke ) {
        krr( "K type mismatch" ) ;
    }
    catch (...) {
        krr( "dht init failed" ) ;
    }
    return _kNULL ;
}

K dht_client_setloglevel ( K logfilenameq) 
{
    if(_client) {
        SKClient::setLogFile(STDSTRING(logfilenameq).c_str());
    }
    return _kNULL ;
}


//------------------------ SKSession --------------------------------
K dht_session_createnamespace3( K sessionhq, K namespaceq, K nsoptionsStrq )
{
    try {
        return session_create_ns((SKSession *) ((void *) KLong(sessionhq)),  STDSTRING(namespaceq), STDSTRING(nsoptionsStrq));
    }
    catch (...) {
        exhandler("session create namespace  failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}

K dht_session_createns( K sessionhq, K namespaceq )
{
    try {
        SKSession * pSession = (SKSession *) ((void *) KLong(sessionhq));
        SKNamespace * pNs = pSession->createNamespace( STDSTRING(namespaceq).c_str() );
        if ( pNs != NULL ) 
            return make_symbol( pNs ) ;
    }
    catch (...) {
        exhandler( "Session get default NamespaceOptions failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}

K dht_session_getnamespace( K sessionhq, K namespaceq )
{
    try {
        return session_get_ns((SKSession *) ((void *) KLong(sessionhq)),  STDSTRING(namespaceq) );
    }
    catch (...) {
        exhandler("session get namespace  failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}

K dht_session_getsyncnsp( K sessionhq, K namespaceq, K nsperspectiveoptionsq )
{
    try {
        return session_get_sync_nsp((SKSession *) ((void *) KLong(sessionhq)),  STDSTRING(namespaceq),
            (SKNamespacePerspectiveOptions *) ((void *) KLong(nsperspectiveoptionsq)));
    }
    catch (...) {
        exhandler("session get sync nsp  failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}

K dht_session_getasyncnsp( K sessionhq, K namespaceq, K nsperspectiveoptionsq  )
{
    try {
        return session_get_async_nsp((SKSession *) ((void *) KLong(sessionhq)),  STDSTRING(namespaceq),
            (SKNamespacePerspectiveOptions *) ((void *) KLong(nsperspectiveoptionsq)));
    }
    catch (...) {
        exhandler("session get async nsp failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}

K dht_session_getnscreationoptions( K sessionhq )
{
    try {
        SKSession * pSession = (SKSession *) ((void *) KLong(sessionhq));
        SKNamespaceCreationOptions * pNscOpt = pSession->getNamespaceCreationOptions();
        if ( pNscOpt != NULL ) 
            return make_symbol( pNscOpt ) ;
    }
    catch (...) {
        exhandler( "Session get NamespaceCreationOptions failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}

K dht_session_getdefaultnamespaceoptions( K sessionhq )
{
    try {
        SKSession * pSession = (SKSession *) ((void *) KLong(sessionhq));
        SKNamespaceOptions * pNsOpt = pSession->getDefaultNamespaceOptions();
        if ( pNsOpt != NULL ) 
            return make_symbol( pNsOpt ) ;
    }
    catch (...) {
        exhandler( "Session get default NamespaceOptions failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}

K dht_session_getdefaultputoptions( K sessionhq )
{
    try {
        SKSession * pSession = (SKSession *) ((void *) KLong(sessionhq));
        SKPutOptions * pPutOpt = pSession->getDefaultPutOptions();
        if ( pPutOpt != NULL ) 
            return make_symbol( pPutOpt ) ;
    }
    catch (...) {
        exhandler( "Session get default PutOptions failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}

K dht_session_getdefaultgetoptions( K sessionhq )
{
    try {
        SKSession * pSession = (SKSession *) ((void *) KLong(sessionhq));
        SKGetOptions * pGetOpt = pSession->getDefaultGetOptions();
        if ( pGetOpt != NULL ) 
            return make_symbol( pGetOpt ) ;
    }
    catch (...) {
        exhandler( "Session get default GetOptions failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}

K dht_session_getdefaultwaitoptions( K sessionhq )
{
    try {
        SKSession * pSession = (SKSession *) ((void *) KLong(sessionhq));
        SKWaitOptions * pWaitOpt = pSession->getDefaultWaitOptions();
        if ( pWaitOpt != NULL ) 
            return make_symbol( pWaitOpt ) ;
    }
    catch (...) {
        exhandler( "Session get default WaitOpt failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}

K dht_session_deletens( K sessionhq, K namespaceq )
{
    try {
        SKSession * pSession = (SKSession *) ((void *) KLong(sessionhq));
        pSession->deleteNamespace(STDSTRING(namespaceq).c_str());
        return kb( 1 );
    }
    catch (...) {
        exhandler("session delete Namespace failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}

K dht_session_recoverns( K sessionhq, K namespaceq )
{
    try {
        SKSession * pSession = (SKSession *) ((void *) KLong(sessionhq));
        pSession->recoverNamespace(STDSTRING(namespaceq).c_str());
        return kb( 1 );
    }
    catch (...) {
        exhandler("session delete Namespace failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}

K dht_session_close( K sessionhq )
{
    try {
        SKSession * pSession = (SKSession *) ((void *) KLong(sessionhq));
        pSession->close();
        return kb( 1 );
    }
    catch (...) {
        exhandler("session close failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}

K dht_session_delete( K sessionhq )
{
    try {
        SKSession * pSession = (SKSession *) ((void *) KLong(sessionhq));
        delete pSession;
        sessionhq->j = 0;
        return _kNULL;
    }
    catch (...) {
        exhandler("session delete failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}

//------------------------ SKNamespace --------------------------------
K dht_namespace_getdefaultnspoptions (  K namespacehq )
{
    try {
        SKNamespace * pNs = (SKNamespace *) ((void *) KLong(namespacehq));
        return make_symbol( pNs->getDefaultNSPOptions());  //returns SKNamespacePerspectiveOptions*
    }
    catch (...) {
        exhandler( "namespace get options failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}

K dht_namespace_getoptions( K namespacehq )
{
    try {
        SKNamespace * pNs = (SKNamespace *) ((void *) KLong(namespacehq));
        return make_symbol( pNs->getOptions());
        //return namespace_get_options( (SKNamespace *) ((void *) KLong(namespacehq)) );
    }
    catch (...) {
        exhandler( "namespace get options failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}

K dht_namespace_openasyncnsp( K namespacehq, K nsperspectiveoptionhq )
{
    try {
        SKNamespace * pNs = (SKNamespace *) ((void *) KLong(namespacehq));
        SKNamespacePerspectiveOptions * pNSo = (SKNamespacePerspectiveOptions *) ((void *) KLong(nsperspectiveoptionhq));
        if( pNs && pNSo )
            return make_symbol( pNs->openAsyncPerspective( pNSo ) ) ;
    }
    catch (...) {
        exhandler( "namespace open async nsp failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}

K dht_namespace_opensyncnsp( K namespacehq, K nsperspectiveoptionhq )
{
    try {
        SKNamespace * pNs = (SKNamespace *) ((void *) KLong(namespacehq));
        SKNamespacePerspectiveOptions * pNSo = (SKNamespacePerspectiveOptions *) ((void *) KLong(nsperspectiveoptionhq));
        if( pNs && pNSo )
            return make_symbol( pNs->openSyncPerspective( pNSo ) ) ;
    }
    catch (...) {
        exhandler( "namespace open sync nsp failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}

K dht_namespace_getname( K namespacehq )
{
    try {
        SKNamespace * pNs = (SKNamespace *) ((void *) KLong(namespacehq));
        char * str = pNs->getName() ;
        K nameq = kpn( str, strlen(str));
        free(str);
        return nameq;
    }
    catch (...) {
        exhandler( "namespace get name failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}

K dht_namespace_clone( K namespacehq, K childNameq )
{
    try {
        if(kC(childNameq)[childNameq->n] != 0)
            kC(childNameq)[childNameq->n] = 0;
        SKNamespace * pNs = (SKNamespace *) ((void *) KLong(namespacehq));
        return make_symbol( pNs->clone( STDSTRING(childNameq).c_str() ) );
    }
    catch (...) {
        return exhandler( "write-once namespace clone failed", __FILE__, __LINE__);
    }
}

K dht_namespace_clonev( K namespacehq, K childNameq, K versionq )
{
    try {
        if(kC(childNameq)[childNameq->n] != 0)
            kC(childNameq)[childNameq->n] = 0;
        SKNamespace * pNs = (SKNamespace *) ((void *) KLong(namespacehq));
        int64_t version  = (int64_t) (KLong(versionq));
        return make_symbol( pNs->clone( STDSTRING(childNameq).c_str(), version ) );
    }
    catch (...) {
        return exhandler( "versioned namespace clone failed", __FILE__, __LINE__);
    }
}

K dht_namespace_linkto( K namespacehq, K targetq )
{
    try {
        if(kC(targetq)[targetq->n] != 0)
            kC(targetq)[targetq->n] = 0;
        SKNamespace * pNs = (SKNamespace *) ((void *) KLong(namespacehq));
        pNs->linkTo( STDSTRING(targetq).c_str() ) ;
        return _kNULL;
    }
    catch (...) {
        return exhandler( "namespace linking failed", __FILE__, __LINE__);
    }
}

K dht_namespace_delete( K namespacehq )
{
    try {
        SKNamespace * pnamespace = (SKNamespace *) ((void *) KLong(namespacehq));
        delete pnamespace;
        namespacehq->j = 0;
        return _kNULL;
    }
    catch (...) {
        exhandler("namespace delete failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}

//------------------------ SKNamespaceOptions --------------------------------
K dht_nsoptions_parse( K nsoptq )
{
    try {
        //e.g. nsoptq: "versionMode=SINGLE_VERSION,storageType=FILE,consistencyProtocol=TWO_PHASE_COMMIT";
        return make_symbol( SKNamespaceOptions::parse(STDSTRING(nsoptq).c_str() ) );
    }
    catch (...) {
        exhandler( "namespace options parsing failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}

K dht_nsoptions_new( K storageTypeq, K consistencyProtocolq, K versionModeq, K revisionModeq,
    K defaultPutOptionsq, K defaultInvalidationOptionsq, 
    K defaultGetOptionsq, K defaultWaitOptionsq,
    K secondarySyncIntervalSecondsq, K segmentSizeq, K allowLinksq) 
{
    try {
        SKStorageType::SKStorageType st = (SKStorageType::SKStorageType) ((int) (Kinteger(storageTypeq))) ;
        SKConsistency cp = (SKConsistency) ((int) (Kinteger(consistencyProtocolq))) ;
        SKVersionMode vm = (SKVersionMode) ((int) (Kinteger(versionModeq))) ;
        SKRevisionMode rm = (SKRevisionMode) ((int) (Kinteger(revisionModeq))) ;
        int secondarySyncIntervalSeconds = (int) (Kinteger(secondarySyncIntervalSecondsq));
        int segmentSize = (int) (Kinteger(segmentSizeq));
        bool allowLinks = (int)(Kinteger(allowLinksq)) != 0 ? true : false;
        
        SKPutOptions * pPutOpt = (SKPutOptions *) defaultPutOptionsq->j;
        SKInvalidationOptions * pInvalidationOpt = (SKInvalidationOptions *) defaultInvalidationOptionsq->j;
        SKGetOptions * pGetOpt = (SKGetOptions *) defaultGetOptionsq->j;
        SKWaitOptions * pWaitOpt = (SKWaitOptions *) defaultWaitOptionsq->j;
        SKNamespaceOptions * pNsOpt = new SKNamespaceOptions(st, cp, vm, rm,
        pPutOpt, pInvalidationOpt, pGetOpt, pWaitOpt,
        secondarySyncIntervalSeconds, segmentSize, 
        (1024 * 1024 * 1024), // max value size
        allowLinks);
        return make_symbol( pNsOpt );
    } catch (...) {
        return exhandler( "NamespaceOptions creation failed", __FILE__, __LINE__);
    }
}

void get_options_from_dict (K OpsOptionsDictq, SKPutOptions * pPutOpt, SKGetOptions * pGetOpt, SKWaitOptions * pWaitOpt) {
    if(pPutOpt)  { delete pPutOpt; pPutOpt = NULL;  }
    if(pGetOpt)  { delete pGetOpt; pGetOpt = NULL;  }
    if(pWaitOpt) { delete pWaitOpt; pWaitOpt = NULL;}

    K keys = kK(OpsOptionsDictq)[0];
    K values = kK(OpsOptionsDictq)[1];
    int keysize=keys->n;
    int valsize=values->n;
    if(keys->t == -KC || keys->t == 0) 
    {
        for(int ii = 0; ii < keysize; ++ii)
        {
            K dkey = kK(keys)[ii];
            int kstrsize=dkey->n;
            K dval = kK(values)[ii];
            void * pOptionPointer =  (void *)KLong(dval);
            if( strncmp(STDSTRING(dkey).c_str(), "putOptions", 10) == 0 ) 
            {
                pPutOpt = (SKPutOptions *) pOptionPointer;
            }
            else if( strncmp(STDSTRING(dkey).c_str(), "getOptions", 10) == 0 )
            {
                pGetOpt = (SKGetOptions *) pOptionPointer;
            }
            else if( strncmp(STDSTRING(dkey).c_str(), "waitOptions", 11) == 0 )
            {
                pWaitOpt = (SKWaitOptions *) pOptionPointer;
            }
            // just ignore other cases ?
        }
    }
}

/*
K dht_nsoptions_new7( K storageTypeq, K consistencyProtocolq, K versionModeq, K revisionModeq, K opsOptionsDictq, K secondarySyncIntervalSecondsq, K segmentSizeq ) 
{
    try {
        SKStorageType::SKStorageType st = (SKStorageType::SKStorageType) ((int) (Kinteger(storageTypeq)));
        SKConsistency cp = (SKConsistency) ((int)(Kinteger(consistencyProtocolq)));
        SKVersionMode vm = (SKVersionMode) ((int)(Kinteger(versionModeq)));
        SKRevisionMode rm = (SKRevisionMode) ((int)(Kinteger(revisionModeq)));

        SKPutOptions * pPutOpt = NULL;
        SKGetOptions * pGetOpt = NULL;
        SKWaitOptions * pWaitOpt = NULL;
        get_options_from_dict(opsOptionsDictq, pPutOpt, pGetOpt, pWaitOpt);
        if(pPutOpt==NULL || pGetOpt == NULL || pWaitOpt == NULL ) {
            krr( "dht_nsoptions_new7 put/get/wait options missing" ) ;
        }
        
        int secondarySyncIntervalSeconds =  (int) (Kinteger(secondarySyncIntervalSecondsq));
        int segmentSize = (int) (Kinteger(segmentSizeq));
        SKNamespaceOptions * pNsOpt = new SKNamespaceOptions(st, cp, vm, rm, pPutOpt, pGetOpt, pWaitOpt, secondarySyncIntervalSeconds, segmentSize);
        return make_symbol( pNsOpt );
    } catch (...) {
        return exhandler( "NamespaceOptions creation failed", __FILE__, __LINE__);
    }
}

K dht_nsoptions_new8( K storageTypeq, K consistencyProtocolq, K versionModeq, K revisionModeq, K opsOptionsDictq, K secondarySyncIntervalSecondsq, K segmentSizeq, K allowLinksq) 
{
    try {
        SKStorageType::SKStorageType st = (SKStorageType::SKStorageType) ((int) (Kinteger(storageTypeq)));
        SKConsistency cp = (SKConsistency) ((int)(Kinteger(consistencyProtocolq)));
        SKVersionMode vm = (SKVersionMode) ((int)(Kinteger(versionModeq)));
        SKRevisionMode rm = (SKRevisionMode) ((int)(Kinteger(revisionModeq)));

        SKPutOptions * pPutOpt = NULL;
        SKGetOptions * pGetOpt = NULL;
        SKWaitOptions * pWaitOpt = NULL;
        get_options_from_dict(opsOptionsDictq, pPutOpt, pGetOpt, pWaitOpt);
        if(pPutOpt==NULL || pGetOpt == NULL || pWaitOpt == NULL ) {
            krr( "dht_nsoptions_new8 put/get/wait options missing" ) ;
        }

        int secondarySyncIntervalSeconds = (int) (Kinteger(secondarySyncIntervalSecondsq));
        int segmentSize = (int) (Kinteger(segmentSizeq));
        bool allowLinks = (allowLinksq->t==-KI) ? 
                (bool) kI(allowLinksq) : (allowLinksq->t==-KJ) ? (bool) kJ(allowLinksq) : (bool) kG(allowLinksq) ;
        SKNamespaceOptions * pNsOpt = new SKNamespaceOptions(st, cp, vm, rm, pPutOpt, pGetOpt, pWaitOpt, secondarySyncIntervalSeconds, segmentSize, allowLinks);
        return make_symbol( pNsOpt );
    } catch (...) {
        return exhandler( "NamespaceOptions creation failed", __FILE__, __LINE__);
    }
}
*/

K dht_nsoptions_getconsistencyprotocol( K nsoptionshq )
{
    try {
        SKNamespaceOptions * pNSOpts = (SKNamespaceOptions *) ((void *) KLong(nsoptionshq));
        return ki( (int) pNSOpts->getConsistencyProtocol() );
    }
    catch (...) {
        exhandler( "namespace options get consistencyprotocol failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}

K dht_nsoptions_getstoragetype( K nsoptionshq )
{
    try {
        SKNamespaceOptions * pNSOpts = (SKNamespaceOptions *) ((void *) KLong(nsoptionshq));
        return ki( (int) pNSOpts->getStorageType() );
    }
    catch (...) {
        exhandler( "namespace options get storagetype failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}

K dht_nsoptions_getversionmode( K nsoptionshq )
{
    try {
        SKNamespaceOptions * pNSOpts = (SKNamespaceOptions *) ((void *) KLong(nsoptionshq));
        return ki( (int) pNSOpts->getVersionMode() );
    }
    catch (...) {
        exhandler( "namespace options get versionmode failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}

K dht_nsoptions_getrevisionmode( K nsoptionshq )
{
    try {
        SKNamespaceOptions * pNSOpts = (SKNamespaceOptions *) ((void *) KLong(nsoptionshq));
        return ki( (int) pNSOpts->getRevisionMode() );
    }
    catch (...) {
        exhandler( "namespace options get Revision Mode failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}

K dht_nsoptions_getsegmentsize( K nsoptionshq )
{
    try {
        SKNamespaceOptions * pNSOpts = (SKNamespaceOptions *) ((void *) KLong(nsoptionshq));
        return ki( (int) pNSOpts->getSegmentSize() );
    }
    catch (...) {
        exhandler( "namespace options get Segment Size failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}

K dht_nsoptions_getallowlinks( K nsoptionshq )
{
    try {
        SKNamespaceOptions * pNSOpts = (SKNamespaceOptions *) ((void *) KLong(nsoptionshq));
        return kb( (int) pNSOpts->getAllowLinks() );
    }
    catch (...) {
        exhandler( "namespace options get AllowLinks failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}

K dht_nsoptions_getputopts( K nsoptionshq )
{
    try {
        SKNamespaceOptions * pNSOpts = (SKNamespaceOptions *) ((void *) KLong(nsoptionshq));
        return make_symbol( pNSOpts->getDefaultPutOptions() );
    }
    catch (...) {
        exhandler( "namespace options get default PutOptions failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}

K dht_nsoptions_getgetopts( K nsoptionshq )
{
    try {
        SKNamespaceOptions * pNSOpts = (SKNamespaceOptions *) ((void *) KLong(nsoptionshq));
        return make_symbol( pNSOpts->getDefaultGetOptions() );
    }
    catch (...) {
        exhandler( "namespace options get default GetOptions failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}

K dht_nsoptions_getwaitopts( K nsoptionshq )
{
    try {
        SKNamespaceOptions * pNSOpts = (SKNamespaceOptions *) ((void *) KLong(nsoptionshq));
        return make_symbol( pNSOpts->getDefaultWaitOptions() );
    }
    catch (...) {
        exhandler( "namespace options get default WaitOptions failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}

K dht_nsoptions_tostring( K nsoptionshq )
{
    try {
        SKNamespaceOptions * pNSOpts = (SKNamespaceOptions *) ((void *) KLong(nsoptionshq));
        if(pNSOpts) {
            char * pStr = pNSOpts->toString();
            if(pStr) {
                K str = kpn( pStr, strlen(pStr) );
                free(pStr);
                return str;
            }
        }
    }
    catch (...) {
        exhandler( "namespace options tostring failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}

K dht_nsoptions_storagetype( K nsoptq, K storageTypeq )
{
    try {
        SKStorageType::SKStorageType st = (SKStorageType::SKStorageType) ((int) (KInt(storageTypeq)));
        SKNamespaceOptions * pNsOpt = (SKNamespaceOptions *) nsoptq->j;
        return make_symbol( pNsOpt->storageType(st) );
    } catch (...) {
        return exhandler( "SKNamespaceOptions set storageType failed", __FILE__, __LINE__);
    }
}

K dht_nsoptions_consistencyprotocol( K nsoptq, K consistencyProtocolq )
{
    try {
        SKConsistency cp = (SKConsistency) ((int) (KInt(consistencyProtocolq)));
        SKNamespaceOptions * pNsOpt = (SKNamespaceOptions *) nsoptq->j;
        return make_symbol( pNsOpt->consistencyProtocol(cp) );
    } catch (...) {
        return exhandler( "SKNamespaceOptions set consistencyProtocol failed", __FILE__, __LINE__);
    }
}

K dht_nsoptions_versionmode( K nsoptq, K versionModeq )
{
    try {
        SKVersionMode vm = (SKVersionMode) ((int) (KInt(versionModeq)));
        SKNamespaceOptions * pNsOpt = (SKNamespaceOptions *) nsoptq->j;
        return make_symbol( pNsOpt->versionMode(vm) );
    } catch (...) {
        return exhandler( "SKNamespaceOptions set versionMode failed", __FILE__, __LINE__);
    }
}

K dht_nsoptions_revisionmode( K nsoptq, K revisionModeq )
{
    try {
        SKRevisionMode rm = (SKRevisionMode) ((int) (KInt(revisionModeq)));
        SKNamespaceOptions * pNsOpt = (SKNamespaceOptions *) nsoptq->j;
        return make_symbol( pNsOpt->revisionMode(rm) );
    } catch (...) {
        return exhandler( "SKNamespaceOptions set revisionMode failed", __FILE__, __LINE__);
    }
}

K dht_nsoptions_segmentsize( K nsoptq, K segmentSizeq )
{
    try {
        int segmentSize = (int) (KInt(segmentSizeq));
        SKNamespaceOptions * pNsOpt = (SKNamespaceOptions *) nsoptq->j;
        return make_symbol( pNsOpt->segmentSize(segmentSize) );
    } catch (...) {
        return exhandler( "SKNamespaceOptions set segmentSize failed", __FILE__, __LINE__);
    }
}

K dht_nsoptions_allowlinks( K nsoptq, K allowLinksq )
{
    try {
        bool allowLinks = (allowLinksq->t==-KI) ? 
                (bool) kI(allowLinksq) : (bool) kG(allowLinksq) ;
        SKNamespaceOptions * pNsOpt = (SKNamespaceOptions *) nsoptq->j;
        return make_symbol( pNsOpt->allowLinks(allowLinks) );
    } catch (...) {
        return exhandler( "SKNamespaceOptions set allowLinks failed", __FILE__, __LINE__);
    }
}

K dht_nsoptions_defaultputoptions( K nsOptq , K defaultPutOpsq)
{
    try {
        SKPutOptions * putOpts = (SKPutOptions *) defaultPutOpsq->j;
        SKNamespaceOptions * nsOpt = (SKNamespaceOptions *) nsOptq->j;
        return make_symbol( nsOpt->defaultPutOptions(putOpts) );
    } catch (...) {
        return exhandler( "NamespaceOptions defaultPutOptions failed", __FILE__, __LINE__);
    }
}

K dht_nsoptions_getsecondarysyncintervalseconds( K nsOptq )
{
    try {
        SKNamespaceOptions * pNSOpts = (SKNamespaceOptions *) ((void *) KLong(nsOptq));
        return ki( (int) pNSOpts->getSecondarySyncIntervalSeconds() );
    }
    catch (...) {
        exhandler( "namespace options get SecondarySyncIntervalSeconds failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}

K dht_nsoptions_secondarysyncintervalseconds( K nsOptq , K secondsq)
{
    try {
        SKNamespaceOptions * pNSOpts = (SKNamespaceOptions *) ((void *) KLong(nsOptq));
        int seconds = (int) (KInt(secondsq));
        SKNamespaceOptions * pNsOpts = (SKNamespaceOptions *) nsOptq->j;
        return make_symbol( pNsOpts->secondarySyncIntervalSeconds(seconds) );
    }
    catch (...) {
        exhandler( "namespace options set SecondarySyncIntervalSeconds failed", __FILE__, __LINE__);
    }
}

K dht_nsoptions_delete( K nsoptionshq )
{
    try {
        SKNamespaceOptions * pnso = (SKNamespaceOptions *) ((void *) KLong(nsoptionshq));
        delete pnso;
        nsoptionshq->j = 0;
        return _kNULL;
    }
    catch (...) {
        exhandler("namespaceoptions delete failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}

//------------------------ SKStoredValue --------------------------------
K dht_storedvalue_getstoredlength( K storedvalHandleq ) 
{
    try {
        return ki( ((SKStoredValue *) storedvalHandleq->j)->getStoredLength() );
    }
    catch (...) {
        return exhandler( "StoredValue get storedlength failed", __FILE__, __LINE__);
    }
}

K dht_storedvalue_getuncompressedlength( K storedvalHandleq ) 
{
    try {
        return ki( ((SKStoredValue *) storedvalHandleq->j)->getUncompressedLength() );
    }
    catch (...) {
        return exhandler( "StoredValue get UncompressedLength failed", __FILE__, __LINE__);
    }
}

K dht_storedvalue_getcompression( K storedvalHandleq ) 
{
    try {
        return ki( (int) ((SKStoredValue *) storedvalHandleq->j)->getCompression() );
    }
    catch (...) {
        return exhandler( "StoredValue get Compression failed", __FILE__, __LINE__);
    }
}

K dht_storedvalue_getchecksumtype( K storedvalHandleq ) 
{
    try {
        return ki( (int) ((SKStoredValue *) storedvalHandleq->j)->getChecksumType() );
    }
    catch (...) {
        return exhandler( "StoredValue get ChecksumType failed", __FILE__, __LINE__);
    }
}

K dht_storedvalue_getversion( K storedvalHandleq ) 
{
    try {
        return kj( ((SKStoredValue *) storedvalHandleq->j)->getVersion() );
    }
    catch (...) {
        return exhandler( "StoredValue get Version failed", __FILE__, __LINE__);
    }
}

K dht_storedvalue_getcreationtime( K storedvalHandleq ) 
{
    try {
        return kz(zu( ((SKStoredValue *) storedvalHandleq->j)->getCreationTime()/1000 ));
    }
    catch (...) {
        return exhandler( "StoredValue get CreationTime failed", __FILE__, __LINE__);
    }
}

K dht_storedvalue_getvalue( K storedvalHandleq ) 
{
    try {
        return internal_storedvalue_get_value( (SKStoredValue *) storedvalHandleq->j );
    }
    catch (...) {
        return exhandler( "StoredValue get Value failed", __FILE__, __LINE__);
    }
}

K dht_storedvalue_getuserdata( K storedvalHandleq ) 
{
    try {
        return internal_storedvalue_get_userdata( (SKStoredValue *) storedvalHandleq->j );
    }
    catch (...) {
        return exhandler( "StoredValue get userdata failed", __FILE__, __LINE__);
    }
}

K dht_storedvalue_getchecksum( K storedvalHandleq ) 
{
    try {
        return internal_storedvalue_get_checksum( (SKStoredValue *) storedvalHandleq->j );
    }
    catch (...) {
        return exhandler( "StoredValue get checksum failed", __FILE__, __LINE__);
    }
}

K dht_storedvalue_getcreatorid( K storedvalHandleq ) 
{
    try {
        return internal_storedvalue_get_creatorid( (SKStoredValue *) storedvalHandleq->j );
    }
    catch (...) {
        return exhandler( "StoredValue get creator PID failed", __FILE__, __LINE__);
    }
}

K dht_storedvalue_getcreatorip( K storedvalHandleq ) 
{
    try {
        return internal_storedvalue_get_creatorip( (SKStoredValue *) storedvalHandleq->j );
    }
    catch (...) {
        return exhandler( "StoredValue get creator IP failed", __FILE__, __LINE__);
    }
}

K dht_storedvalue_todict( K storedvalHandleq ) 
{
    try {
        return internal_storedvalue_todict( (SKStoredValue *) storedvalHandleq->j );
    }
    catch (...) {
        return exhandler( "StoredValue todict failed", __FILE__, __LINE__);
    }
}

K dht_storedvalue_delete( K storedvalHandleq )
{
    try {
        SKStoredValue * psv = (SKStoredValue *) ((void *) KLong(storedvalHandleq));
        delete psv;
        storedvalHandleq->j = 0;
        return _kNULL;
    }
    catch (...) {
        exhandler("storedvalue delete failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}

//------------------------ SKSyncNSPerspective --------------------------------

K dht_syncnsp_put( K snspHandleq, K keyq, K valueq ) 
{
    try {
        if(kC(keyq)[keyq->n] != 0){
           kC(keyq)[keyq->n] = 0;
        }
        if (valueq->t == KC && kC(valueq)[valueq->n] != 0)
        {
            kC(valueq)[valueq->n] = 0;
        }
        return internal_put( static_cast<SKSyncWritableNSPerspective*>((SKSyncNSPerspective *) snspHandleq->j), keyq, valueq );
    }
    catch (...) {
        exhandler( "syncnsp put failed", __FILE__, __LINE__);
    }
    return K(0) ;    
}

K dht_syncnsp_putwo( K snspHandleq, K keyq, K valueq, K putopthq ) 
{
    try {
        if(kC(keyq)[keyq->n] != 0){
           kC(keyq)[keyq->n] = 0;
        }
        if (valueq->t == KC && kC(valueq)[valueq->n] != 0)
        {
            kC(valueq)[valueq->n] = 0;
        }
        return internal_put( static_cast<SKSyncWritableNSPerspective*>((SKSyncNSPerspective *) snspHandleq->j), keyq, valueq, (SKPutOptions *) putopthq->j );
    } catch (...) {
        exhandler( "syncnsp put failed", __FILE__, __LINE__);
    }
    return K(0) ;    
}

K dht_syncnsp_mputwo( K snspHandleq, K keylistq, K valuelistq, K putopthq ) 
{
    try {
        if ( keylistq->n == valuelistq->n ) {
            internal_nullterm_list(keylistq);
            internal_nullterm_list(valuelistq);
            return internal_mput( static_cast<SKSyncWritableNSPerspective*>((SKSyncNSPerspective *) snspHandleq->j), keylistq, valuelistq, (SKPutOptions *) putopthq->j );
        }
        krr( "dht_syncnsp_mput: keylist and valuelist sizes do not match" ) ;
    } catch (...) {
        exhandler( "syncnsp mput failed", __FILE__, __LINE__);
    }
    return K(0) ;    
}

K dht_syncnsp_mput( K snspHandleq, K keylistq, K valuelistq ) 
{
    try {
        if ( keylistq->n == valuelistq->n ) {
            internal_nullterm_list(keylistq);
            internal_nullterm_list(valuelistq);
            return internal_mput( static_cast<SKSyncWritableNSPerspective*>((SKSyncNSPerspective *) snspHandleq->j), keylistq, valuelistq );
        }
        krr( "dht_syncnsp_mput: keylist and valuelist sizes do not match" ) ;
    } catch (...) {
        exhandler( "syncnsp mput failed", __FILE__, __LINE__);
    }
    return K(0) ;    

}

K dht_syncnsp_mputdictwo( K snspHandleq, K dictq, K putopthq ) 
{
    try {
        K keylistq = kK(dictq)[0] ; 
        K valuelistq = kK(dictq)[1] ; 
        return dht_syncnsp_mputwo(snspHandleq, keylistq, valuelistq, putopthq );
    } catch (...) {
        exhandler( "syncnsp mput failed", __FILE__, __LINE__);
    }
}

K dht_syncnsp_mputdict( K snspHandleq, K dictq ) 
{
    K keylistq = kK(dictq)[0] ; 
    K valuelistq = kK(dictq)[1] ; 
    return dht_syncnsp_mput(snspHandleq, keylistq, valuelistq );
}

K dht_syncnsp_get( K snspHandleq, K keyq )
{
    try {
        return internal_get( static_cast<SKSyncReadableNSPerspective*>((SKSyncNSPerspective *) snspHandleq->j), keyq, false ) ;
    } catch (...) {
        exhandler( "syncnsp get failed", __FILE__, __LINE__);
    }
}

K dht_syncnsp_waitfor( K snspHandleq, K keyq )
{
    try {
        return internal_get( static_cast<SKSyncReadableNSPerspective*>((SKSyncNSPerspective *) snspHandleq->j), keyq, true ) ;
    } catch (...) {
        exhandler( "syncnsp waitfor failed", __FILE__, __LINE__);
    }
}

K dht_syncnsp_getwo( K snspHandleq, K keyq, K getopsHandleq )
{
    try {
        SKGetOptions* pGetOpt =  (SKGetOptions*)getopsHandleq->j;
        SKStoredValue * psv = internal_getwo( static_cast<SKSyncReadableNSPerspective*>((SKSyncNSPerspective *) snspHandleq->j), keyq, false, pGetOpt );
        return (psv!=NULL) ? make_symbol(psv) : K(0);
    } catch (...) {
        exhandler( "syncnsp get failed", __FILE__, __LINE__);
    }
}

K dht_syncnsp_waitforwo( K snspHandleq, K keyq, K waitopsHandleq )
{
    try {
        SKWaitOptions* pWaitOpt =  (SKWaitOptions*)waitopsHandleq->j;
        SKStoredValue * psv = internal_getwo( static_cast<SKSyncReadableNSPerspective*>((SKSyncNSPerspective *) snspHandleq->j), keyq, true, pWaitOpt );
        return (psv!=NULL) ? make_symbol(psv) : K(0);
    } catch (...) {
        exhandler( "syncnsp get failed", __FILE__, __LINE__);
    }
}

K dht_syncnsp_getasdictwo( K snspHandleq, K keyq, K getopsHandleq )
{
    try {
        SKGetOptions* pGetOpt =  (SKGetOptions*)getopsHandleq->j ;
        return internal_getwo_asdict( static_cast<SKSyncReadableNSPerspective*>((SKSyncNSPerspective *) snspHandleq->j), keyq, false, pGetOpt );
    } catch (...) {
        exhandler( "syncnsp get failed", __FILE__, __LINE__);
    }
}

K dht_syncnsp_getmeta( K snspHandleq, K keyq, K getopsHandleq ) 
{
    return dht_syncnsp_getasdictwo( snspHandleq, keyq, getopsHandleq );
}


K dht_syncnsp_waitforasdictwo( K snspHandleq, K keyq, K waitopsHandleq )
{
    try {
        SKWaitOptions* pWaitOpt =  (SKWaitOptions*)waitopsHandleq->j ;
        return internal_getwo_asdict( static_cast<SKSyncReadableNSPerspective*>((SKSyncNSPerspective *) snspHandleq->j), keyq, true, pWaitOpt );
    } catch (...) {
        exhandler( "syncnsp waitfor with options failed", __FILE__, __LINE__);
    }
}

K dht_syncnsp_mget( K snspHandleq, K keylistq )
{
    try {
        internal_nullterm_list(keylistq);
        return internal_mget( static_cast<SKSyncReadableNSPerspective*>((SKSyncNSPerspective *) snspHandleq->j), keylistq, false ) ;
    } catch (...) {
        return exhandler( "syncnsp mget failed", __FILE__, __LINE__);
    }
}

K dht_syncnsp_mwait( K snspHandleq, K keylistq )
{
    try {
        internal_nullterm_list(keylistq);
        return internal_mget( static_cast<SKSyncReadableNSPerspective*>((SKSyncNSPerspective *) snspHandleq->j), keylistq , true) ;
    } catch (...) {
        exhandler( "syncnsp waitfor failed", __FILE__, __LINE__);
    }
}

K dht_syncnsp_mgetwo( K snspHandleq, K keylistq, K getoptsHandleq )
{
    try {
        internal_nullterm_list(keylistq);
        return internal_mgetwo( static_cast<SKSyncReadableNSPerspective*>((SKSyncNSPerspective *) snspHandleq->j), keylistq, false, getoptsHandleq ) ;
    } catch (...) {
        exhandler( "syncnsp get failed", __FILE__, __LINE__);
    }
}

K dht_syncnsp_mwaitwo( K snspHandleq, K keylistq, K waitoptsHandleq )
{
    try {
        internal_nullterm_list(keylistq);
        return internal_mgetwo( static_cast<SKSyncReadableNSPerspective*>((SKSyncNSPerspective *) snspHandleq->j), keylistq , true, waitoptsHandleq) ;
    } catch (...) {
        exhandler( "syncnsp waitfor failed", __FILE__, __LINE__);
    }
}

K dht_syncnsp_mgetmeta( K snspHandleq, K keylistq, K getopsHandleq ) 
{
    try {
        internal_nullterm_list(keylistq);
        return internal_mgetwo_asdict( static_cast<SKSyncReadableNSPerspective*>((SKSyncNSPerspective *) snspHandleq->j), keylistq, false, getopsHandleq );
    } catch (...) {
        exhandler( "syncnsp get failed", __FILE__, __LINE__);
    }
}

K dht_syncnsp_getoptions( K snspHandleq )
{
    try {
        SKBaseNSPerspective * bp = static_cast<SKBaseNSPerspective*>((SKSyncNSPerspective *) snspHandleq->j);
        return make_symbol( bp->getOptions() );
    } catch (...) {
        exhandler( "syncnsp get options failed", __FILE__, __LINE__);
    }
}

K dht_syncnsp_getnamespace( K snspHandleq )
{
    try {
        SKBaseNSPerspective * bp = static_cast<SKBaseNSPerspective*>((SKSyncNSPerspective *) snspHandleq->j);
        return make_symbol( bp->getNamespace() );
    } catch (...) {
        exhandler( "syncnsp get namespace failed", __FILE__, __LINE__);
    }
}

K dht_syncnsp_getname( K snspHandleq )
{
    try {
        SKBaseNSPerspective * bp = static_cast<SKBaseNSPerspective*>((SKSyncNSPerspective *) snspHandleq->j);
        string str = bp->getName() ;
        return kpn( const_cast<char*>(str.c_str()), str.size());
    } catch (...) {
        exhandler( "syncnsp get name failed", __FILE__, __LINE__);
    }
}

K dht_syncnsp_setoptions( K syncnspq , K nsPerspOptq)
{
    try {
        SKBaseNSPerspective * bp = static_cast<SKBaseNSPerspective*>((SKSyncNSPerspective *) syncnspq->j);
        SKNamespacePerspectiveOptions * pNsPOpt = (SKNamespacePerspectiveOptions *) nsPerspOptq->j;
        bp->setOptions(pNsPOpt);
        return _kNULL ;
    } catch (...) {
        return exhandler( "syncnsp set default NamespacePerspectiveOptions failed", __FILE__, __LINE__);
    }
}

K dht_syncnsp_setdefaultversion( K syncnspq , K versionq)
{
    try {
        SKBaseNSPerspective * bp = static_cast<SKBaseNSPerspective*>((SKSyncNSPerspective *) syncnspq->j);
        int64_t version  = (int64_t) (kJ(versionq));
        bp->setDefaultVersion(version);
        return _kNULL ;
    } catch (...) {
        return exhandler( "syncnsp set default version failed", __FILE__, __LINE__);
    }
}

K dht_syncnsp_setretrievalverconstraint( K syncnspq , K versionConstraintq)
{
    try {
        SKBaseNSPerspective * bp = static_cast<SKBaseNSPerspective*>((SKSyncNSPerspective *) syncnspq->j);
        SKVersionConstraint * pVersionConstraint = (SKVersionConstraint *) versionConstraintq->j;
        bp->setDefaultRetrievalVersionConstraint(pVersionConstraint);
        return _kNULL ;
    } catch (...) {
        return exhandler( "syncnsp set default Retrieval VersionConstraint failed", __FILE__, __LINE__);
    }
}

K dht_syncnsp_setversionprovider( K syncnspq , K versionProviderq)
{
    try {
        SKBaseNSPerspective * bp = static_cast<SKBaseNSPerspective*>((SKSyncNSPerspective *) syncnspq->j);
        SKVersionProvider * pVersionProvider = (SKVersionProvider *) versionProviderq->j;
        bp->setDefaultVersionProvider(pVersionProvider);
        return _kNULL ;
    } catch (...) {
        return exhandler( "syncnsp set default VersionProvider failed", __FILE__, __LINE__);
    }
}

K dht_syncnsp_close( K syncnspq )
{
    try {
        SKBaseNSPerspective * bp = static_cast<SKBaseNSPerspective*>((SKSyncNSPerspective *) syncnspq->j);
        bp->close();
        return _kNULL ;
    } catch (...) {
        exhandler( "syncnsp close failed", __FILE__, __LINE__);
    }
}

K dht_syncnsp_delete( K snspHandleq )
{
    try {
        SKSyncNSPerspective * snsp = (SKSyncNSPerspective *) ((void *) KLong(snspHandleq));
        delete snsp;
        snspHandleq->j = 0;
        return _kNULL;
    }
    catch (...) {
        exhandler("syncnsp delete failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}

//------------------------ SKAsyncNSPerspective --------------------------------

K dht_asyncnsp_put( K anspHandleq, K keyq, K valueq ) 
{
    try {
        if(kC(keyq)[keyq->n] != 0){
           kC(keyq)[keyq->n] = 0;
        }
        return internal_awnsp_put( static_cast<SKAsyncWritableNSPerspective*>((SKAsyncNSPerspective *) anspHandleq->j), keyq, valueq );
    }
    catch (...) {
        exhandler( "asyncnsp put failed", __FILE__, __LINE__);
    }
    return K(0) ;    
}

K dht_asyncnsp_putwo( K anspHandleq, K keyq, K valueq, K putopthq ) 
{
    try {
        if(kC(keyq)[keyq->n] != 0){
           kC(keyq)[keyq->n] = 0;
        }
        return internal_awnsp_put( static_cast<SKAsyncWritableNSPerspective*>((SKAsyncNSPerspective *) anspHandleq->j), keyq, valueq, (SKPutOptions *) putopthq->j );
    } catch (...) {
        exhandler( "asyncnsp put failed", __FILE__, __LINE__);
    }
    return K(0) ;    
}

K dht_asyncnsp_mputwo( K anspHandleq, K keylistq, K valuelistq, K putopthq ) 
{
    try {
        if ( keylistq->n == valuelistq->n ) {
            internal_nullterm_list(keylistq);
            internal_nullterm_list(valuelistq);
            return internal_awnsp_mput( static_cast<SKAsyncWritableNSPerspective*>((SKAsyncNSPerspective *) anspHandleq->j), keylistq, valuelistq, (SKPutOptions *) putopthq->j );
        }
        krr( "dht_asyncnsp_mput: keylist and valuelist sizes do not match" ) ;
    } catch (...) {
        exhandler( "ansp mput failed", __FILE__, __LINE__);
    }
    return K(0) ;    
}

K dht_asyncnsp_mput( K anspHandleq, K keylistq, K valuelistq ) 
{
    return dht_asyncnsp_mputwo( anspHandleq, keylistq, valuelistq, _kNULL );
}

K dht_asyncnsp_mputdictwo( K anspHandleq, K dictq, K putopthq ) 
{
    try {
        //internal_nullterm_dict(dictq);
        K keylistq = kK(dictq)[0] ; 
        K valuelistq = kK(dictq)[1] ; 
        return dht_asyncnsp_mputwo(anspHandleq, keylistq, valuelistq, putopthq );
    } catch (...) {
        exhandler( "asyncnsp mput failed", __FILE__, __LINE__);
    }
}

K dht_asyncnsp_mputdict( K anspHandleq, K dictq ) 
{
    return dht_asyncnsp_mputdictwo(anspHandleq, dictq, _kNULL );
}

K dht_asyncnsp_get( K anspHandleq, K keyq )
{
    try {
        return internal_arnsp_get( static_cast<SKAsyncReadableNSPerspective*>((SKAsyncNSPerspective *) anspHandleq->j), keyq, false ) ;
    } catch (...) {
        exhandler( "asyncnsp get failed", __FILE__, __LINE__);
    }
}

K dht_asyncnsp_waitfor( K anspHandleq, K keyq )
{
    try {
        return internal_arnsp_get( static_cast<SKAsyncReadableNSPerspective*>((SKAsyncNSPerspective *) anspHandleq->j), keyq, true ) ;
    } catch (...) {
        exhandler( "asyncnsp waitfor failed", __FILE__, __LINE__);
    }
}

K dht_asyncnsp_mget( K anspHandleq, K keylistq )
{
    try {
        internal_nullterm_list(keylistq);
        return internal_arnsp_mget( static_cast<SKAsyncReadableNSPerspective*>((SKAsyncNSPerspective *) anspHandleq->j), keylistq, false ) ;
    } catch (...) {
        exhandler( "asyncnsp get failed", __FILE__, __LINE__);
    }
}

K dht_asyncnsp_mwait( K anspHandleq, K keylistq )
{
    try {
        internal_nullterm_list(keylistq);
        return internal_arnsp_mget( static_cast<SKAsyncReadableNSPerspective*>((SKAsyncNSPerspective *) anspHandleq->j), keylistq , true) ;
    } catch (...) {
        exhandler( "asyncnsp waitfor failed", __FILE__, __LINE__);
    }
}

K dht_asyncnsp_mgetwo( K anspHandleq, K keylistq, K getoptsHandleq )
{
    try {
        internal_nullterm_list(keylistq);
        return internal_arnsp_mgetwo( static_cast<SKAsyncReadableNSPerspective*>((SKAsyncNSPerspective *) anspHandleq->j), keylistq, false, getoptsHandleq ) ;
    } catch (...) {
        exhandler( "Asyncnsp mget failed", __FILE__, __LINE__);
    }
}

K dht_asyncnsp_mwaitwo( K anspHandleq, K keylistq, K waitoptsHandleq )
{
    try {
        internal_nullterm_list(keylistq);
        return internal_arnsp_mgetwo( static_cast<SKAsyncReadableNSPerspective*>((SKAsyncNSPerspective *) anspHandleq->j), keylistq , true, waitoptsHandleq) ;
    } catch (...) {
        exhandler( "Asyncnsp mwaitfor failed", __FILE__, __LINE__);
    }
}

K dht_asyncnsp_getoptions( K anspHandleq )
{
    try {
        SKBaseNSPerspective * bp = static_cast<SKBaseNSPerspective*>((SKAsyncNSPerspective *) anspHandleq->j);
        return make_symbol( bp->getOptions() );
    } catch (...) {
        exhandler( "asyncnsp get options failed", __FILE__, __LINE__);
    }
}

K dht_asyncnsp_getnamespace( K anspHandleq )
{
    try {
        SKBaseNSPerspective * bp = static_cast<SKBaseNSPerspective*>((SKAsyncNSPerspective *) anspHandleq->j);
        return make_symbol( bp->getNamespace() );
    } catch (...) {
        exhandler( "asyncnsp get namespace failed", __FILE__, __LINE__);
    }
}

K dht_asyncnsp_getname( K anspHandleq )
{
    try {
        SKBaseNSPerspective * bp = static_cast<SKBaseNSPerspective*>((SKAsyncNSPerspective *) anspHandleq->j);
        string str = bp->getName() ;
        return kpn( const_cast<char*>(str.c_str()), str.size());
    } catch (...) {
        exhandler( "asyncnsp get name failed", __FILE__, __LINE__);
    }
}

K dht_asyncnsp_setoptions( K asyncnspq , K nsPerspOptq)
{
    try {
        SKBaseNSPerspective * bp = static_cast<SKBaseNSPerspective*>((SKAsyncNSPerspective *) asyncnspq->j);
        SKNamespacePerspectiveOptions * pNsPOpt = (SKNamespacePerspectiveOptions *) nsPerspOptq->j;
        bp->setOptions(pNsPOpt);
        return _kNULL ;
    } catch (...) {
        return exhandler( "asyncnsp set default NamespacePerspectiveOptions failed", __FILE__, __LINE__);
    }
}

K dht_asyncnsp_setdefaultversion( K asyncnspq , K versionq)
{
    try {
        SKBaseNSPerspective * bp = static_cast<SKBaseNSPerspective*>((SKAsyncNSPerspective *) asyncnspq->j);
        int64_t version  = (int64_t) (kJ(versionq));
        bp->setDefaultVersion(version);
        return _kNULL ;
    } catch (...) {
        return exhandler( "asyncnsp set default version failed", __FILE__, __LINE__);
    }
}

K dht_asyncnsp_setretrievalverconstraint( K asyncnspq , K versionConstraintq)
{
    try {
        SKBaseNSPerspective * bp = static_cast<SKBaseNSPerspective*>((SKAsyncNSPerspective *) asyncnspq->j);
        SKVersionConstraint * pVersionConstraint = (SKVersionConstraint *) versionConstraintq->j;
        bp->setDefaultRetrievalVersionConstraint(pVersionConstraint);
        return _kNULL ;
    } catch (...) {
        return exhandler( "asyncnsp set default Retrieval VersionConstraint failed", __FILE__, __LINE__);
    }
}

K dht_asyncnsp_setversionprovider( K asyncnspq , K versionProviderq)
{
    try {
        SKBaseNSPerspective * bp = static_cast<SKBaseNSPerspective*>((SKAsyncNSPerspective *) asyncnspq->j);
        SKVersionProvider * pVersionProvider = (SKVersionProvider *) versionProviderq->j;
        bp->setDefaultVersionProvider(pVersionProvider);
        return _kNULL ;
    } catch (...) {
        return exhandler( "asyncnsp set default VersionProvider failed", __FILE__, __LINE__);
    }
}

K dht_asyncnsp_close( K asyncnspq )
{
    try {
        SKBaseNSPerspective * bp = static_cast<SKBaseNSPerspective*>((SKAsyncNSPerspective *) asyncnspq->j);
        bp->close();
        return _kNULL ;
    } catch (...) {
        exhandler( "asyncnsp close failed", __FILE__, __LINE__);
    }
}

K dht_asyncnsp_delete( K anspHandleq )
{
    try {
        SKAsyncNSPerspective * ansp = (SKAsyncNSPerspective *) ((void *) KLong(anspHandleq));
        delete ansp;
        anspHandleq->j = 0;
        return _kNULL;
    }
    catch (...) {
        exhandler("asyncnsp delete failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}
//------------------------ SKAsyncSnapshot ----------------------------------
/*
K dht_asnapshot_getstate( K asnapshotHandleq )
{
    try {
        return ki( (int)((SKAsyncSnapshot *) asnapshotHandleq->j)->getState() );
    } catch (...) {
        exhandler( "async snapshot get state failed", __FILE__, __LINE__);
    }
}

K dht_asnapshot_getfailurecause( K asnapshotHandleq )
{
    try {
        return ki( (int)((SKAsyncSnapshot *) asnapshotHandleq->j)->getFailureCause() );
    } catch (...) {
        exhandler( "async snapshot get FailureCause failed", __FILE__, __LINE__);
    }
}

K dht_asnapshot_waitforcompletion( K asnapshotHandleq )
{
    try {
        ((SKAsyncSnapshot *) asnapshotHandleq->j)->waitForCompletion();
        return kb( 1 );
    } catch (...) {
        exhandler( "async snapshot waitforcompletion failed", __FILE__, __LINE__);
    }
}

K dht_asnapshot_waitforcompletiontm( K asnapshotHandleq, K timeoutq, K timeunitq )
{
    try {
        return kb( (int)((SKAsyncSnapshot *) asnapshotHandleq->j)->waitForCompletion((int)KInt(timeoutq), (SKTimeUnit)((int)KInt(timeunitq))) );
    } catch (...) {
        exhandler( "async snapshot waitforcompletion failed", __FILE__, __LINE__);
    }
}

K dht_asnapshot_close( K asnapshotHandleq )
{
    try {
        ((SKAsyncSnapshot *) asnapshotHandleq->j)->close();
        return kb( 1 );
    } catch (...) {
        exhandler( "async snapshot close failed", __FILE__, __LINE__);
    }
}

K dht_asnapshot_delete( K asnapshotHandleq )
{
    try {
        SKAsyncSnapshot * asnapshot = (SKAsyncSnapshot *) ((void *) KLong(asnapshotHandleq));
        delete asnapshot;
        asnapshotHandleq->j = 0;
        return _kNULL;
    }
    catch (...) {
        exhandler("async snapshot delete failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}
*/

//------------------------ SKAsyncSyncRequest --------------------------------
/*
K dht_asyncreq_getstate( K asyncreqHandleq )
{
    try {
        return ki( (int)((SKAsyncSyncRequest *) asyncreqHandleq->j)->getState() );
    } catch (...) {
        exhandler( "async request get state failed", __FILE__, __LINE__);
    }
}

K dht_asyncreq_getfailurecause( K asyncreqHandleq )
{
    try {
        return ki( (int)((SKAsyncSyncRequest *) asyncreqHandleq->j)->getFailureCause() );
    } catch (...) {
        exhandler( "async request get FailureCause failed", __FILE__, __LINE__);
    }
}

K dht_asyncreq_waitforcompletion( K asyncreqHandleq )
{
    try {
        ((SKAsyncSyncRequest *) asyncreqHandleq->j)->waitForCompletion();
        return kb( 1 );
    } catch (...) {
        exhandler( "async request waitforcompletion failed", __FILE__, __LINE__);
    }
}

K dht_asyncreq_waitforcompletiontm( K asyncreqHandleq, K timeoutq, K timeunitq )
{
    try {
        return kb( (int)((SKAsyncSyncRequest *) asyncreqHandleq->j)->waitForCompletion((int)KInt(timeoutq), (SKTimeUnit)((int)KInt(timeunitq))) );
    } catch (...) {
        exhandler( "async request waitforcompletion failed", __FILE__, __LINE__);
    }
}

K dht_asyncreq_close( K asyncreqHandleq )
{
    try {
        ((SKAsyncSyncRequest *) asyncreqHandleq->j)->close();
        return kb( 1 );
    } catch (...) {
        exhandler( "async request close failed", __FILE__, __LINE__);
    }
}

K dht_asyncreq_delete( K asyncreqHandleq )
{
    try {
        SKAsyncSyncRequest * syncreq = (SKAsyncSyncRequest *) ((void *) KLong(asyncreqHandleq));
        delete syncreq;
        asyncreqHandleq->j = 0;
        return _kNULL;
    }
    catch (...) {
        exhandler("async request delete failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}
*/

//------------------------ SKAsyncPut ---------------------------------------
K dht_asyncput_getstate( K asyncputHandleq )
{
    try {
        return ki( (int)((SKAsyncPut *) asyncputHandleq->j)->getState() );
    } catch (...) {
        exhandler( "asyncput get state failed", __FILE__, __LINE__);
    }
}

K dht_asyncput_getfailurecause( K asyncputHandleq )
{
    try {
        return ki( (int)((SKAsyncPut *) asyncputHandleq->j)->getFailureCause() );
    } catch (...) {
        exhandler( "asyncput get FailureCause failed", __FILE__, __LINE__);
    }
}

K dht_asyncput_waitforcompletion( K asyncputHandleq )
{
    try {
        ((SKAsyncPut *) asyncputHandleq->j)->waitForCompletion();
        return kb( 1 );
    } catch (...) {
        exhandler( "asyncput waitforcompletion failed", __FILE__, __LINE__);
    }
}

K dht_asyncput_waitforcompletiontm( K asyncputHandleq, K timeoutq, K timeunitq )
{
    try {
        return kb( (int)((SKAsyncPut *) asyncputHandleq->j)->waitForCompletion((int)KInt(timeoutq), (SKTimeUnit)((int)KInt(timeunitq))) );
    } catch (...) {
        exhandler( "asyncput waitforcompletion failed", __FILE__, __LINE__);
    }
}

K dht_asyncput_close( K asyncputHandleq )
{
    try {
        ((SKAsyncPut *) asyncputHandleq->j)->close();
        return kb( 1 );
    } catch (...) {
        exhandler( "asyncput close failed", __FILE__, __LINE__);
    }
}

K dht_asyncput_getoperationstate( K asyncputHandleq, K keyq )
{
    try {
        return ki( ((SKAsyncPut *) asyncputHandleq->j)->getOperationState(STDSTRING(keyq)) );
    } catch (...) {
        exhandler( "asyncput get OperationState failed", __FILE__, __LINE__);
    }
}

K dht_asyncput_getkeys( K asyncputHandleq )
{
    try {
        return internal_asynckeyedop_get_keys(static_cast<SKAsyncKeyedOperation*>((SKAsyncPut *) asyncputHandleq->j), false);
    } catch (...) {
        exhandler( "asyncput get keys failed", __FILE__, __LINE__);
    }
}

K dht_asyncput_getincompletekeys( K asyncputHandleq )
{
    try {
        return internal_asynckeyedop_get_keys(static_cast<SKAsyncKeyedOperation*>((SKAsyncPut *) asyncputHandleq->j), true);
    } catch (...) {
        return exhandler( "asyncput get IncompleteKeys failed", __FILE__, __LINE__);
    }
}

K dht_asyncput_getoperationstatemap( K asyncputHandleq )
{
    try {
        return internal_asynckeyedop_get_opstatemap(static_cast<SKAsyncKeyedOperation*>((SKAsyncPut *) asyncputHandleq->j) );
    } catch (...) {
        return exhandler( "asyncput get OperationState map failed", __FILE__, __LINE__);
    }
}

K dht_asyncput_getnumkeys ( K asyncputHandleq )
{
    try {
        SKAsyncPut * pAsyncPut = (SKAsyncPut *) ((void *) KLong(asyncputHandleq));
        return ki( (int) (pAsyncPut->getNumKeys()) );
    }
    catch (...) {
        return exhandler( "asyncput get NumKeys failed", __FILE__, __LINE__);
    }
}

K dht_asyncput_delete( K asyncputHandleq )
{
    try {
        SKAsyncPut * asyncput = (SKAsyncPut *) ((void *) KLong(asyncputHandleq));
        delete asyncput;
        asyncputHandleq->j = 0;
        return _kNULL;
    }
    catch (...) {
        exhandler("asyncput delete failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}

//------------------------ SKAsyncRetrieval --------------------------------
K dht_aretrieval_getstate( K aretievalHandleq )
{
    try {
        return ki( (int)((SKAsyncRetrieval *) aretievalHandleq->j)->getState() );
    } catch (...) {
        exhandler( "asyncretrieval get state failed", __FILE__, __LINE__);
    }
}

K dht_aretrieval_getfailurecause( K aretievalHandleq )
{
    try {
        return ki( (int)((SKAsyncRetrieval *) aretievalHandleq->j)->getFailureCause() );
    } catch (...) {
        exhandler( "asyncretrieval get FailureCause failed", __FILE__, __LINE__);
    }
}

K dht_aretrieval_waitforcompletion( K aretievalHandleq )
{
    try {
        SKAsyncRetrieval * pAr = (SKAsyncRetrieval *) aretievalHandleq->j;
        pAr->waitForCompletion();
        return kb( 1 );
    } catch (...) {
        exhandler( "asyncretrieval waitforcompletion failed", __FILE__, __LINE__);
    }
}

//K dht_aretrieval_waitforcompletiontm( K aretievalHandleq, K timeoutq, K timeunitq )
//{
//    try {
//        return kb( (int)((SKAsyncRetrieval *) aretievalHandleq->j)->waitForCompletion((int)KInt(timeoutq), (SKTimeUnit)((int)KInt(timeunitq))) );
//    } catch (...) {
//        exhandler( "asyncretrieval waitforcompletion failed", __FILE__, __LINE__);
//    }
//}

K dht_aretrieval_close( K aretievalHandleq )
{
    try {
        ((SKAsyncRetrieval *) aretievalHandleq->j)->close();
        return kb( 1 );
    } catch (...) {
        exhandler( "asyncretrieval close failed", __FILE__, __LINE__);
    }
}

K dht_aretrieval_getoperationstate( K aretievalHandleq, K keyq )
{
    try {
        return ki( ((SKAsyncRetrieval *) aretievalHandleq->j)->getOperationState(STDSTRING(keyq)) );
    } catch (...) {
        exhandler( "asyncretrieval get OperationState failed", __FILE__, __LINE__);
    }
}

K dht_aretrieval_getkeys( K aretievalHandleq )
{
    try {
        return internal_asynckeyedop_get_keys(static_cast<SKAsyncKeyedOperation*>((SKAsyncRetrieval *) aretievalHandleq->j), false);
    } catch (...) {
        exhandler( "asyncretrieval get keys failed", __FILE__, __LINE__);
    }
}


K dht_aretrieval_getincompletekeys( K aretievalHandleq )
{
    try {
        return internal_asynckeyedop_get_keys(static_cast<SKAsyncKeyedOperation*>((SKAsyncRetrieval *) aretievalHandleq->j), true);
    } catch (...) {
        return exhandler( "asyncretrieval get IncompleteKeys failed", __FILE__, __LINE__);
    }
}

K dht_aretrieval_getoperationstatemap( K aretievalHandleq )
{
    try {
        return internal_asynckeyedop_get_opstatemap(static_cast<SKAsyncKeyedOperation*>((SKAsyncRetrieval *) aretievalHandleq->j) );
    } catch (...) {
        return exhandler( "asyncretrieval get OperationState map failed", __FILE__, __LINE__);
    }
}

K dht_aretrieval_getstoredvalue( K aretievalHandleq, K keyq )
{
    try {
        SKAsyncRetrieval * pAsyncRet = (SKAsyncRetrieval *) ((void *) KLong(aretievalHandleq));
        std::string key = STDSTRING(keyq);
        return make_symbol( pAsyncRet->getStoredValue(key) );
    }
    catch (...) {
        return exhandler( "asyncretrieval get StoredValue failed", __FILE__, __LINE__);
    }
}

K dht_aretrieval_getmeta( K aretievalHandleq, K keyq )
{
    try {
        std::string key = STDSTRING(keyq);
        return  internal_storedvalue_todict( ((SKAsyncRetrieval *) aretievalHandleq->j)->getStoredValue(key) );
    }
    catch (...) {
        return exhandler( "asyncretrieval get StoredValue failed", __FILE__, __LINE__);
    }
}

K dht_aretrieval_getnumkeys ( K aretievalHandleq )
{
    try {
        SKAsyncRetrieval * pAsyncRet = (SKAsyncRetrieval *) ((void *) KLong(aretievalHandleq));
        return ki( (int) (pAsyncRet->getNumKeys()) );
    }
    catch (...) {
        return exhandler( "asyncretrieval get StoredValue failed", __FILE__, __LINE__);
    }
}

K dht_aretrieval_delete( K aretievalHandleq )
{
    try {
        SKAsyncRetrieval * aretrieval = (SKAsyncRetrieval *) ((void *) KLong(aretievalHandleq));
        delete aretrieval;
        aretievalHandleq->j = 0;
        return _kNULL;
    }
    catch (...) {
        exhandler("aretrieval delete failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}

K dht_aretrieval_getstoredvalues( K aretievalHandleq )
{
    try {
        SKAsyncRetrieval * pAsyncRet = (SKAsyncRetrieval *) aretievalHandleq->j;
        return internal_asyncretrieval_get_storedvalues( pAsyncRet, false );
    }
    catch (...) {
        return exhandler( "asyncRetrieval get StoredValues failed", __FILE__, __LINE__);
    }
}

K dht_aretrieval_getlateststoredvalues( K aretievalHandleq )
{
    try {
        SKAsyncRetrieval * pAsyncRet = (SKAsyncRetrieval *) aretievalHandleq->j;
        return internal_asyncretrieval_get_storedvalues( pAsyncRet, true );
    }
    catch (...) {
        return exhandler( "asyncValueRetrieval get latest StoredValues failed", __FILE__, __LINE__);
    }
}

K dht_aretrieval_getstoredvalueval( K aretievalHandleq, K keyq )
{
    try {
        return  internal_asyncretrieval_get_storedvalue( (SKAsyncRetrieval *) aretievalHandleq->j, keyq );
    }
    catch (...) {
        return exhandler( "asyncretrieval get StoredValue\'s value  failed", __FILE__, __LINE__);
    }
}

//------------------------ SKAsyncValueRetrieval ---------------------------
K dht_avalret_getstate( K avalretHandleq )
{
    try {
        return ki( (int)((SKAsyncValueRetrieval *) avalretHandleq->j)->getState() );
    } catch (...) {
        exhandler( "asyncValueRetrieval get state failed", __FILE__, __LINE__);
    }
}

K dht_avalret_getfailurecause( K avalretHandleq )
{
    try {
        return ki( (int)((SKAsyncValueRetrieval *) avalretHandleq->j)->getFailureCause() );
    } catch (...) {
        exhandler( "asyncValueRetrieval get FailureCause failed", __FILE__, __LINE__);
    }
}

K dht_avalret_waitforcompletion( K avalretHandleq )
{
    try {
        ((SKAsyncValueRetrieval *) avalretHandleq->j)->waitForCompletion();
        return kb( 1 );
    } catch (...) {
        exhandler( "asyncValueRetrieval waitforcompletion failed", __FILE__, __LINE__);
    }
}

//K dht_avalret_waitforcompletiontm( K avalretHandleq, K timeoutq, K timeunitq )
//{
//    try {
//        return kb( (int)((SKAsyncValueRetrieval *) avalretHandleq->j)->waitForCompletion((int)KInt(timeoutq), (SKTimeUnit)((int)KInt(timeunitq))) );
//    } catch (...) {
//        exhandler( "asyncValueRetrieval waitforcompletion failed", __FILE__, __LINE__);
//    }
//}

K dht_avalret_close( K avalretHandleq )
{
    try {
        ((SKAsyncValueRetrieval *) avalretHandleq->j)->close();
        return kb( 1 );
    } catch (...) {
        exhandler( "asyncValueRetrieval close failed", __FILE__, __LINE__);
    }
}

K dht_avalret_getoperationstate( K avalretHandleq, K keyq )
{
    try {
        return ki( ((SKAsyncValueRetrieval *) avalretHandleq->j)->getOperationState(STDSTRING(keyq)) );
    } catch (...) {
        exhandler( "asyncValueRetrieval get OperationState failed", __FILE__, __LINE__);
    }
}

K dht_avalret_getkeys( K avalretHandleq )
{
    try {
        return internal_asynckeyedop_get_keys(static_cast<SKAsyncKeyedOperation*>((SKAsyncValueRetrieval *) avalretHandleq->j), false);
    } catch (...) {
        exhandler( "asyncValueRetrieval get keys failed", __FILE__, __LINE__);
    }
}


K dht_avalret_getincompletekeys( K avalretHandleq )
{
    try {
        return internal_asynckeyedop_get_keys(static_cast<SKAsyncKeyedOperation*>((SKAsyncValueRetrieval *) avalretHandleq->j), true);
    } catch (...) {
        return exhandler( "asyncValueRetrieval get IncompleteKeys failed", __FILE__, __LINE__);
    }
}

K dht_avalret_getoperationstatemap( K avalretHandleq )
{
    try {
        return internal_asynckeyedop_get_opstatemap(static_cast<SKAsyncKeyedOperation*>((SKAsyncValueRetrieval *) avalretHandleq->j) );
    } catch (...) {
        return exhandler( "asyncValueRetrieval get OperationState map failed", __FILE__, __LINE__);
    }
}

K dht_avalret_getstoredvalue( K avalretHandleq, K keyq )
{
    try {
        SKAsyncValueRetrieval * pAsyncRet = (SKAsyncValueRetrieval *) avalretHandleq->j;
        string key = STDSTRING(keyq);
        return make_symbol( pAsyncRet->getStoredValue(key) );
    }
    catch (...) {
        return exhandler( "asyncValueRetrieval get StoredValue failed", __FILE__, __LINE__);
    }
}

K dht_avalret_getmeta( K avalretHandleq, K keyq )
{
    try {
        string key = STDSTRING(keyq);
        return  internal_storedvalue_todict( ((SKAsyncValueRetrieval *) avalretHandleq->j)->getStoredValue(key) );
    }
    catch (...) {
        return exhandler( "asyncValueRetrieval get StoredValue failed", __FILE__, __LINE__);
    }
}


K dht_avalret_getvalue( K avalretHandleq, K keyq )
{
    try {
        SKAsyncValueRetrieval * pAsyncRet = (SKAsyncValueRetrieval *) avalretHandleq->j;
        return internal_avalret_get_value( pAsyncRet, keyq );
    }
    catch (...) {
        return exhandler( "asyncValueRetrieval get value failed", __FILE__, __LINE__);
    }
}

K dht_avalret_getvalues( K avalretHandleq )
{
    try {
        SKAsyncValueRetrieval * pAsyncRet = (SKAsyncValueRetrieval *) avalretHandleq->j;
        return internal_avalret_get_values( pAsyncRet, false );
    }
    catch (...) {
        return exhandler( "asyncValueRetrieval get values failed", __FILE__, __LINE__);
    }
}

K dht_avalret_getlatestvalues( K avalretHandleq )
{
    try {
        SKAsyncValueRetrieval * pAsyncRet = (SKAsyncValueRetrieval *) avalretHandleq->j;
        return internal_avalret_get_values( pAsyncRet, true );
    }
    catch (...) {
        return exhandler( "asyncValueRetrieval get latest values failed", __FILE__, __LINE__);
    }
}

K dht_avalret_getnumkeys ( K avalretHandleq )
{
    try {
        SKAsyncValueRetrieval * pAsyncRet = (SKAsyncValueRetrieval *) ((void *) KLong(avalretHandleq));
        return ki( (int) (pAsyncRet->getNumKeys()) );
    }
    catch (...) {
        return exhandler( "asyncretrieval get StoredValue failed", __FILE__, __LINE__);
    }
}

K dht_avalret_delete( K avalretHandleq )
{
    try {
        SKAsyncValueRetrieval * avalret = (SKAsyncValueRetrieval *) ((void *) KLong(avalretHandleq));
        delete avalret;
        avalretHandleq->j = 0;
        return _kNULL;
    }
    catch (...) {
        exhandler("asyncValueRetrieval delete failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}

//FIXME: TODO: 
//SKMap<string,SKStoredValue * > *  getLatestStoredValues();
//SKMap<string,SKStoredValue * > *  getStoredValues();

//------------------------ SKNamespaceCreationOptions --------------------------------
K dht_nscreationopt_parse( K defstringq )
{
    try {
        if(kC(defstringq)[defstringq->n] != 0)
            kC(defstringq)[defstringq->n] = 0;
        return make_symbol( SKNamespaceCreationOptions::parse( STDSTRING(defstringq).c_str() ) );
    }
    catch (...) {
        return exhandler( "NamespaceCreationOptions parse failed", __FILE__, __LINE__);
    }
}

K dht_nscreationopt_defaultoptions( K dummy )
{
    try {
        return make_symbol( SKNamespaceCreationOptions::defaultOptions( ) );
    }
    catch (...) {
        return exhandler( "NamespaceCreationOptions get defaultOptions failed", __FILE__, __LINE__);
    }
}

K dht_nscreationopt_canbeexplicitlycreated( K nscOptsHandleq, K nsq )
{
    try {
        if(kC(nsq)[nsq->n] != 0)
            kC(nsq)[nsq->n] = 0;
        SKNamespaceCreationOptions * nscOpt = (SKNamespaceCreationOptions *) nscOptsHandleq->j;
        return kb( nscOpt->canBeExplicitlyCreated( STDSTRING(nsq).c_str() ) );
    }
    catch (...) {
        return exhandler( "NamespaceCreationOptions canbeexplicitlycreated failed", __FILE__, __LINE__);
    }
}

K dht_nscreationopt_canbeautocreated( K nscOptsHandleq, K nsq )
{
    try {
        if(kC(nsq)[nsq->n] != 0)
            kC(nsq)[nsq->n] = 0;
        SKNamespaceCreationOptions * nscOpt = (SKNamespaceCreationOptions *) nscOptsHandleq->j;
        return kb( nscOpt->canBeAutoCreated( STDSTRING(nsq).c_str()  ) );
    }
    catch (...) {
        return exhandler( "NamespaceCreationOptions canbeautocreated failed", __FILE__, __LINE__);
    }
}

K dht_nscreationopt_getdefaultnsoptions( K nscOptsHandleq, K nsq )
{
    try {
        SKNamespaceCreationOptions * nscOpt = (SKNamespaceCreationOptions *) nscOptsHandleq->j;
        return make_symbol( nscOpt->getDefaultNamespaceOptions() );
    }
    catch (...) {
        return exhandler( "NamespaceCreationOptions get defaultnsoptions failed", __FILE__, __LINE__);
    }
}

K dht_nscreationopt_new( K nscreationmodeq, K regexq, K nsOptsHandleq )
{
    try {
        if(kC(regexq)[regexq->n] != 0)
            kC(regexq)[regexq->n] = 0;
        SKNamespaceOptions * nsOpt = (SKNamespaceOptions *) nsOptsHandleq->j;
        return make_symbol( new SKNamespaceCreationOptions((NsCreationMode)((int)KInt(nscreationmodeq)), STDSTRING(regexq).c_str(), nsOpt) );
    }
    catch (...) {
        return exhandler( "NamespaceCreationOptions get defaultnsoptions failed", __FILE__, __LINE__);
    }
}

K dht_nscreationopt_delete( K nscOptsHandleq )
{
    try {
        SKNamespaceCreationOptions * nscOpts = (SKNamespaceCreationOptions *) ((void *) KLong(nscOptsHandleq));
        delete nscOpts;
        nscOptsHandleq->j = 0;
        return _kNULL;
    }
    catch (...) {
        exhandler("NamespaceCreationOptions delete failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}

//------------------------ SKRetrievalOptions --------------------------------
/*
K dht_retrivalopt_new( K retrievalTypeq, K waitModeq )
{
    try {
        SKRetrievalType rt = (SKRetrievalType) ((int) (KInt(retrievalTypeq)));
        SKWaitMode wm = (SKWaitMode) ((int) (KInt(waitModeq)));
        SKRetrievalOptions * pRetrOpt = new SKRetrievalOptions(rt, wm);
        return make_symbol( pRetrOpt );
    } catch (...) {
        return exhandler( "RetrievalOptions creation failed", __FILE__, __LINE__);
    }
}

K dht_retrivalopt_new3( K retrievalTypeq, K waitModeq, K versionConstrq )
{
    try {
        SKRetrievalType rt = (SKRetrievalType) ((int) (KInt(retrievalTypeq)));
        SKWaitMode wm = (SKWaitMode) ((int) (KInt(waitModeq)));
        SKVersionConstraint * pVersionConstraint = (SKVersionConstraint *) versionConstrq->j;
        SKRetrievalOptions * pRetrOpt = new SKRetrievalOptions(rt, wm, pVersionConstraint);
        return make_symbol( pRetrOpt );
    } catch (...) {
        return exhandler( "RetrievalOptions creation failed", __FILE__, __LINE__);
    }
}

K dht_retrivalopt_new5( K retrievalTypeq, K waitModeq, K versionConstrq, K nonExistenceResponseq, K verifyChecksumsq )
{
    try {
        SKRetrievalType rt = (SKRetrievalType) ((int) (KInt(retrievalTypeq)));
        SKWaitMode wm = (SKWaitMode) ((int) (KInt(waitModeq)));
        SKVersionConstraint * pVersionConstraint = (SKVersionConstraint *) versionConstrq->j;
        SKNonExistenceResponse::SKNonExistenceResponse ner = (SKNonExistenceResponse::SKNonExistenceResponse) ((int) (KInt(nonExistenceResponseq)));
        bool verifyChecksum = (verifyChecksumsq->t==-KI) ? 
                (bool) kI(verifyChecksumsq) : (bool) kG(verifyChecksumsq) ;
        SKRetrievalOptions * pRetrOpt = new SKRetrievalOptions(rt, wm, pVersionConstraint, ner, verifyChecksum);
        return make_symbol( pRetrOpt );
    } catch (...) {
        return exhandler( "RetrievalOptions creation failed", __FILE__, __LINE__);
    }
}
*/

//methods
K dht_retrivalopt_getnonexistenceresponse( K retroptHandleq )
{
    try {
        SKRetrievalOptions * pRetrOpts = (SKRetrievalOptions *) retroptHandleq->j;
        return ki( (int)(pRetrOpts->getNonExistenceResponse()) );
    } catch (...) {
        return exhandler( "RetrievalOptions get nonexistenceresponse failed", __FILE__, __LINE__);
    }
}

K dht_retrivalopt_getretrievaltype( K retroptHandleq )
{
    try {
        SKRetrievalOptions * pRetrOpts = (SKRetrievalOptions *) retroptHandleq->j;
        return ki( (int)(pRetrOpts->getRetrievalType()) );
    } catch (...) {
        return exhandler( "RetrievalOptions get retrievaltype failed", __FILE__, __LINE__);
    }
}

K dht_retrivalopt_getwaitmode( K retroptHandleq )
{
    try {
        SKRetrievalOptions * pRetrOpts = (SKRetrievalOptions *) retroptHandleq->j;
        return ki( (int)(pRetrOpts->getWaitMode()) );
    } catch (...) {
        return exhandler( "RetrievalOptions get waitmode failed", __FILE__, __LINE__);
    }
}

K dht_retrivalopt_getversionconstraint( K retroptHandleq )
{
    try {
        SKRetrievalOptions * pRetrOpts = (SKRetrievalOptions *) retroptHandleq->j;
        return make_symbol( pRetrOpts->getVersionConstraint() );
    } catch (...) {
        return exhandler( "RetrievalOptions get VersionConstraint failed", __FILE__, __LINE__);
    }
}

K dht_retrivalopt_getverifychecksums( K retroptHandleq )
{
    try {
        SKRetrievalOptions * pRetrOpts = (SKRetrievalOptions *) retroptHandleq->j;
        return kb( pRetrOpts->getVerifyChecksums() );
    } catch (...) {
        return exhandler( "RetrievalOptions get VerifyChecksums failed", __FILE__, __LINE__);
    }
}

K dht_retrivalopt_retrievaltype( K retroptHandleq, K retrievalTypeq )
{
    try {
        SKRetrievalOptions * pRetrOpt = (SKRetrievalOptions *) retroptHandleq->j;
        SKRetrievalType rt = (SKRetrievalType) ((int) (KInt(retrievalTypeq)));
        return make_symbol( pRetrOpt->retrievalType(rt) );
    }
    catch (...) {
        return exhandler( "RetrievalOptions retrievaltype failed", __FILE__, __LINE__);
    }
}

K dht_retrivalopt_waitmode( K retroptHandleq, K waitModeq )
{
    try {
        SKRetrievalOptions * pRetrOpt = (SKRetrievalOptions *) retroptHandleq->j;
        SKWaitMode wm = (SKWaitMode) ((int) (KInt(waitModeq)));
        return make_symbol( pRetrOpt->waitMode(wm) );
    }
    catch (...) {
        return exhandler( "RetrievalOptions waitMode failed", __FILE__, __LINE__);
    }
}

K dht_retrivalopt_nonexistenceresponse( K retroptHandleq, K nonExistenceResponseq )
{
    try {
        SKRetrievalOptions * pRetrOpt = (SKRetrievalOptions *) retroptHandleq->j;
        SKNonExistenceResponse::SKNonExistenceResponse ner = (SKNonExistenceResponse::SKNonExistenceResponse) ((int) (KInt(nonExistenceResponseq)));
        return make_symbol( pRetrOpt->nonExistenceResponse(ner) );
    }
    catch (...) {
        return exhandler( "RetrievalOptions nonExistenceResponse failed", __FILE__, __LINE__);
    }
}

K dht_retrivalopt_versionconstraint( K retroptHandleq, K versionConstrq )
{
    try {
        SKRetrievalOptions * pRetrOpt = (SKRetrievalOptions *) retroptHandleq->j;
        SKVersionConstraint * pVersionConstraint = (SKVersionConstraint *) versionConstrq->j;
        return make_symbol( pRetrOpt->versionConstraint(pVersionConstraint) );
    }
    catch (...) {
        return exhandler( "RetrievalOptions versionConstraint failed", __FILE__, __LINE__);
    }
}

K dht_retrivalopt_getforwardingmode ( K retroptHandleq )
{
    try {
        SKRetrievalOptions * pRetrOpt = (SKRetrievalOptions *) ((void *) KLong(retroptHandleq));
        return ki( (int)(pRetrOpt->getForwardingMode()) );
    } catch (...) {
        return exhandler( "RetrievalOptions get ForwardingMode failed", __FILE__, __LINE__);
    }
}

K dht_retrivalopt_forwardingmode ( K retroptHandleq, K forwardingModeq )
{
    try {
        SKRetrievalOptions * pRetrOpt = (SKRetrievalOptions *) ((void *) KLong(retroptHandleq));
        SKForwardingMode fm = (SKForwardingMode) ((int) (KInt(forwardingModeq)));
        return make_symbol( pRetrOpt->forwardingMode(fm) );
    }
    catch (...) {
        return exhandler( "RetrievalOptions forwardingMode failed", __FILE__, __LINE__);
    }
}

K dht_retrivalopt_delete( K retroptHandleq )
{
    try {
        SKRetrievalOptions * retrivalopt = (SKRetrievalOptions *) ((void *) KLong(retroptHandleq));
        delete retrivalopt;
        retroptHandleq->j = 0;
        return _kNULL;
    }
    catch (...) {
        exhandler("RetrievalOptions delete failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}

//------------------------ SKGetOptions ----------------------------------------
/*
K dht_getopts_new( K retrievalTypeq )
{
    try {
        SKRetrievalType rt = (SKRetrievalType) ((int) (KInt(retrievalTypeq)));
        SKGetOptions * pGetOpts = new SKGetOptions(rt);
        return make_symbol( pGetOpts );
    } catch (...) {
        return exhandler( "GetOptions creation failed", __FILE__, __LINE__);
    }
}

K dht_getopts_new2( K retrievalTypeq, K versionConstrq )
{
    try {
        SKRetrievalType rt = (SKRetrievalType) ((int) (KInt(retrievalTypeq)));
        SKVersionConstraint * pVersionConstraint = (SKVersionConstraint *) versionConstrq->j;
        SKGetOptions * pGetOpts = new SKGetOptions(rt, pVersionConstraint);
        return make_symbol( pGetOpts );
    } catch (...) {
        return exhandler( "GetOptions creation failed", __FILE__, __LINE__);
    }
}

K dht_getopts_new3 ( K opTimeoutCtrlq, K retrievalTypeq, K versionConstrq )
{
    try {
        SKOpTimeoutController * pOpCtrl = (SKOpTimeoutController *) ((void*) KLong(opTimeoutCtrlq) );
        SKRetrievalType rt = (SKRetrievalType) ((int) (Kinteger(retrievalTypeq)));
        SKVersionConstraint * pVersionConstraint = (SKVersionConstraint *) ((void*) KLong(versionConstrq));
        SKGetOptions * pGetOpt = new SKGetOptions(pOpCtrl, rt, pVersionConstraint);
        return make_symbol( pGetOpt );
    } catch (...) {
        return exhandler( "GetOptions creation failed", __FILE__, __LINE__);
    }
}

K dht_getopts_new7 ( K opTimeoutCtrlq, K retrievalTypeq, K versionConstrq, K nonExistenceRespq, K verifyChkSumq,
 K updateSecTgtOnMissq, K secondaryTgtsq )
{
    try {
        SKOpTimeoutController * pOpCtrl = (SKOpTimeoutController *) ((void*) KLong(opTimeoutCtrlq) );
        SKRetrievalType rt = (SKRetrievalType) ((int) (Kinteger(retrievalTypeq)));
        SKVersionConstraint * pVersionConstraint = (SKVersionConstraint *) ((void*) KLong(versionConstrq));
        SKNonExistenceResponse::SKNonExistenceResponse nonExistResp = 
            (SKNonExistenceResponse::SKNonExistenceResponse) ((int) (Kinteger(nonExistenceRespq)));
        bool verifyChkSum = (verifyChkSumq->t==-KI) ? (bool) kI(verifyChkSumq) : 
            (verifyChkSumq->t==-KJ) ? (bool) kJ(verifyChkSumq) : (bool) kG(verifyChkSumq) ;            
        bool updateSecondariesOnMiss = (updateSecTgtOnMissq->t==-KI) ? (bool) kI(updateSecTgtOnMissq) : 
            (updateSecTgtOnMissq->t==-KJ) ? (bool) kJ(updateSecTgtOnMissq) : (bool) kG(updateSecTgtOnMissq) ;            
        std::set<SKSecondaryTarget*> * pSecondaryTgts = (std::set<SKSecondaryTarget*> *) ((void*) KLong(secondaryTgtsq) );

        SKGetOptions * pGetOpt = new SKGetOptions(pOpCtrl, rt, pVersionConstraint, nonExistResp, verifyChkSum,
            updateSecondariesOnMiss, pSecondaryTgts);
        return make_symbol( pGetOpt );
    } catch (...) {
        return exhandler( "GetOptions creation failed", __FILE__, __LINE__);
    }
}
*/

K dht_getopts_getnonexistenceresponse( K getoptHandleq )
{
    try {
        SKGetOptions * pGetOpts = (SKGetOptions *) getoptHandleq->j;
        return ki( (int)(pGetOpts->getNonExistenceResponse()) );
    } catch (...) {
        return exhandler( "GetOptions get nonexistenceresponse failed", __FILE__, __LINE__);
    }
}

K dht_getopts_getretrievaltype( K getoptHandleq )
{
    try {
        SKGetOptions * pGetOpts = (SKGetOptions *) getoptHandleq->j;
        return ki( (int)(pGetOpts->getRetrievalType()) );
    } catch (...) {
        return exhandler( "GetOptions get retrievaltype failed", __FILE__, __LINE__);
    }
}

K dht_getopts_getwaitmode( K getoptHandleq )
{
    try {
        SKGetOptions * pGetOpts = (SKGetOptions *) getoptHandleq->j;
        return ki( (int)(pGetOpts->getWaitMode()) );
    } catch (...) {
        return exhandler( "GetOptions get waitmode failed", __FILE__, __LINE__);
    }
}

K dht_getopts_getversionconstraint( K getoptHandleq )
{
    try {
        SKGetOptions * pGetOpts = (SKGetOptions *) getoptHandleq->j;
        return make_symbol( pGetOpts->getVersionConstraint() );
    } catch (...) {
        return exhandler( "GetOptions get VersionConstraint failed", __FILE__, __LINE__);
    }
}

K dht_getopts_getverifychecksums( K getoptHandleq )
{
    try {
        SKGetOptions * pGetOpts = (SKGetOptions *) getoptHandleq->j;
        return kb( pGetOpts->getVerifyChecksums() );
    } catch (...) {
        return exhandler( "GetOptions get VerifyChecksums failed", __FILE__, __LINE__);
    }
}

K dht_getopts_retrievaltype( K getOptq, K retrievalTypeq )
{
    try {
        SKGetOptions * pGetOpt = (SKGetOptions *) getOptq->j;
        SKRetrievalType rt = (SKRetrievalType) ((int) (KInt(retrievalTypeq)));
        return make_symbol( pGetOpt->retrievalType(rt) );
    }
    catch (...) {
        return exhandler( "GetOptions retrievalType failed", __FILE__, __LINE__);
    }
}

K dht_getopts_versionconstraint( K getOptq, K versionConstraintq )
{
    try {
        SKGetOptions * pGetOpt = (SKGetOptions *) getOptq->j;
        SKVersionConstraint * pVc = (SKVersionConstraint *) versionConstraintq->j;
        return make_symbol( pGetOpt->versionConstraint(pVc) );
    }
    catch (...) {
        return exhandler( "GetOptions versionConstraint failed", __FILE__, __LINE__);
    }
}

K dht_getopts_parse ( K defq )
{
    try {
        if(kC(defq)[defq->n] != 0)
            kC(defq)[defq->n] = 0;
        return make_symbol( SKGetOptions::parse( STDSTRING(defq).c_str() ) );
    }
    catch (...) {
        return exhandler( "GetOptions parse failed", __FILE__, __LINE__);
    }
}

K dht_getopts_tostring ( K getoptHandleq )
{
    try {
        SKGetOptions * pGetopt = (SKGetOptions *) ((void *) KLong(getoptHandleq));
        std::string str = pGetopt->toString() ;
        K repr = kpn( const_cast<char*>(str.c_str()), str.size() );
        return repr;
    } 
    catch (...) {
        return exhandler( "GetOptions tostring failed", __FILE__, __LINE__);
    }
}

K dht_getopts_getforwardingmode ( K getoptHandleq )
{
    try {
        SKGetOptions * pGetOpt = (SKGetOptions *) ((void *) KLong(getoptHandleq));
        return ki( (int)(pGetOpt->getForwardingMode()) );
    } catch (...) {
        return exhandler( "GetOptions get ForwardingMode failed", __FILE__, __LINE__);
    }
}

K dht_getopts_forwardingmode ( K getoptHandleq, K forwardingModeq )
{
    try {
        SKGetOptions * pGetOpt = (SKGetOptions *) ((void *) KLong(getoptHandleq));
        SKForwardingMode fm = (SKForwardingMode) ((int) (KInt(forwardingModeq)));
        return make_symbol( pGetOpt->forwardingMode(fm) );
    }
    catch (...) {
        return exhandler( "GetOptions forwardingMode failed", __FILE__, __LINE__);
    }
}

K dht_getopts_delete( K getoptHandleq )
{
    try {
        SKGetOptions * getopt = (SKGetOptions *) ((void *) KLong(getoptHandleq));
        delete getopt;
        getoptHandleq->j = 0;
        return _kNULL;
    }
    catch (...) {
        exhandler("GetOptions delete failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}

//FIXME: TODO: not overwritten in Java/c++??
//SKRetrievalOptions * retrievalType(SKRetrievalType retrievalType);
//SKRetrievalOptions * waitMode(SKWaitMode waitMode);
//SKRetrievalOptions * versionConstraint(SKVersionConstraint * versionConstraint);
//SKRetrievalOptions * nonExistenceResponse(SKNonExistenceResponse::SKNonExistenceResponse nonExistenceResponse);



//------------------------ SKWaitOptions ---------------------------------------
/*
K dht_waitopts_new( K dummy )
{
    try {
        return make_symbol( new SKWaitOptions() );
    } catch (...) {
        return exhandler( "WaitOptions creation failed", __FILE__, __LINE__);
    }
}

K dht_waitopts_new1( K retrievalTypeq )
{
    try {
        SKRetrievalType rt = (SKRetrievalType) ((int) (Kinteger(retrievalTypeq)));
        return make_symbol( new SKWaitOptions(rt) );
    } catch (...) {
        return exhandler( "WaitOptions creation failed", __FILE__, __LINE__);
    }
}

K dht_waitopts_new2( K retrievalTypeq, K versionConstrq )
{
    try {
        SKRetrievalType rt = (SKRetrievalType) ((int) (Kinteger(retrievalTypeq)));
        SKVersionConstraint * pVersionConstraint = (SKVersionConstraint *) versionConstrq->j;
        SKWaitOptions * pWaitOpts = new SKWaitOptions(rt, pVersionConstraint);
        return make_symbol( pWaitOpts );
    } catch (...) {
        return exhandler( "WaitOptions creation failed", __FILE__, __LINE__);
    }
}

K dht_waitopts_new3( K retrievalTypeq, K versionConstrq, K timeoutSecq )
{
    try {
        SKRetrievalType rt = (SKRetrievalType) ((int) (Kinteger(retrievalTypeq)));
        int timeoutSeconds  = (int) (Kinteger(timeoutSecq));
        SKVersionConstraint * pVersionConstraint = (SKVersionConstraint *) versionConstrq->j;
        SKWaitOptions * pWaitOpts = new SKWaitOptions(rt, pVersionConstraint, timeoutSeconds);
        return make_symbol( pWaitOpts );
    } catch (...) {
        return exhandler( "WaitOptions creation failed", __FILE__, __LINE__);
    }
}

K dht_waitopts_new4( K retrievalTypeq, K versionConstrq, K timeoutSecq, K thresholdq )
{
    try {
        SKRetrievalType rt = (SKRetrievalType) ((int) (Kinteger(retrievalTypeq)));
        int timeoutSeconds  = (int) (Kinteger(timeoutSecq));
        int threshold       = (int) (Kinteger(thresholdq));
        SKVersionConstraint * pVersionConstraint = (SKVersionConstraint *) versionConstrq->j;
        SKWaitOptions * pWaitOpts = new SKWaitOptions(rt, pVersionConstraint, timeoutSeconds, threshold);
        return make_symbol( pWaitOpts );
    } catch (...) {
        return exhandler( "WaitOptions creation failed", __FILE__, __LINE__);
    }
}

K dht_waitopts_new5( K retrievalTypeq, K versionConstrq, K timeoutSecq, K thresholdq, K timeoutRespq )
{
    try {
        SKRetrievalType rt = (SKRetrievalType) ((int) (Kinteger(retrievalTypeq)));
        int timeoutSeconds  = (int) (Kinteger(timeoutSecq));
        int threshold       = (int) (Kinteger(thresholdq));
        SKTimeoutResponse::SKTimeoutResponse timeoutResponse = (SKTimeoutResponse::SKTimeoutResponse) ((int) (Kinteger(timeoutRespq)));
        SKVersionConstraint * pVersionConstraint = (SKVersionConstraint *) versionConstrq->j;
        SKWaitOptions * pWaitOpts = new SKWaitOptions(rt, pVersionConstraint, timeoutSeconds, threshold, timeoutResponse);
        return make_symbol( pWaitOpts );
    } catch (...) {
        return exhandler( "WaitOptions creation failed", __FILE__, __LINE__);
    }
}
*/

K dht_waitopts_parse ( K optDefStringq ) 
{
    try {
        return make_symbol( SKWaitOptions::parse((char*)(KBytes(optDefStringq)) ) );
    } catch (...) {
        return exhandler( "WaitOptions parse failed", __FILE__, __LINE__);
    }
}

K dht_waitopts_retrievaltype( K waitoptq,  K retrievalTypeq )
{
    try {
        SKRetrievalType rt = (SKRetrievalType) ((int) (Kinteger(retrievalTypeq)));
        SKWaitOptions * pWaitOpt = (SKWaitOptions *) waitoptq->j;
        return make_symbol( pWaitOpt->retrievalType(rt) );
    } catch (...) {
        return exhandler( "WaitOptions retrievalType failed", __FILE__, __LINE__);
    }
}

K dht_waitopts_versionconstraint( K waitoptq,  K versionConstraintq )
{
    try {
        SKWaitOptions * pWaitOpt = (SKWaitOptions *) waitoptq->j;
        SKVersionConstraint * pverConstr = (SKVersionConstraint *) versionConstraintq->j;
        return make_symbol( pWaitOpt->versionConstraint(pverConstr) );
    } catch (...) {
        return exhandler( "WaitOptions versionConstraint failed", __FILE__, __LINE__);
    }
}

K dht_waitopts_timeoutseconds( K waitoptq,  K timeoutSecq )
{
    try {
        int timeoutSecs = (int) (Kinteger(timeoutSecq));
        SKWaitOptions * pWaitOpt = (SKWaitOptions *) waitoptq->j;
        return make_symbol( pWaitOpt->timeoutSeconds(timeoutSecs) );
    } catch (...) {
        return exhandler( "WaitOptions timeoutSeconds failed", __FILE__, __LINE__);
    }
}

K dht_waitopts_threshold( K waitoptq,  K thresholdq )
{
    try {
        int threshold = (int) (Kinteger(thresholdq));
        SKWaitOptions * pWaitOpt = (SKWaitOptions *) waitoptq->j;
        return make_symbol( pWaitOpt->threshold(threshold) );
    } catch (...) {
        return exhandler( "WaitOptions threshold failed", __FILE__, __LINE__);
    }
}

K dht_waitopts_timeoutresponse( K waitoptq,  K timeoutResponseq )
{
    try {
        SKTimeoutResponse::SKTimeoutResponse tr = (SKTimeoutResponse::SKTimeoutResponse) ((int) (Kinteger(timeoutResponseq)));
        SKWaitOptions * pWaitOpt = (SKWaitOptions *) waitoptq->j;
        return make_symbol( pWaitOpt->timeoutResponse(tr) );
    } catch (...) {
        return exhandler( "WaitOptions timeoutResponse failed", __FILE__, __LINE__);
    }
}

K dht_waitopts_gettimeoutresponse( K waitoptq )
{
    try {
        SKWaitOptions * pWaitOpt = (SKWaitOptions *) waitoptq->j;
        return ki( (int)(pWaitOpt->getTimeoutResponse()) );
    } catch (...) {
        return exhandler( "WaitOptions get timeoutresponse failed", __FILE__, __LINE__);
    }
}

K dht_waitopts_nonexistenceresponse( K waitoptq,  K nonexistResponseq )
{
    try {
        SKNonExistenceResponse::SKNonExistenceResponse ner = (SKNonExistenceResponse::SKNonExistenceResponse) ((int) (KInt(nonexistResponseq)));
        SKWaitOptions * pWaitOpt = (SKWaitOptions *) waitoptq->j;
        return make_symbol( pWaitOpt->nonExistenceResponse(ner) );
    } catch (...) {
        return exhandler( "WaitOptions timeoutResponse failed", __FILE__, __LINE__);
    }
}

K dht_waitopts_getnonexistenceresponse( K waitoptq )
{
    try {
        SKWaitOptions * pWaitOpt = (SKWaitOptions *) waitoptq->j;
        return ki( (int)(pWaitOpt->getNonExistenceResponse()) );
    } catch (...) {
        return exhandler( "WaitOptions get nonexistenceresponse failed", __FILE__, __LINE__);
    }
}

K dht_waitopts_getretrievaltype( K waitoptq )
{
    try {
        SKWaitOptions * pWaitOpt = (SKWaitOptions *) waitoptq->j;
        return ki( (int)(pWaitOpt->getRetrievalType()) );
    } catch (...) {
        return exhandler( "WaitOptions get retrievaltype failed", __FILE__, __LINE__);
    }
}

K dht_waitopts_getwaitmode( K waitoptq )
{
    try {
        SKWaitOptions * pWaitOpt = (SKWaitOptions *) waitoptq->j;
        return ki( (int)(pWaitOpt->getWaitMode()) );
    } catch (...) {
        return exhandler( "WaitOptions get waitmode failed", __FILE__, __LINE__);
    }
}

K dht_waitopts_getversionconstraint( K waitoptq )
{
    try {
        SKWaitOptions * pWaitOpt = (SKWaitOptions *) waitoptq->j;
        return make_symbol( pWaitOpt->getVersionConstraint() );
    } catch (...) {
        return exhandler( "WaitOptions get VersionConstraint failed", __FILE__, __LINE__);
    }
}

K dht_waitopts_getverifychecksums( K waitoptq )
{
    try {
        SKWaitOptions * pWaitOpt = (SKWaitOptions *) waitoptq->j;
        return kb( pWaitOpt->getVerifyChecksums() );
    } catch (...) {
        return exhandler( "WaitOptions get VerifyChecksums failed", __FILE__, __LINE__);
    }
}

K dht_waitopts_getforwardingmode ( K waitoptq )
{
    try {
        SKWaitOptions * pWaitOpt = (SKWaitOptions *) ((void *) KLong(waitoptq));
        return ki( (int)(pWaitOpt->getForwardingMode()) );
    } catch (...) {
        return exhandler( "WaitOptions get ForwardingMode failed", __FILE__, __LINE__);
    }
}

K dht_waitopts_forwardingmode ( K waitoptq, K forwardingModeq )
{
    try {
        SKWaitOptions * pWaitOpt = (SKWaitOptions *) ((void *) KLong(waitoptq));
        SKForwardingMode fm = (SKForwardingMode) ((int) (KInt(forwardingModeq)));
        return make_symbol( pWaitOpt->forwardingMode(fm) );
    }
    catch (...) {
        return exhandler( "WaitOptions forwardingMode failed", __FILE__, __LINE__);
    }
}


K dht_waitopts_delete( K waitoptq )
{
    try {
        SKWaitOptions * pWaitOpt = (SKWaitOptions *) ((void *) KLong(waitoptq));
        delete pWaitOpt;
        waitoptq->j = 0;
        return _kNULL;
    }
    catch (...) {
        exhandler("WaitOptions delete failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}

//FIXME: TODO: ? not overwritten in Java
//SKRetrievalOptions * retrievalType(SKRetrievalType retrievalType);
//SKRetrievalOptions * waitMode(SKWaitMode waitMode);
//SKRetrievalOptions * versionConstraint(SKVersionConstraint * versionConstraint);
//SKRetrievalOptions * nonExistenceResponse(SKNonExistenceResponse::SKNonExistenceResponse nonExistenceResponse);

//------------------------ SKPutOptions ----------------------------------------
void internal_putoptions_gettargets( K secondaryTargetsq, std::set<SKSecondaryTarget*> & targets)
{
    if (secondaryTargetsq->t == 0 || secondaryTargetsq->t == KJ)
    {
        for (int ii = 0; ii < secondaryTargetsq->n; ++ii)
        {
            K dkey = kK(secondaryTargetsq)[ii];
            if (dkey->t == -KJ )
            {
                void * pTmp = (void*) KLong(dkey);
                if(pTmp != NULL ){
                    SKSecondaryTarget* pTgt = (SKSecondaryTarget*) pTmp;
                    // we don't create objects and won't delete them
                    targets.insert(pTgt);
                }
            }
        }
    }
    else{
        krr("internal_putoptions_gettargets type error");
    }
}

/*
K dht_putopts_new (  K opTimeoutControllerq, K compressionq, K checksumtypeq, K checksumcomprvalsq, K versionq, K secondaryTargetsq, K userdataq )
{
    try {
        SKOpTimeoutController * pOtController = (SKOpTimeoutController *) ((void *) KLong(opTimeoutControllerq));
        SKCompression::SKCompression compr = (SKCompression::SKCompression) ((int) (Kinteger(compressionq)));
        SKChecksumType::SKChecksumType chksm = (SKChecksumType::SKChecksumType) ((int) (Kinteger(checksumtypeq)));
        bool chksmComprVals = (checksumcomprvalsq->t==-KI) ? (bool) kI(checksumcomprvalsq) : 
            (checksumcomprvalsq->t==-KJ) ? (bool) kJ(checksumcomprvalsq) : (bool) kG(checksumcomprvalsq) ;
        int64_t version  = ((int64_t) (Kinteger(versionq)));
        
        std::set<SKSecondaryTarget*> * targets = NULL;
        if(secondaryTargetsq->n > 0) {
            targets = new std::set<SKSecondaryTarget*>();
            internal_putoptions_gettargets( secondaryTargetsq, *targets);
        }

        SKVal * pVal = sk_create_val();
        KBytes val(userdataq);
        sk_set_val(pVal, val.size(), (char*)val ); //copy data
        SKPutOptions * pPutOpts = new SKPutOptions(pOtController, compr, chksm, chksmComprVals, version, targets, pVal);
        
        //do a cleanup
        if(targets) {
            std::set<SKSecondaryTarget*>::iterator it;
            for (it = targets->begin(); it != targets->end(); ++it)
            {
                SKSecondaryTarget * pSt = *it;
                delete pSt;
            }
            delete targets;  // this will delete just only pointers from the set , not the objects. 
        }
        sk_destroy_val(&pVal);
        
        return make_symbol( pPutOpts );
    } catch (...) {
        return exhandler( "PutOptions creation failed", __FILE__, __LINE__);
    }
}
*/

K dht_putopts_parse( K optDefStringq )
{
    try {
        return make_symbol( SKPutOptions::parse((char*)(KBytes(optDefStringq)) ) );
    } catch (...) {
        return exhandler( "PutOptions parse failed", __FILE__, __LINE__);
    }
}

K dht_putopts_compression( K putoptq, K compressionq )
{
    try {
        SKCompression::SKCompression compr = (SKCompression::SKCompression) ((int) (KInt(compressionq)));
        SKPutOptions * pPutOpt = (SKPutOptions *) putoptq->j;
        return make_symbol( pPutOpt->compression(compr) );
    } catch (...) {
        return exhandler( "PutOptions retrievalType failed", __FILE__, __LINE__);
    }
}

K dht_putopts_version( K putoptq, K versionq )
{
    try {
        int64_t ver = (int64_t)(kJ(versionq));
        SKPutOptions * pPutOpt = (SKPutOptions *) putoptq->j;
        return make_symbol( pPutOpt->version(ver) );
    } catch (...) {
        return exhandler( "PutOptions version failed", __FILE__, __LINE__);
    }
}

K dht_putopts_userdata( K putoptq, K userdataq )
{
    try {
        SKVal * pVal = sk_create_val();
        KBytes val(userdataq);                      //any scalar K
        sk_set_val(pVal, val.size(), (char*)val ); //copy data
        SKPutOptions * pPutOpt = (SKPutOptions *) putoptq->j;
        K rv = make_symbol( pPutOpt->userData(pVal) );
        sk_destroy_val(&pVal);
        return rv;
    } catch (...) {
        return exhandler( "PutOptions userData failed", __FILE__, __LINE__);
    }
}

K dht_putopts_checksumtype( K putoptq , K checksumTypeq)
{
    try {
        SKChecksumType::SKChecksumType chksumType = (SKChecksumType::SKChecksumType) ((int) (KInt(checksumTypeq)));
        SKPutOptions * putOpt = (SKPutOptions *) putoptq->j;
        return make_symbol( putOpt->checksumType(chksumType) );
    } catch (...) {
        return exhandler( "PutOptions checksumType failed", __FILE__, __LINE__);
    }
}


K dht_putopts_optimeoutcontroller ( K putoptq, K opTimeoutControllerq )
{
    try {
        SKOpTimeoutController * pOtController = (SKOpTimeoutController*) ((void *) (KLong(opTimeoutControllerq)));
        SKPutOptions * putOpt = (SKPutOptions *) putoptq->j;
        return make_symbol( putOpt->opTimeoutController(pOtController) );
    } catch (...) {
        return exhandler( "PutOptions opTimeoutController failed", __FILE__, __LINE__);
    }
}

K dht_putopts_secondarytargets(K putoptq, K secondaryTargetListq)
{
    try {
        std::set<SKSecondaryTarget*> * targets = new std::set<SKSecondaryTarget*>();
        internal_putoptions_gettargets( secondaryTargetListq, *targets);
    
        SKPutOptions * putOpt = (SKPutOptions *) putoptq->j;
        K res = make_symbol( putOpt->secondaryTargets(targets) );

        //do a cleanup
        std::set<SKSecondaryTarget*>::iterator it;
        for (it = targets->begin(); it != targets->end(); ++it)
        {
            SKSecondaryTarget * pSt = *it;
            delete pSt;
        }
        delete targets;

        return res;
    } catch (...) {
        return exhandler( "PutOptions secondaryTargets failed", __FILE__, __LINE__);
    }
}

K dht_putopts_secondarytarget(K putoptq, K secondaryTargetq)
{
    try {
        SKSecondaryTarget * pSecondaryTarget = (SKSecondaryTarget*) ((void *) (KLong(secondaryTargetq)));
        SKPutOptions * putOpt = (SKPutOptions *) putoptq->j;
        return make_symbol( putOpt->secondaryTargets(pSecondaryTarget) );
    } catch (...) {
        return exhandler( "PutOptions secondaryTarget failed", __FILE__, __LINE__);
    }
}

K dht_putopts_getsecondarytargets(K putoptq)
{
    try{
        SKPutOptions * putOpt = (SKPutOptions *) putoptq->j;
        std::set<SKSecondaryTarget*> * targets = putOpt->getSecondaryTargets();
        
        if(!targets || !targets->size()) return _kNULL;
        
        size_t len = targets->size();
        K resultlist = ktn(KJ, len);  // or // = ktn(0, len);
        int ii = 0;
        std::set<SKSecondaryTarget*>::iterator it;
        for (it = targets->begin(); it != targets->end(); ++it)
        {
            SKSecondaryTarget * pSt = *it;
            K tgtq = make_symbol( pSt) ; 
            kK(resultlist)[ii] = tgtq ; 
            ii++;
        }
        delete targets; // just delete set, don't delete objects
        return make_symbol(resultlist);
        
    } catch (...) {
        return exhandler( "PutOptions get SecondaryTargets failed", __FILE__, __LINE__);
    }
}


/*
K dht_putopt_checksumcompressedvalues( K putoptHandleq , K chksumComprValsq)
{
    try {
        bool chksumComprVals = (chksumComprValsq->t==-KI) ? 
                (bool) kI(chksumComprValsq) : (bool) kG(chksumComprValsq) ;
        SKPutOptions * putOpt = (SKPutOptions *) putoptHandleq->j;
        return make_symbol( putOpt->checksumCompressedValues(chksumComprVals) );
    } catch (...) {
        return exhandler( "PutOptions checksumCompressedValues failed", __FILE__, __LINE__);
    }
}
*/

K dht_putopts_getchecksumcompressedvalues( K putoptq )
{
    try {
        SKPutOptions * pPutOpt = (SKPutOptions *) putoptq->j;
        return kb( (bool)(pPutOpt->getChecksumCompressedValues()) );
    } catch (...) {
        return exhandler( "PutOptions get checksumcompressedvalues failed", __FILE__, __LINE__);
    }
}

K dht_putopts_getcompression( K putoptq )
{
    try {
        SKPutOptions * pPutOpt = (SKPutOptions *) putoptq->j;
        return ki( (int)(pPutOpt->getCompression()) );
    } catch (...) {
        return exhandler( "PutOptions get Compression failed", __FILE__, __LINE__);
    }
}

K dht_putopts_getchecksumtype( K putoptq )
{
    try {
        SKPutOptions * pPutOpt = (SKPutOptions *) putoptq->j;
        return ki( (int)(pPutOpt->getChecksumType()) );
    } catch (...) {
        return exhandler( "PutOptions get ChecksumType failed", __FILE__, __LINE__);
    }
}

K dht_putopts_getversion( K putoptq )
{
    try {
        SKPutOptions * pPutOpt = (SKPutOptions *) putoptq->j;
        return kj( (int64_t)(pPutOpt->getVersion()) );
    } catch (...) {
        return exhandler( "PutOptions get Version failed", __FILE__, __LINE__);
    }
}

K dht_putopts_getuserdata( K putoptq )
{
    try {
        SKPutOptions * pPutOpt = (SKPutOptions *) putoptq->j;
        if(!pPutOpt) krr(",ERROR: no PutOptions from Q client"); //error
        return internal_putopts_get_userdata(pPutOpt);
    } catch (...) {
        return exhandler( "PutOptions get UserData failed", __FILE__, __LINE__);
    }
}

K dht_putopts_delete( K putoptq )
{
    try {
        SKPutOptions * pPutOpt = (SKPutOptions *) ((void *) KLong(putoptq));
        delete pPutOpt;
        putoptq->j = 0;
        return _kNULL;
    }
    catch (...) {
        exhandler("PutOptions delete failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}

//------------------------ SKSessionOptions ------------------------------------
K dht_sessopts_new( K dhtConfigq )
{
    try {
        SKClientDHTConfiguration * pDhtConf = (SKClientDHTConfiguration *) dhtConfigq->j;
        SKSessionOptions * sessOpts = new SKSessionOptions( pDhtConf );
        return make_symbol( sessOpts );
    } catch (...) {
        return exhandler( "SessionOptions creation failed", __FILE__, __LINE__);
    }
}

K dht_sessopts_new2( K dhtConfigq, K preferredServerq )
{
    try {
        SKClientDHTConfiguration * pDhtConf = (SKClientDHTConfiguration *) dhtConfigq->j;
        KBytes host(preferredServerq);
        SKSessionOptions * sessOpts = new SKSessionOptions(pDhtConf, (char*)host);
        return make_symbol( sessOpts );
    } catch (...) {
        return exhandler( "SessionOptions creation failed", __FILE__, __LINE__);
    }
}

K dht_sessopts_new3 (  K dhtConfigq, K preferredServerq, K sessTmoutCtrq )
{
    try {
        SKClientDHTConfiguration * pDhtConf = (SKClientDHTConfiguration *) dhtConfigq->j;
        SKSessionEstablishmentTimeoutController * pSessTmoutCtr = (SKSessionEstablishmentTimeoutController *) sessTmoutCtrq->j;
        KBytes host(preferredServerq);
        SKSessionOptions * sessOpts = new SKSessionOptions(pDhtConf, (char*)host, pSessTmoutCtr );
        return make_symbol( sessOpts );
    } catch (...) {
        return exhandler( "SessionOptions creation failed", __FILE__, __LINE__);
    }
}

K dht_sessopts_getdefaulttimeoutcontroller (  K dummyq )
{
    try {
        SKSessionEstablishmentTimeoutController * pController = SKSessionOptions::getDefaultTimeoutController();
        return make_symbol(pController);
    } catch (...) {
        return exhandler( "SessionOptions get DefaultTimeoutController failed", __FILE__, __LINE__);
    }
}
// static SKSessionEstablishmentTimeoutController * getDefaultTimeoutController();

K dht_sessopts_setdefaulttimeoutcontroller (  K sessionoptq, K sessTmoutCtrq )
{
    try {
        SKSessionOptions * sessOpts = (SKSessionOptions *) sessionoptq->j;
        SKSessionEstablishmentTimeoutController * pController = (SKSessionEstablishmentTimeoutController*)sessTmoutCtrq->j;
        sessOpts->setDefaultTimeoutController(pController);
        return _kNULL;
    } catch (...) {
        return exhandler( "SessionOptions setDefaultTimeoutController failed", __FILE__, __LINE__);
    }
}

K dht_sessopts_gettimeoutcontroller (  K sessionoptq )
{
    try {
        SKSessionOptions * sessOpts = (SKSessionOptions *) sessionoptq->j;
        return make_symbol(sessOpts->getTimeoutController());
    } catch (...) {
        return exhandler( "SessionOptions getTimeoutController failed", __FILE__, __LINE__);
    }
}


K dht_sessopts_getdhtconfig( K sessionoptq )
{
    try {
        SKSessionOptions * sessOpts = (SKSessionOptions *) sessionoptq->j;
        return make_symbol(sessOpts->getDHTConfig());
    } catch (...) {
        return exhandler( "SessionOptions get DHTConfig failed", __FILE__, __LINE__);
    }
}

K dht_sessopts_getpreferredserver( K sessionoptq )
{
    try {
        SKSessionOptions * sessOpts = (SKSessionOptions *) sessionoptq->j;
        char * host = sessOpts->getPreferredServer();
        K rv = kp(host);
        free(host);
        return rv;
    } catch (...) {
        return exhandler( "SessionOptions get PreferredServer failed", __FILE__, __LINE__);
    }
}

K dht_sessopts_tostring( K sessionoptq )
{
    try {
        SKSessionOptions * sessOpts = (SKSessionOptions *) sessionoptq->j;
        char * str = sessOpts->toString();
        K rv = kp(str);
        free(str);
        return rv;
    } catch (...) {
        return exhandler( "SessionOptions toString failed", __FILE__, __LINE__);
    }
}

K dht_sessopts_delete( K sessionoptq )
{
    try {
        SKSessionOptions * sessOpts = (SKSessionOptions *) ((void *) KLong(sessionoptq));
        delete sessOpts;
        sessionoptq->j = 0;
        return _kNULL;
    }
    catch (...) {
        exhandler("SessionOptions delete failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}

//------------------------ SKVersionConstraint ---------------------------------
K dht_verconstraint_new( K minVersionq, K maxVersionq, K constrModeq, K maxStorageTimeq )
{
    try {
        int64_t minVersion         = (int64_t) (KLong(minVersionq));
        int64_t maxVersion         = (int64_t) (KLong(maxVersionq));
        int64_t maxStorageTime     = (int64_t) (KLong(maxStorageTimeq));
        int mode = (int) (KInt(constrModeq));
        SKVersionConstraint * verConstraint = new SKVersionConstraint(minVersion, maxVersion, (SKVersionConstraintMode)mode, maxStorageTime);
        return make_symbol( verConstraint );
    } catch (...) {
        return exhandler( "VersionConstraint creation failed", __FILE__, __LINE__);
    }
}

K dht_verconstraint_new3( K minVersionq, K maxVersionq, K constrModeq )
{
    try {
        int64_t minVersion         = (int64_t) (KLong(minVersionq));
        int64_t maxVersion         = (int64_t) (KLong(maxVersionq));
        int mode = (int) (KInt(constrModeq));
        SKVersionConstraint * verConstraint = new SKVersionConstraint(minVersion, maxVersion, (SKVersionConstraintMode)mode );
        return make_symbol( verConstraint );
    } catch (...) {
        return exhandler( "VersionConstraint creation failed", __FILE__, __LINE__);
    }
}

K dht_verconstraint_exactmatch( K versionq )
{
    try {
        int64_t version         = (int64_t) (KLong(versionq));
        SKVersionConstraint * verConstraint = SKVersionConstraint::exactMatch( version );
        return make_symbol( verConstraint );
    } catch (...) {
        return exhandler( "VersionConstraint exactmatch failed", __FILE__, __LINE__);
    }
}

K dht_verconstraint_maxaboveorequal( K versionq )
{
    try {
        int64_t version         = (int64_t) (KLong(versionq));
        SKVersionConstraint * verConstraint = SKVersionConstraint::maxAboveOrEqual( version );
        return make_symbol( verConstraint );
    } catch (...) {
        return exhandler( "VersionConstraint maxAboveOrEqual failed", __FILE__, __LINE__);
    }
}

K dht_verconstraint_maxbeloworequal( K versionq )
{
    try {
        int64_t version         = (int64_t) (KLong(versionq));
        SKVersionConstraint * verConstraint = SKVersionConstraint::maxBelowOrEqual( version );
        return make_symbol( verConstraint );
    } catch (...) {
        return exhandler( "VersionConstraint maxBelowOrEqual failed", __FILE__, __LINE__);
    }
}

K dht_verconstraint_minaboveorequal( K versionq )
{
    try {
        int64_t version         = (int64_t) (KLong(versionq));
        SKVersionConstraint * verConstraint = SKVersionConstraint::minAboveOrEqual( version );
        return make_symbol( verConstraint );
    } catch (...) {
        return exhandler( "VersionConstraint minAboveOrEqual failed", __FILE__, __LINE__);
    }
}

K dht_verconstraint_getmax( K verConstraintHandleq )
{
    try {
        SKVersionConstraint * verconstraint  = (SKVersionConstraint*) ((void*)KLong(verConstraintHandleq));
        int64_t maxver = verconstraint->getMax();
        return kj( maxver );
    } catch (...) {
        return exhandler( "VersionConstraint get max failed", __FILE__, __LINE__);
    }
}

K dht_verconstraint_getmaxcreationtime( K verConstraintHandleq )
{
    try {
        SKVersionConstraint * verconstraint  = (SKVersionConstraint*) ((void*)KLong(verConstraintHandleq));
        int64_t maxst = verconstraint->getMaxCreationTime();
        return kj( maxst );
    } catch (...) {
        return exhandler( "VersionConstraint get MaxCreationTime failed", __FILE__, __LINE__);
    }
}

K dht_verconstraint_getmin( K verConstraintHandleq )
{
    try {
        SKVersionConstraint * verconstraint  = (SKVersionConstraint*) ((void*)KLong(verConstraintHandleq));
        int64_t minver = verconstraint->getMin();
        return kj( minver );
    } catch (...) {
        return exhandler( "VersionConstraint get Min failed", __FILE__, __LINE__);
    }
}

K dht_verconstraint_getmode( K verConstraintHandleq )
{
    try {
        SKVersionConstraint * verconstraint  = (SKVersionConstraint*) ((void*)KLong(verConstraintHandleq));
        SKVersionConstraintMode mode = verconstraint->getMode();
        return ki( (int)mode );
    } catch (...) {
        return exhandler( "VersionConstraint getMode failed", __FILE__, __LINE__);
    }
}

K dht_verconstraint_matches( K verConstraintHandleq, K versionq )
{
    try {
        SKVersionConstraint * verconstraint  = (SKVersionConstraint*) ((void*)KLong(verConstraintHandleq));
        int64_t version         = (int64_t) (KLong(versionq));
        return kb( verconstraint->matches(version) );
    } catch (...) {
        return exhandler( "VersionConstraint matches failed", __FILE__, __LINE__);
    }
}

K dht_verconstraint_overlaps( K verConstraintHandleq, K otherVerConstrHandleq )
{
    try {
        SKVersionConstraint * verconstraint  = (SKVersionConstraint*) ((void*)KLong(verConstraintHandleq));
        SKVersionConstraint * verconstraint2  = (SKVersionConstraint*) ((void*)KLong(otherVerConstrHandleq));
        return kb( verconstraint->overlaps(verconstraint2) );
    } catch (...) {
        return exhandler( "VersionConstraint overlaps failed", __FILE__, __LINE__);
    }
}

K dht_verconstraint_equals( K verConstraintHandleq, K otherVerConstrHandleq )
{
    try {
        SKVersionConstraint * verconstraint  = (SKVersionConstraint*) ((void*)KLong(verConstraintHandleq));
        SKVersionConstraint * verconstraint2  = (SKVersionConstraint*) ((void*)KLong(otherVerConstrHandleq));
        return kb( verconstraint->equals(verconstraint2) );
    } catch (...) {
        return exhandler( "VersionConstraint equals failed", __FILE__, __LINE__);
    }
}

K dht_verconstraint_max( K verConstraintHandleq, K versionq )
{
    try {
        int64_t version         = (int64_t) (KLong(versionq));
        SKVersionConstraint * verconstraint  = (SKVersionConstraint*) ((void*)KLong(verConstraintHandleq));
        return make_symbol( verconstraint->max( version ) );
    } catch (...) {
        return exhandler( "VersionConstraint max() failed", __FILE__, __LINE__);
    }
}

K dht_verconstraint_maxcreationtime( K verConstraintHandleq, K creationTimeq )
{
    try {
        int64_t storeTime         = (int64_t) (KLong(creationTimeq));
        SKVersionConstraint * verconstraint  = (SKVersionConstraint*) ((void*)KLong(verConstraintHandleq));
        return make_symbol( verconstraint->maxCreationTime( storeTime ) );
    } catch (...) {
        return exhandler( "VersionConstraint maxCreationTime failed", __FILE__, __LINE__);
    }
}

K dht_verconstraint_min( K verConstraintHandleq, K versionq )
{
    try {
        int64_t version         = (int64_t) (KLong(versionq));
        SKVersionConstraint * verconstraint  = (SKVersionConstraint*) ((void*)KLong(verConstraintHandleq));
        return make_symbol( verconstraint->min( version ) );
    } catch (...) {
        return exhandler( "VersionConstraint min failed", __FILE__, __LINE__);
    }
}

K dht_verconstraint_mode( K verConstraintHandleq, K vcModeq )
{
    try {
        int vcMode         = (int) (KInt(vcModeq));
        SKVersionConstraint * verconstraint  = (SKVersionConstraint*) ((void*)KLong(verConstraintHandleq));
        return make_symbol( verconstraint->mode( (SKVersionConstraintMode) vcMode ) );
    } catch (...) {
        return exhandler( "VersionConstraint mode failed", __FILE__, __LINE__);
    }
}

K dht_verconstraint_delete( K verConstraintHandleq )
{
    try {
        SKVersionConstraint * verconstraint = (SKVersionConstraint *) ((void *) KLong(verConstraintHandleq));
        delete verconstraint;
        verConstraintHandleq->j = 0;
        return _kNULL;
    }
    catch (...) {
        exhandler("VersionConstraint delete failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}

//------------------------ SKNamespacePerspectiveOptions -----------------------
/*
K dht_nsperspectiveopt_new5 (  K keydigesttypeq, K putOptionsq, K getOptionsq, K waitOptionsq, K versionProviderq )
{
    try {
        SKKeyDigestType::SKKeyDigestType digetType  = (SKKeyDigestType::SKKeyDigestType) ((int) (KInt(keydigesttypeq)));
        SKPutOptions * putOpt = (SKPutOptions *) putOptionsq->j;
        SKGetOptions * getOpt = (SKGetOptions *) getOptionsq->j;
        SKWaitOptions * waitOpt = (SKWaitOptions *) waitOptionsq->j;
        SKVersionProvider * versionProv = (SKVersionProvider *) versionProviderq->j;
        
        return make_symbol( new SKNamespacePerspectiveOptions(digetType, putOpt, getOpt, waitOpt, versionProv ) );
    } catch (...) {
        return exhandler( "NamespacePerspective Options creation failed", __FILE__, __LINE__);
    }
}
*/

K dht_nsperspectiveopt_parse( K nspOptsHandleq, K defq )
{
    try {
        SKNamespacePerspectiveOptions * pNspOpts  = (SKNamespacePerspectiveOptions*) ((void*)KLong(nspOptsHandleq));
        char * def          = (char*)(KBytes(defq));
        return make_symbol( pNspOpts->parse( def ) );
    } catch (...) {
        return exhandler( "NamespacePerspectiveOptions parse failed", __FILE__, __LINE__);
    }
}

K dht_nsperspectiveopt_getkeydigesttype( K nspoptHandleq )
{
    try {
        SKNamespacePerspectiveOptions * nspOpt = (SKNamespacePerspectiveOptions *) nspoptHandleq->j;
        return ki( (int)(nspOpt->getKeyDigestType()) );
    } catch (...) {
        return exhandler( "NamespacePerspectiveOptions getKeyDigestType failed", __FILE__, __LINE__);
    }
}

K dht_nsperspectiveopt_getdefaultputoptions( K nspoptHandleq )
{
    try {
        SKNamespacePerspectiveOptions * nspOpt = (SKNamespacePerspectiveOptions *) nspoptHandleq->j;
        return make_symbol( nspOpt->getDefaultPutOptions() );
    } catch (...) {
        return exhandler( "NamespacePerspectiveOptions getDefaultPutOptions failed", __FILE__, __LINE__);
    }
}

K dht_nsperspectiveopt_getdefaultgetoptions( K nspoptHandleq )
{
    try {
        SKNamespacePerspectiveOptions * nspOpt = (SKNamespacePerspectiveOptions *) nspoptHandleq->j;
        return make_symbol( nspOpt->getDefaultGetOptions() );
    } catch (...) {
        return exhandler( "NamespacePerspectiveOptions getDefaultGetOptions failed", __FILE__, __LINE__);
    }
}

K dht_nsperspectiveopt_getdefaultwaitoptions( K nspoptHandleq )
{
    try {
        SKNamespacePerspectiveOptions * nspOpt = (SKNamespacePerspectiveOptions *) nspoptHandleq->j;
        return make_symbol( nspOpt->getDefaultWaitOptions() );
    } catch (...) {
        return exhandler( "NamespacePerspectiveOptions getDefaultWaitOptions failed", __FILE__, __LINE__);
    }
}

K dht_nsperspectiveopt_getdefaultversionprovider( K nspoptHandleq )
{
    try {
        SKNamespacePerspectiveOptions * nspOpt = (SKNamespacePerspectiveOptions *) nspoptHandleq->j;
        return make_symbol( nspOpt->getDefaultVersionProvider() );
    } catch (...) {
        return exhandler( "NamespacePerspectiveOptions getDefaultVersionProvider failed", __FILE__, __LINE__);
    }
}

K dht_nsperspectiveopt_keydigesttype( K nspoptHandleq , K keyDigestTypeq)
{
    try {
        SKKeyDigestType::SKKeyDigestType digesttype = (SKKeyDigestType::SKKeyDigestType) ((int) (KInt(keyDigestTypeq)));
        SKNamespacePerspectiveOptions * nspOpt = (SKNamespacePerspectiveOptions *) nspoptHandleq->j;
        return make_symbol( nspOpt->keyDigestType(digesttype) );
    } catch (...) {
        return exhandler( "NamespacePerspectiveOptions keyDigestType failed", __FILE__, __LINE__);
    }
}

K dht_nsperspectiveopt_defaultputoptions( K nspoptHandleq , K defaultPutOpsq)
{
    try {
        SKPutOptions * putOpts = (SKPutOptions *) defaultPutOpsq->j;
        SKNamespacePerspectiveOptions * nspOpt = (SKNamespacePerspectiveOptions *) nspoptHandleq->j;
        return make_symbol( nspOpt->defaultPutOptions(putOpts) );
    } catch (...) {
        return exhandler( "NamespacePerspectiveOptions defaultPutOptions failed", __FILE__, __LINE__);
    }
}

K dht_nsperspectiveopt_defaultgetoptions( K nspoptHandleq , K defaultGetOpsq)
{
    try {
        SKGetOptions * getOpts = (SKGetOptions *) defaultGetOpsq->j;
        SKNamespacePerspectiveOptions * nspOpt = (SKNamespacePerspectiveOptions *) nspoptHandleq->j;
        return make_symbol( nspOpt->defaultGetOptions(getOpts) );
    } catch (...) {
        return exhandler( "NamespacePerspectiveOptions defaultGetOptions failed", __FILE__, __LINE__);
    }
}

K dht_nsperspectiveopt_defaultwaitoptions( K nspoptHandleq , K defaultWaitOpsq)
{
    try {
        SKWaitOptions * waitOpts = (SKWaitOptions *) defaultWaitOpsq->j;
        SKNamespacePerspectiveOptions * nspOpt = (SKNamespacePerspectiveOptions *) nspoptHandleq->j;
        return make_symbol( nspOpt->defaultWaitOptions(waitOpts) );
    } catch (...) {
        return exhandler( "NamespacePerspectiveOptions defaultWaitOptions failed", __FILE__, __LINE__);
    }
}

K dht_nsperspectiveopt_defaultversionprovider( K nspoptHandleq , K versionProviderq)
{
    try {
        SKVersionProvider * versionProvider = (SKVersionProvider *) versionProviderq->j;
        SKNamespacePerspectiveOptions * nspOpt = (SKNamespacePerspectiveOptions *) nspoptHandleq->j;
        return make_symbol( nspOpt->defaultVersionProvider(versionProvider) );
    } catch (...) {
        return exhandler( "NamespacePerspectiveOptions defaultVersionProvider failed", __FILE__, __LINE__);
    }
}

K dht_nsperspectiveopt_delete( K nspoptHandleq )
{
    try {
        SKNamespacePerspectiveOptions * nspopt = (SKNamespacePerspectiveOptions *) ((void *) KLong(nspoptHandleq));
        delete nspopt;
        nspoptHandleq->j = 0;
        return _kNULL;
    }
    catch (...) {
        exhandler("NamespacePerspectiveOptions delete failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}

//------------------------ SKVersionProvider -----------------------------------
//TODO: C-TOR ??
K dht_versionprovider_getversion( K versionProviderq )
{
    try {
        SKVersionProvider * verProvider  = (SKVersionProvider*) ((void*)KLong(versionProviderq));
        return kj( verProvider->getVersion() );
    } catch (...) {
        return exhandler( "VersionProvider getVersion failed", __FILE__, __LINE__);
    }
}

K dht_versionprovider_delete( K versionProviderq )
{
    try {
        SKVersionProvider * verProvider = (SKVersionProvider *) ((void *) KLong(versionProviderq));
        delete verProvider;
        versionProviderq->j = 0;
        return _kNULL;
    }
    catch (...) {
        exhandler("VersionProvider delete failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}

//------------------------ SKSyncWritableNSPerspective -------------------------
K dht_syncwritablensp_put( K swnspHandleq, K keyq, K valueq ) 
{
    try {
        if(kC(keyq)[keyq->n] != 0){
           kC(keyq)[keyq->n] = 0;
        }
        return internal_put( (SKSyncWritableNSPerspective *) swnspHandleq->j , keyq, valueq );
    }
    catch (...) {
        exhandler( "syncWritableNsp put failed", __FILE__, __LINE__);
    }
    return K(0) ;    
}

K dht_syncwritablensp_putwo( K swnspHandleq, K keyq, K valueq, K putopthq ) 
{
    try {
        if(kC(keyq)[keyq->n] != 0){
           kC(keyq)[keyq->n] = 0;
        }
        return internal_put( (SKSyncWritableNSPerspective *) swnspHandleq->j , keyq, valueq, (SKPutOptions *) putopthq->j );
    } catch (...) {
        exhandler( "syncWritableNsp put failed", __FILE__, __LINE__);
    }
    return K(0) ;    
}

K dht_syncwritablensp_mputwo( K swnspHandleq, K keylistq, K valuelistq, K putopthq ) 
{
    try {
        if ( keylistq->n == valuelistq->n ) {
            internal_nullterm_list(keylistq);
            internal_nullterm_list(valuelistq);
            return internal_mput( (SKSyncWritableNSPerspective *) swnspHandleq->j, keylistq, valuelistq, (SKPutOptions *) putopthq->j );
        }
        krr( "dht_syncwritablensp_mput: keylist and valuelist sizes do not match" ) ;
    } catch (...) {
        exhandler( "syncWritableNsp mput failed", __FILE__, __LINE__);
    }
    return K(0) ;    
}

K dht_syncwritablensp_mput( K swnspHandleq, K keylistq, K valuelistq ) 
{
    return dht_syncwritablensp_mputwo( swnspHandleq, keylistq, valuelistq, kj(0) );
}

K dht_syncwritablensp_mputdictwo( K swnspHandleq, K dictq, K putopthq ) 
{
    try {
        //internal_nullterm_dict(dictq);
        K keylistq = kK(dictq)[0] ; 
        K valuelistq = kK(dictq)[1] ; 
        return dht_syncwritablensp_mputwo(swnspHandleq, keylistq, valuelistq, putopthq );
    } catch (...) {
        exhandler( "syncWritableNsp mput failed", __FILE__, __LINE__);
    }
}

K dht_syncwritablensp_mputdict( K swnspHandleq, K dictq ) 
{
    return dht_syncwritablensp_mputdictwo(swnspHandleq, dictq, kj(0) );
}

K dht_syncwritablensp_getoptions( K swnspHandleq )
{
    try {
        SKBaseNSPerspective * bp = static_cast<SKBaseNSPerspective*>((SKSyncWritableNSPerspective *) swnspHandleq->j);
        return make_symbol( bp->getOptions() );
    } catch (...) {
        exhandler( "syncwritablensp get options failed", __FILE__, __LINE__);
    }
}

K dht_syncwritablensp_getnamespace( K swnspHandleq )
{
    try {
        SKBaseNSPerspective * bp = static_cast<SKBaseNSPerspective*>((SKSyncWritableNSPerspective *) swnspHandleq->j);
        return make_symbol( bp->getNamespace() );
    } catch (...) {
        exhandler( "syncwritablensp get namespace failed", __FILE__, __LINE__);
    }
}

K dht_syncwritablensp_getname( K swnspHandleq )
{
    try {
        SKBaseNSPerspective * bp = static_cast<SKBaseNSPerspective*>((SKSyncWritableNSPerspective *) swnspHandleq->j);
        string str = bp->getName() ;
        return kpn( const_cast<char*>(str.c_str()), str.size());
    } catch (...) {
        exhandler( "syncwritablensp get name failed", __FILE__, __LINE__);
    }
}

K dht_syncwritablensp_setoptions( K swnspHandleq , K nsPerspOptq )
{
    try {
        SKBaseNSPerspective * bp = static_cast<SKBaseNSPerspective*>((SKSyncWritableNSPerspective *) swnspHandleq->j);
        SKNamespacePerspectiveOptions * pNsPOpt = (SKNamespacePerspectiveOptions *) nsPerspOptq->j;
        bp->setOptions(pNsPOpt);
        return _kNULL ;
    } catch (...) {
        return exhandler( "syncwritablensp set default NamespacePerspectiveOptions failed", __FILE__, __LINE__);
    }
}

K dht_syncwritablensp_setdefaultversion( K swnspHandleq , K versionq )
{
    try {
        SKBaseNSPerspective * bp = static_cast<SKBaseNSPerspective*>((SKSyncWritableNSPerspective *) swnspHandleq->j);
        int64_t version  = (int64_t) (kJ(versionq));
        bp->setDefaultVersion(version);
        return _kNULL ;
    } catch (...) {
        return exhandler( "syncwritablensp set default version failed", __FILE__, __LINE__);
    }
}

K dht_syncwritablensp_setretrievalverconstraint( K swnspHandleq , K versionConstraintq )
{
    try {
        SKBaseNSPerspective * bp = static_cast<SKBaseNSPerspective*>((SKSyncWritableNSPerspective *) swnspHandleq->j);
        SKVersionConstraint * pVersionConstraint = (SKVersionConstraint *) versionConstraintq->j;
        bp->setDefaultRetrievalVersionConstraint(pVersionConstraint);
        return _kNULL ;
    } catch (...) {
        return exhandler( "syncwritablensp set default Retrieval VersionConstraint failed", __FILE__, __LINE__);
    }
}

K dht_syncwritablensp_setversionprovider( K swnspHandleq , K versionProviderq )
{
    try {
        SKBaseNSPerspective * bp = static_cast<SKBaseNSPerspective*>((SKSyncWritableNSPerspective *) swnspHandleq->j);
        SKVersionProvider * pVersionProvider = (SKVersionProvider *) versionProviderq->j;
        bp->setDefaultVersionProvider(pVersionProvider);
        return _kNULL ;
    } catch (...) {
        return exhandler( "syncwritablensp set default VersionProvider failed", __FILE__, __LINE__);
    }
}

K dht_syncwritablensp_close( K swnspHandleq )
{
    try {
        SKBaseNSPerspective * bp = static_cast<SKBaseNSPerspective*>((SKSyncWritableNSPerspective *) swnspHandleq->j);
        bp->close();
        return _kNULL ;
    } catch (...) {
        exhandler( "syncwritablensp close failed", __FILE__, __LINE__);
    }
}

K dht_syncwritablensp_delete( K swnspHandleq )
{
    try {
        SKSyncWritableNSPerspective * swnsp = (SKSyncWritableNSPerspective *) ((void *) KLong(swnspHandleq));
        delete swnsp;
        swnspHandleq->j = 0;
        return _kNULL;
    }
    catch (...) {
        exhandler("syncwritablensp delete failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}

//------------------------ SKSyncReadableNSPerspective -------------------------
K dht_syncreadablensp_get( K srnspHandleq, K keyq )
{
    try {
        return internal_get( (SKSyncReadableNSPerspective *) srnspHandleq->j, keyq, false ) ;
    } catch (...) {
        exhandler( "syncreadablensp get failed", __FILE__, __LINE__);
    }
}

K dht_syncreadablensp_waitfor( K srnspHandleq, K keyq )
{
    try {
        return internal_get( (SKSyncReadableNSPerspective *) srnspHandleq->j, keyq, true ) ;
    } catch (...) {
        exhandler( "syncreadablensp waitfor failed", __FILE__, __LINE__);
    }
}

K dht_syncreadablensp_getwo( K srnspHandleq, K keyq, K getopsHandleq )
{
    try {
        SKGetOptions* pGetOpt =  (SKGetOptions*)getopsHandleq->j;
        SKStoredValue * psv = internal_getwo( (SKSyncReadableNSPerspective *) srnspHandleq->j, keyq, false, pGetOpt );
        return (psv!=NULL) ? make_symbol(psv) : K(0);
    } catch (...) {
        exhandler( "syncreadablensp get failed", __FILE__, __LINE__);
    }
}

K dht_syncreadablensp_waitforwo( K srnspHandleq, K keyq, K waitopsHandleq )
{
    try {
        SKWaitOptions* pWaitOpt =  (SKWaitOptions*)waitopsHandleq->j;
        SKStoredValue * psv = internal_getwo( (SKSyncReadableNSPerspective *) srnspHandleq->j, keyq, true, pWaitOpt );
        return (psv!=NULL) ? make_symbol(psv) : K(0);
    } catch (...) {
        exhandler( "syncreadablensp get failed", __FILE__, __LINE__);
    }
}

K dht_syncreadablensp_getasdictwo( K srnspHandleq, K keyq, K getopsHandleq )
{
    try {
        SKGetOptions* pGetOpt =  (SKGetOptions*)getopsHandleq->j ;
        return internal_getwo_asdict( (SKSyncReadableNSPerspective *) srnspHandleq->j, keyq, false, pGetOpt );
    } catch (...) {
        exhandler( "syncreadablensp get with options failed", __FILE__, __LINE__);
    }
}

K dht_syncreadablensp_waitforasdictwo( K srnspHandleq, K keyq, K waitopsHandleq )
{
    try {
        SKWaitOptions* pWaitOpt =  (SKWaitOptions*)waitopsHandleq->j ;
        return internal_getwo_asdict( (SKSyncReadableNSPerspective *) srnspHandleq->j, keyq, true, pWaitOpt );
    } catch (...) {
        exhandler( "syncreadablensp waitfor with options failed", __FILE__, __LINE__);
    }
}

K dht_syncreadablensp_mget( K srnspHandleq, K keylistq )
{
    try {
        internal_nullterm_list(keylistq);
        return internal_mget( (SKSyncReadableNSPerspective *) srnspHandleq->j, keylistq, false ) ;
    } catch (...) {
        exhandler( "syncreadablensp get failed", __FILE__, __LINE__);
    }
}

K dht_syncreadablensp_mwait( K srnspHandleq, K keylistq )
{
    try {
        internal_nullterm_list(keylistq);
        return internal_mget( (SKSyncReadableNSPerspective *) srnspHandleq->j, keylistq , true) ;
    } catch (...) {
        exhandler( "syncreadablensp waitfor failed", __FILE__, __LINE__);
    }
}

K dht_syncreadablensp_getoptions( K srnspHandleq )
{
    try {
        SKBaseNSPerspective * bp = static_cast<SKBaseNSPerspective*>((SKSyncReadableNSPerspective *) srnspHandleq->j);
        return make_symbol( bp->getOptions() );
    } catch (...) {
        exhandler( "syncreadablensp get options failed", __FILE__, __LINE__);
    }
}

K dht_syncreadablensp_getnamespace( K srnspHandleq )
{
    try {
        SKBaseNSPerspective * bp = static_cast<SKBaseNSPerspective*>((SKSyncReadableNSPerspective *) srnspHandleq->j);
        return make_symbol( bp->getNamespace() );
    } catch (...) {
        exhandler( "syncreadablensp get namespace failed", __FILE__, __LINE__);
    }
}

K dht_syncreadablensp_getname( K srnspHandleq )
{
    try {
        SKBaseNSPerspective * bp = static_cast<SKBaseNSPerspective*>((SKSyncReadableNSPerspective *) srnspHandleq->j);
        string str = bp->getName() ;
        return kpn( const_cast<char*>(str.c_str()), str.size());
    } catch (...) {
        exhandler( "syncreadablensp get name failed", __FILE__, __LINE__);
    }
}

K dht_syncreadablensp_setoptions( K srnspHandleq , K nsPerspOptq )
{
    try {
        SKBaseNSPerspective * bp = static_cast<SKBaseNSPerspective*>((SKSyncReadableNSPerspective *) srnspHandleq->j);
        SKNamespacePerspectiveOptions * pNsPOpt = (SKNamespacePerspectiveOptions *) nsPerspOptq->j;
        bp->setOptions(pNsPOpt);
        return _kNULL ;
    } catch (...) {
        return exhandler( "syncreadablensp set default NamespacePerspectiveOptions failed", __FILE__, __LINE__);
    }
}

K dht_syncreadablensp_setdefaultversion( K srnspHandleq , K versionq )
{
    try {
        SKBaseNSPerspective * bp = static_cast<SKBaseNSPerspective*>((SKSyncReadableNSPerspective *) srnspHandleq->j);
        int64_t version  = (int64_t) (kJ(versionq));
        bp->setDefaultVersion(version);
        return _kNULL ;
    } catch (...) {
        return exhandler( "syncreadablensp set default version failed", __FILE__, __LINE__);
    }
}

K dht_syncreadablensp_setretrievalverconstraint( K srnspHandleq , K versionConstraintq )
{
    try {
        SKBaseNSPerspective * bp = static_cast<SKBaseNSPerspective*>((SKSyncReadableNSPerspective *) srnspHandleq->j);
        SKVersionConstraint * pVersionConstraint = (SKVersionConstraint *) versionConstraintq->j;
        bp->setDefaultRetrievalVersionConstraint(pVersionConstraint);
        return _kNULL ;
    } catch (...) {
        return exhandler( "syncreadablensp set default Retrieval VersionConstraint failed", __FILE__, __LINE__);
    }
}

K dht_syncreadablensp_setversionprovider( K srnspHandleq , K versionProviderq )
{
    try {
        SKBaseNSPerspective * bp = static_cast<SKBaseNSPerspective*>((SKSyncReadableNSPerspective *) srnspHandleq->j);
        SKVersionProvider * pVersionProvider = (SKVersionProvider *) versionProviderq->j;
        bp->setDefaultVersionProvider(pVersionProvider);
        return _kNULL ;
    } catch (...) {
        return exhandler( "syncreadablensp set default VersionProvider failed", __FILE__, __LINE__);
    }
}

K dht_syncreadablensp_close( K srnspHandleq )
{
    try {
        SKBaseNSPerspective * bp = static_cast<SKBaseNSPerspective*>((SKSyncReadableNSPerspective * ) srnspHandleq->j);
        bp->close();
        return _kNULL ;
    } catch (...) {
        exhandler( "syncreadablensp close failed", __FILE__, __LINE__);
    }
}


K dht_syncreadablensp_delete( K srnspHandleq )
{
    try {
        SKSyncReadableNSPerspective * srnsp = (SKSyncReadableNSPerspective *) ((void *) KLong(srnspHandleq));
        delete srnsp;
        srnspHandleq->j = 0;
        return _kNULL;
    }
    catch (...) {
        exhandler("syncreadablensp delete failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}

//------------------------ SKAsyncWritableNSPerspective ------------------------
K dht_asyncwritablensp_put( K awnspHandleq, K keyq, K valueq ) 
{
    try {
        if(kC(keyq)[keyq->n] != 0){
           kC(keyq)[keyq->n] = 0;
        }
        return internal_awnsp_put( (SKAsyncWritableNSPerspective *) awnspHandleq->j , keyq, valueq );
    }
    catch (...) {
        exhandler( "asyncwritablensp put failed", __FILE__, __LINE__);
    }
    return K(0) ;    
}

K dht_asyncwritablensp_putwo( K awnspHandleq, K keyq, K valueq, K putopthq ) 
{
    try {
        if(kC(keyq)[keyq->n] != 0){
           kC(keyq)[keyq->n] = 0;
        }
        return internal_awnsp_put( (SKAsyncWritableNSPerspective *) awnspHandleq->j , keyq, valueq, (SKPutOptions *) putopthq->j );
    } catch (...) {
        exhandler( "asyncwritablensp put failed", __FILE__, __LINE__);
    }
    return K(0) ;    
}

K dht_asyncwritablensp_mputwo( K awnspHandleq, K keylistq, K valuelistq, K putopthq ) 
{
    try {
        if ( keylistq->n == valuelistq->n ) {
            internal_nullterm_list(keylistq);
            internal_nullterm_list(valuelistq);
            return internal_awnsp_mput( (SKAsyncWritableNSPerspective *) awnspHandleq->j, keylistq, valuelistq, (SKPutOptions *) putopthq->j );
        }
        krr( "dht_asyncwritablensp_mputwo: keylist and valuelist sizes do not match" ) ;
    } catch (...) {
        exhandler( "asyncwritablensp mput failed", __FILE__, __LINE__);
    }
    return K(0) ;    
}

K dht_asyncwritablensp_mput( K awnspHandleq, K keylistq, K valuelistq ) 
{
    return dht_asyncwritablensp_mputwo( awnspHandleq, keylistq, valuelistq, _kNULL );
}

K dht_asyncwritablensp_mputdictwo( K awnspHandleq, K dictq, K putopthq ) 
{
    try {
        //internal_nullterm_dict(dictq);
        K keylistq = kK(dictq)[0] ; 
        K valuelistq = kK(dictq)[1] ; 
        return dht_asyncwritablensp_mputwo(awnspHandleq, keylistq, valuelistq, putopthq );
    } catch (...) {
        exhandler( "asyncwritablensp mput failed", __FILE__, __LINE__);
    }
}

K dht_asyncwritablensp_mputdict( K awnspHandleq, K dictq ) 
{
    return dht_asyncwritablensp_mputdictwo(awnspHandleq, dictq, _kNULL );
}

K dht_asyncwritablensp_getoptions( K awnspHandleq )
{
    try {
        SKBaseNSPerspective * bp = static_cast<SKBaseNSPerspective*>((SKAsyncWritableNSPerspective *) awnspHandleq->j);
        return make_symbol( bp->getOptions() );
    } catch (...) {
        exhandler( "asyncwritablensp get options failed", __FILE__, __LINE__);
    }
}

K dht_asyncwritablensp_getnamespace( K awnspHandleq )
{
    try {
        SKBaseNSPerspective * bp = static_cast<SKBaseNSPerspective*>((SKAsyncWritableNSPerspective *) awnspHandleq->j);
        return make_symbol( bp->getNamespace() );
    } catch (...) {
        exhandler( "asyncwritablensp get namespace failed", __FILE__, __LINE__);
    }
}

K dht_asyncwritablensp_getname( K awnspHandleq )
{
    try {
        SKBaseNSPerspective * bp = static_cast<SKBaseNSPerspective*>((SKAsyncWritableNSPerspective *) awnspHandleq->j);
        string str = bp->getName() ;
        return kpn( const_cast<char*>(str.c_str()), str.size());
    } catch (...) {
        exhandler( "asyncwritablensp get name failed", __FILE__, __LINE__);
    }
}

K dht_asyncwritablensp_setoptions( K awnspHandleq , K nsPerspOptq )
{
    try {
        SKBaseNSPerspective * bp = static_cast<SKBaseNSPerspective*>((SKAsyncWritableNSPerspective *) awnspHandleq->j);
        SKNamespacePerspectiveOptions * pNsPOpt = (SKNamespacePerspectiveOptions *) nsPerspOptq->j;
        bp->setOptions(pNsPOpt);
        return _kNULL ;
    } catch (...) {
        return exhandler( "asyncwritablensp set default NamespacePerspectiveOptions failed", __FILE__, __LINE__);
    }
}

K dht_asyncwritablensp_setdefaultversion( K awnspHandleq , K versionq )
{
    try {
        SKBaseNSPerspective * bp = static_cast<SKBaseNSPerspective*>((SKAsyncWritableNSPerspective *) awnspHandleq->j);
        int64_t version  = (int64_t) (kJ(versionq));
        bp->setDefaultVersion(version);
        return _kNULL ;
    } catch (...) {
        return exhandler( "asyncwritablensp set default version failed", __FILE__, __LINE__);
    }
}

K dht_asyncwritablensp_setretrievalverconstraint( K awnspHandleq , K versionConstraintq )
{
    try {
        SKBaseNSPerspective * bp = static_cast<SKBaseNSPerspective*>((SKAsyncWritableNSPerspective *) awnspHandleq->j);
        SKVersionConstraint * pVersionConstraint = (SKVersionConstraint *) versionConstraintq->j;
        bp->setDefaultRetrievalVersionConstraint(pVersionConstraint);
        return _kNULL ;
    } catch (...) {
        return exhandler( "asyncwritablensp set default RetrievalVersionConstraint failed", __FILE__, __LINE__);
    }
}

K dht_asyncwritablensp_setversionprovider( K awnspHandleq , K versionProviderq )
{
    try {
        SKBaseNSPerspective * bp = static_cast<SKBaseNSPerspective*>((SKAsyncWritableNSPerspective *) awnspHandleq->j);
        SKVersionProvider * pVersionProvider = (SKVersionProvider *) versionProviderq->j;
        bp->setDefaultVersionProvider(pVersionProvider);
        return _kNULL ;
    } catch (...) {
        return exhandler( "asyncwritablensp set default VersionProvider failed", __FILE__, __LINE__);
    }
}

K dht_asyncwritablensp_close( K awnspHandleq )
{
    try {
        SKBaseNSPerspective * bp = static_cast<SKBaseNSPerspective*>((SKAsyncWritableNSPerspective *) awnspHandleq->j);
        bp->close();
        return _kNULL ;
    } catch (...) {
        exhandler( "asyncwritablensp close failed", __FILE__, __LINE__);
    }
}

K dht_asyncwritablensp_delete( K awnspHandleq )
{
    try {
        SKAsyncWritableNSPerspective * awnsp = (SKAsyncWritableNSPerspective *) ((void *) KLong(awnspHandleq));
        delete awnsp;
        awnspHandleq->j = 0;
        return _kNULL;
    }
    catch (...) {
        exhandler("asyncwritablensp delete failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}

//------------------------ SKAsyncReadableNSPerspective ------------------------
//FIXME: TODO any Async retrievals with Options ?
K dht_asyncreadablensp_get( K arnspHandleq, K keyq )
{
    try {
        return internal_arnsp_get( (SKAsyncReadableNSPerspective *) arnspHandleq->j, keyq, false ) ;
    } catch (...) {
        exhandler( "asyncreadablensp get failed", __FILE__, __LINE__);
    }
}

K dht_asyncreadablensp_waitfor( K arnspHandleq, K keyq )
{
    try {
        return internal_arnsp_get( (SKAsyncReadableNSPerspective *) arnspHandleq->j, keyq, true ) ;
    } catch (...) {
        exhandler( "asyncreadablensp waitfor failed", __FILE__, __LINE__);
    }
}

K dht_asyncreadablensp_mget( K arnspHandleq, K keylistq )
{
    try {
        internal_nullterm_list(keylistq);
        return internal_arnsp_mget( (SKAsyncReadableNSPerspective *) arnspHandleq->j, keylistq, false ) ;
    } catch (...) {
        exhandler( "asyncreadablensp get failed", __FILE__, __LINE__);
    }
}

K dht_asyncreadablensp_mwait( K arnspHandleq, K keylistq )
{
    try {
        internal_nullterm_list(keylistq);
        return internal_arnsp_mget( (SKAsyncReadableNSPerspective *) arnspHandleq->j, keylistq , true) ;
    } catch (...) {
        exhandler( "asyncreadablensp waitfor failed", __FILE__, __LINE__);
    }
}

K dht_asyncreadablensp_mgetwo( K arnspHandleq, K keylistq, K getoptsHandleq )
{
    try {
        internal_nullterm_list(keylistq);
        return internal_arnsp_mgetwo( (SKAsyncReadableNSPerspective *) arnspHandleq->j, keylistq, false, getoptsHandleq ) ;
    } catch (...) {
        exhandler( "Asyncnsp mget failed", __FILE__, __LINE__);
    }
}

K dht_asyncreadablensp_mwaitwo( K arnspHandleq, K keylistq, K waitoptsHandleq )
{
    try {
        internal_nullterm_list(keylistq);
        return internal_arnsp_mgetwo( (SKAsyncReadableNSPerspective *) arnspHandleq->j, keylistq , true, waitoptsHandleq) ;
    } catch (...) {
        exhandler( "Asyncnsp mwaitfor failed", __FILE__, __LINE__);
    }
}

K dht_asyncreadablensp_getoptions( K arnspHandleq )
{
    try {
        SKBaseNSPerspective * bp = dynamic_cast<SKBaseNSPerspective*>((SKAsyncReadableNSPerspective *) arnspHandleq->j);
        return make_symbol( bp->getOptions() );
    } catch (...) {
        exhandler( "asyncreadablensp get options failed", __FILE__, __LINE__);
    }
}

K dht_asyncreadablensp_getnamespace( K arnspHandleq )
{
    try {
        SKBaseNSPerspective * bp = dynamic_cast<SKBaseNSPerspective*>((SKAsyncReadableNSPerspective *) arnspHandleq->j);
        return make_symbol( bp->getNamespace() );
    } catch (...) {
        exhandler( "asyncreadablensp get namespace failed", __FILE__, __LINE__);
    }
}

K dht_asyncreadablensp_getname( K arnspHandleq )
{
    try {
        SKBaseNSPerspective * bp = dynamic_cast<SKBaseNSPerspective*>((SKAsyncReadableNSPerspective *) arnspHandleq->j);
        string str = bp->getName() ;
        return kpn( const_cast<char*>(str.c_str()), str.size());
    } catch (...) {
        exhandler( "asyncreadablensp get name failed", __FILE__, __LINE__);
    }
}

K dht_asyncreadablensp_setoptions( K arnspHandleq , K nsPerspOptq )
{
    try {
        SKBaseNSPerspective * bp = static_cast<SKBaseNSPerspective*>((SKAsyncReadableNSPerspective *) arnspHandleq->j);
        SKNamespacePerspectiveOptions * pNsPOpt = (SKNamespacePerspectiveOptions *) nsPerspOptq->j;
        bp->setOptions(pNsPOpt);
        return _kNULL ;
    } catch (...) {
        return exhandler( "asyncreadablensp set default NamespacePerspectiveOptions failed", __FILE__, __LINE__);
    }
}

K dht_asyncreadablensp_setdefaultversion( K arnspHandleq , K versionq )
{
    try {
        SKBaseNSPerspective * bp = static_cast<SKBaseNSPerspective*>((SKAsyncReadableNSPerspective *) arnspHandleq->j);
        int64_t version  = (int64_t) (kJ(versionq));
        bp->setDefaultVersion(version);
        return _kNULL ;
    } catch (...) {
        return exhandler( "asyncreadablensp set default version failed", __FILE__, __LINE__);
    }
}

K dht_asyncreadablensp_setretrievalverconstraint( K arnspHandleq , K versionConstraintq )
{
    try {
        SKBaseNSPerspective * bp = static_cast<SKBaseNSPerspective*>((SKAsyncReadableNSPerspective *) arnspHandleq->j);
        SKVersionConstraint * pVersionConstraint = (SKVersionConstraint *) versionConstraintq->j;
        bp->setDefaultRetrievalVersionConstraint(pVersionConstraint);
        return _kNULL ;
    } catch (...) {
        return exhandler( "asyncreadablensp set default Retrieval VersionConstraint failed", __FILE__, __LINE__);
    }
}

K dht_asyncreadablensp_setversionprovider( K arnspHandleq , K versionProviderq )
{
    try {
        SKBaseNSPerspective * bp = static_cast<SKBaseNSPerspective*>((SKAsyncReadableNSPerspective *) arnspHandleq->j);
        SKVersionProvider * pVersionProvider = (SKVersionProvider *) versionProviderq->j;
        bp->setDefaultVersionProvider(pVersionProvider);
        return _kNULL ;
    } catch (...) {
        return exhandler( "asyncreadablensp set default VersionProvider failed", __FILE__, __LINE__);
    }
}

K dht_asyncreadablensp_close( K arnspHandleq )
{
    try {
        SKBaseNSPerspective * bp = static_cast<SKBaseNSPerspective*>((SKAsyncReadableNSPerspective *) arnspHandleq->j);
        bp->close();
        return _kNULL ;
    } catch (...) {
        exhandler( "asyncreadablensp close failed", __FILE__, __LINE__);
    }
}

K dht_asyncreadablensp_delete( K arnspHandleq )
{
    try {
        SKAsyncReadableNSPerspective * arnsp = (SKAsyncReadableNSPerspective *) ((void *) KLong(arnspHandleq));
        delete arnsp;
        arnspHandleq->j = 0;
        return _kNULL;
    }
    catch (...) {
        exhandler("asyncreadablensp delete failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}


// -------------------- SKSecondaryTarget ---------------------------------
K dht_secondarytarget_new(K targetTypeq, K targetq)
{
    try {
        int targetType =  (int)(Kinteger(targetTypeq));
        SKSecondaryTarget * secTarget = new SKSecondaryTarget( (SKSecondaryTargetType)targetType, STDSTRING(targetq).c_str() );
        return make_symbol(secTarget);
    } catch (...) {
        return exhandler( "secondarytarget c-tor failed", __FILE__, __LINE__);
    }
}

K dht_secondarytarget_gettype (  K secondaryTgtHandleq )
{
    try {
        SKSecondaryTarget * secTarget = (SKSecondaryTarget *) ((void *) KLong(secondaryTgtHandleq));
        SKSecondaryTargetType tgtType = secTarget->getType();
        return ki( (int) tgtType);
    } 
    catch (...) {
        return exhandler( "secondarytarget gettype failed", __FILE__, __LINE__);
    }
}
K dht_secondarytarget_gettarget(  K secondaryTgtHandleq)
{
    try {
        SKSecondaryTarget * secTarget = (SKSecondaryTarget *) ((void *) KLong(secondaryTgtHandleq));
        char * str = secTarget->getTarget() ;
        K tgt = kpn( str, strlen(str) );
        free(str);
        return tgt;
    } 
    catch (...) {
        return exhandler( "secondarytarget getTarget failed", __FILE__, __LINE__);
    }
}

K dht_secondarytarget_tostring( K secondaryTgtHandleq)
{
    try {
        SKSecondaryTarget * secTarget = (SKSecondaryTarget *) ((void *) KLong(secondaryTgtHandleq));
        char * str = secTarget->toString() ;
        K tgt = kpn( str, strlen(str) );
        free(str);
        return tgt;
    } 
    catch (...) {
        return exhandler( "secondarytarget tostring failed", __FILE__, __LINE__);
    }
}

K dht_secondarytarget_delete (  K secondaryTgtHandleq )
{
    try {
        SKSecondaryTarget * secTarget = (SKSecondaryTarget *) ((void *) KLong(secondaryTgtHandleq));
        delete secTarget;
        secondaryTgtHandleq->j = 0;
        return _kNULL;
    }
    catch (...) {
        exhandler("asyncreadablensp delete failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}

// -------------------- SKOpSizeBasedTimeoutController ---------------------------------
K dht_opsizebasedtimeoutcontroller_new(K dummy)
{
    try {
        SKOpSizeBasedTimeoutController * opController = new SKOpSizeBasedTimeoutController( );
        return make_symbol(opController);
    } catch (...) {
        return exhandler( "OpSizeBasedTimeoutController c-tor failed", __FILE__, __LINE__);
    }
}

K dht_opsizebasedtimeoutcontroller_new4(K maxAttemptsq, K constantTimeMillisq, K itemTimeMillisq, K maxRelTimeoutMillisq)
{
    try {
        int maxAttempts =  (int)(Kinteger(maxAttemptsq));
        int constantTimeMillis =  (int)(Kinteger(constantTimeMillisq));
        int itemTimeMillis =  (int)(Kinteger(itemTimeMillisq));
        int maxRelTimeoutMillis =  (int)(Kinteger(maxRelTimeoutMillisq));
        SKOpSizeBasedTimeoutController * opController = new SKOpSizeBasedTimeoutController( maxAttempts, 
                constantTimeMillis, itemTimeMillis, maxRelTimeoutMillis);
        return make_symbol(opController);
    } catch (...) {
        return exhandler( "OpSizeBasedTimeoutController c-tor() failed", __FILE__, __LINE__);
    }
}

K dht_opsizebasedtimeoutcontroller_parse(K defq)
{
    try {
        return make_symbol( SKOpSizeBasedTimeoutController::parse(STDSTRING(defq).c_str() ) );
    }
    catch (...) {
        exhandler( "OpSizeBasedTimeoutController parsing failed", __FILE__, __LINE__);
    }
    return _kNULL ;
}

K dht_opsizebasedtimeoutcontroller_getmaxattempts(K opControllerq, K asyncOperationHandleq)
{
    try {
        SKOpSizeBasedTimeoutController * pOpController = (SKOpSizeBasedTimeoutController *) ((void *) KLong(opControllerq));
        SKAsyncOperation * pAsyncOp = (SKAsyncOperation *) ((void *) KLong(asyncOperationHandleq));
        int maxattempts = pOpController->getMaxAttempts(pAsyncOp);
        return ki(maxattempts);
    } 
    catch (...) {
        return exhandler( "OpSizeBasedTimeoutController getMaxAttempts failed", __FILE__, __LINE__);
    }
}

K dht_opsizebasedtimeoutcontroller_getrelativetimeoutmillisforattempt(K opControllerq, K asyncOperationHandleq, K attemptIndexq)
{
    try {
        SKOpSizeBasedTimeoutController * pOpController = (SKOpSizeBasedTimeoutController *) ((void *) KLong(opControllerq));
        SKAsyncOperation * pAsyncOp = (SKAsyncOperation *) ((void *) KLong(asyncOperationHandleq));
        int attemptIndex = (int) Kinteger(attemptIndexq);
        int relativetimeoutmillis = pOpController->getRelativeTimeoutMillisForAttempt(pAsyncOp, attemptIndex);
        return ki(relativetimeoutmillis);
    } 
    catch (...) {
        return exhandler( "OpSizeBasedTimeoutController getRelativeTimeoutMillisForAttempt failed", __FILE__, __LINE__);
    }
}

K dht_opsizebasedtimeoutcontroller_getmaxrelativetimeoutmillis(K opControllerq, K asyncOperationHandleq)
{
    try {
        SKOpSizeBasedTimeoutController * pOpController = (SKOpSizeBasedTimeoutController *) ((void *) KLong(opControllerq));
        SKAsyncOperation * pAsyncOp = (SKAsyncOperation *) ((void *) KLong(asyncOperationHandleq));
        int maxrelativetimeoutms = pOpController->getMaxRelativeTimeoutMillis(pAsyncOp);
        return ki(maxrelativetimeoutms);
    } 
    catch (...) {
        return exhandler( "OpSizeBasedTimeoutController getMaxRelativeTimeoutMillis failed", __FILE__, __LINE__);
    }
}

K dht_opsizebasedtimeoutcontroller_itemtimemillis(K opControllerq, K itemTimeMillisq)
{
    try {
        int itemTimeMillis = (int) (KInt(itemTimeMillisq));
        SKOpSizeBasedTimeoutController * pOpController = (SKOpSizeBasedTimeoutController *) ((void *) KLong(opControllerq));
        return make_symbol( pOpController->itemTimeMillis(itemTimeMillis) );
    } catch (...) {
        return exhandler( "OpSizeBasedTimeoutController itemTimeMillis failed", __FILE__, __LINE__);
    }
}

K dht_opsizebasedtimeoutcontroller_constanttimemillis(K opControllerq, K constantTimeMillisq)
{
    try {
        int constantTimeMillis = (int) (KInt(constantTimeMillisq));
        SKOpSizeBasedTimeoutController * pOpController = (SKOpSizeBasedTimeoutController *) ((void *) KLong(opControllerq));
        return make_symbol( pOpController->constantTimeMillis(constantTimeMillis) );
    } catch (...) {
        return exhandler( "OpSizeBasedTimeoutController constantTimeMillis failed", __FILE__, __LINE__);
    }
}

K dht_opsizebasedtimeoutcontroller_maxreltimeoutmillis(K opControllerq, K maxRelTimeoutMillisq)
{
    try {
        int maxRelTimeoutMillis = (int) (KInt(maxRelTimeoutMillisq));
        SKOpSizeBasedTimeoutController * pOpController = (SKOpSizeBasedTimeoutController *) ((void *) KLong(opControllerq));
        return make_symbol( pOpController->maxRelTimeoutMillis(maxRelTimeoutMillis) );
    } catch (...) {
        return exhandler( "OpSizeBasedTimeoutController maxRelTimeoutMillis failed", __FILE__, __LINE__);
    }
}

K dht_opsizebasedtimeoutcontroller_maxattempts(K opControllerq, K maxAttemptsq)
{
    try {
        int maxAttempts = (int) (KInt(maxAttemptsq));
        SKOpSizeBasedTimeoutController * pOpController = (SKOpSizeBasedTimeoutController *) ((void *) KLong(opControllerq));
        return make_symbol( pOpController->maxAttempts(maxAttempts) );
    } catch (...) {
        return exhandler( "OpSizeBasedTimeoutController maxAttempts failed", __FILE__, __LINE__);
    }
}

K dht_opsizebasedtimeoutcontroller_tostring(K opControllerq)
{
    try {
        SKOpSizeBasedTimeoutController * pOpController = (SKOpSizeBasedTimeoutController *) ((void *) KLong(opControllerq));
        std::string str = pOpController->toString() ;
        K tgt = kpn( const_cast<char*>(str.c_str()), str.size() );
        return tgt;
    } 
    catch (...) {
        return exhandler( "OpSizeBasedTimeoutController tostring failed", __FILE__, __LINE__);
    }
}

K dht_opsizebasedtimeoutcontroller_delete(K opControllerq)
{
    try {
        SKOpSizeBasedTimeoutController * pOpController = (SKOpSizeBasedTimeoutController *) ((void *) KLong(opControllerq));
        delete pOpController;
        opControllerq->j = 0;
        return _kNULL;
    } 
    catch (...) {
        exhandler( "OpSizeBasedTimeoutController delete failed", __FILE__, __LINE__);
    }
    return _kNULL;
}


// -------------------- SKSimpleTimeoutController ---------------------------------
K dht_simpletimeoutcontroller_new(K maxAttemptsq, K maxRelativeTimeoutMillisq)
{
    try {
        int maxAttempts =  (int)(Kinteger(maxAttemptsq));
        int maxRelativeTimeoutMillis =  (int)(Kinteger(maxRelativeTimeoutMillisq));
        SKSimpleTimeoutController * opController = new SKSimpleTimeoutController( maxAttempts, maxRelativeTimeoutMillis);
        return make_symbol(opController);
    } catch (...) {
        return exhandler( "SimpleTimeoutController c-tor() failed", __FILE__, __LINE__);
    }
}

K dht_simpletimeoutcontroller_parse(K defq)
{
    try {
        return make_symbol( SKSimpleTimeoutController::parse(STDSTRING(defq).c_str() ) );
    }
    catch (...) {
        return exhandler( "SimpleTimeoutController parsing failed", __FILE__, __LINE__);
    }
}

K dht_simpletimeoutcontroller_getmaxattempts(K opControllerq, K asyncOperationq)
{
    try {
        SKSimpleTimeoutController * pOpController = (SKSimpleTimeoutController *) ((void *) KLong(opControllerq));
        SKAsyncOperation * pAsyncOp = (SKAsyncOperation *) ((void *) KLong(asyncOperationq));
        int maxattempts = pOpController->getMaxAttempts(pAsyncOp);
        return ki(maxattempts);
    } 
    catch (...) {
        return exhandler( "SimpleTimeoutController getMaxAttempts failed", __FILE__, __LINE__);
    }
}

K dht_simpletimeoutcontroller_getrelativetimeoutmillisforattempt(K opControllerq, K asyncOperationq, K attemptIndexq)
{
    try {
        SKSimpleTimeoutController * pOpController = (SKSimpleTimeoutController *) ((void *) KLong(opControllerq));
        SKAsyncOperation * pAsyncOp = (SKAsyncOperation *) ((void *) KLong(asyncOperationq));
        int attemptIndex = (int) Kinteger(attemptIndexq);
        int relativetimeoutmillis = pOpController->getRelativeTimeoutMillisForAttempt(pAsyncOp, attemptIndex);
        return ki(relativetimeoutmillis);
    } 
    catch (...) {
        return exhandler( "SimpleTimeoutController getRelativeTimeoutMillisForAttempt failed", __FILE__, __LINE__);
    }
}

K dht_simpletimeoutcontroller_getmaxrelativetimeoutmillis(K opControllerq, K asyncOperationq)
{
    try {
        SKSimpleTimeoutController * pOpController = (SKSimpleTimeoutController *) ((void *) KLong(opControllerq));
        SKAsyncOperation * pAsyncOp = (SKAsyncOperation *) ((void *) KLong(asyncOperationq));
        int maxrelativetimeoutms = pOpController->getMaxRelativeTimeoutMillis(pAsyncOp);
        return ki(maxrelativetimeoutms);
    } 
    catch (...) {
        return exhandler( "SimpleTimeoutController getMaxRelativeTimeoutMillis failed", __FILE__, __LINE__);
    }
}

K dht_simpletimeoutcontroller_maxattempts(K opControllerq, K maxAttemptsq)
{
    try {
        int maxAttempts = (int) (KInt(maxAttemptsq));
        SKSimpleTimeoutController * pOpController = (SKSimpleTimeoutController *) ((void *) KLong(opControllerq));
        return make_symbol( pOpController->maxAttempts(maxAttempts) );
    } catch (...) {
        return exhandler( "SimpleTimeoutController maxAttempts failed", __FILE__, __LINE__);
    }
}

K dht_simpletimeoutcontroller_maxrelativetimeoutmillis(K opControllerq, K maxRelativeTimeoutMillisq)
{
    try {
        int maxRelativeTimeoutMillis = (int) (KInt(maxRelativeTimeoutMillisq));
        SKSimpleTimeoutController * pOpController = (SKSimpleTimeoutController *) ((void *) KLong(opControllerq));
        return make_symbol( pOpController->maxRelativeTimeoutMillis(maxRelativeTimeoutMillis) );
    } catch (...) {
        return exhandler( "SimpleTimeoutController maxRelativeTimeoutMillis failed", __FILE__, __LINE__);
    }
}

K dht_simpletimeoutcontroller_tostring(K opControllerq)
{
    try {
        SKSimpleTimeoutController * pOpController = (SKSimpleTimeoutController *) ((void *) KLong(opControllerq));
        std::string str = pOpController->toString() ;
        K tgt = kpn( const_cast<char*>(str.c_str()), str.size() );
        return tgt;
    } 
    catch (...) {
        return exhandler( "SimpleTimeoutController tostring failed", __FILE__, __LINE__);
    }
}

K dht_simpletimeoutcontroller_delete(K opControllerq)
{
    try {
        SKSimpleTimeoutController * pOpController = (SKSimpleTimeoutController *) ((void *) KLong(opControllerq));
        delete pOpController;
        opControllerq->j = 0;
        return _kNULL;
    } 
    catch (...) {
        exhandler( "SimpleTimeoutController delete failed", __FILE__, __LINE__);
    }
    return _kNULL;
}


// -------------------- SKWaitForTimeoutController ---------------------------------
K dht_waitfortimeoutcontroller_new(K dummy)
{
    try {
        SKWaitForTimeoutController * opController = new SKWaitForTimeoutController( );
        return make_symbol(opController);
    } catch (...) {
        return exhandler( "WaitForTimeoutController c-tor failed", __FILE__, __LINE__);
    }
}

K dht_waitfortimeoutcontroller_new1(K internalRetryIntervalSecondsq)
{
    try {
        int internalRetryIntervalSeconds =  (int)(Kinteger(internalRetryIntervalSecondsq));
        SKWaitForTimeoutController * opController = new SKWaitForTimeoutController( internalRetryIntervalSeconds);
        return make_symbol(opController);
    } catch (...) {
        return exhandler( "WaitForTimeoutController c-tor() failed", __FILE__, __LINE__);
    }
}

K dht_waitfortimeoutcontroller_getmaxattempts(K opControllerq, K asyncOperationq)
{
    try {
        SKWaitForTimeoutController * pOpController = (SKWaitForTimeoutController *) ((void *) KLong(opControllerq));
        SKAsyncOperation * pAsyncOp = (SKAsyncOperation *) ((void *) KLong(asyncOperationq));
        int maxattempts = pOpController->getMaxAttempts(pAsyncOp);
        return ki(maxattempts);
    } 
    catch (...) {
        return exhandler( "WaitForTimeoutController getMaxAttempts failed", __FILE__, __LINE__);
    }
}

K dht_waitfortimeoutcontroller_getrelativetimeoutmillisforattempt(K opControllerq, K asyncOperationq, K attemptIndexq)
{
    try {
        SKWaitForTimeoutController * pOpController = (SKWaitForTimeoutController *) ((void *) KLong(opControllerq));
        SKAsyncOperation * pAsyncOp = (SKAsyncOperation *) ((void *) KLong(asyncOperationq));
        int attemptIndex = (int) Kinteger(attemptIndexq);
        int relativetimeoutmillis = pOpController->getRelativeTimeoutMillisForAttempt(pAsyncOp, attemptIndex);
        return ki(relativetimeoutmillis);
    } 
    catch (...) {
        return exhandler( "WaitForTimeoutController getRelativeTimeoutMillisForAttempt failed", __FILE__, __LINE__);
    }
}

K dht_waitfortimeoutcontroller_getmaxrelativetimeoutmillis(K opControllerq, K asyncOperationq)
{
    try {
        SKWaitForTimeoutController * pOpController = (SKWaitForTimeoutController *) ((void *) KLong(opControllerq));
        SKAsyncOperation * pAsyncOp = (SKAsyncOperation *) ((void *) KLong(asyncOperationq));
        int maxrelativetimeoutms = pOpController->getMaxRelativeTimeoutMillis(pAsyncOp);
        return ki(maxrelativetimeoutms);
    } 
    catch (...) {
        return exhandler( "WaitForTimeoutController getMaxRelativeTimeoutMillis failed", __FILE__, __LINE__);
    }
}

K dht_waitfortimeoutcontroller_tostring(K opControllerq)
{
    try {
        SKWaitForTimeoutController * pOpController = (SKWaitForTimeoutController *) ((void *) KLong(opControllerq));
        std::string str = pOpController->toString() ;
        K tgt = kpn( const_cast<char*>(str.c_str()), str.size() );
        return tgt;
    } 
    catch (...) {
        return exhandler( "WaitForTimeoutController tostring failed", __FILE__, __LINE__);
    }
}

K dht_waitfortimeoutcontroller_delete(K opControllerq)
{
    try {
        SKWaitForTimeoutController * pOpController = (SKWaitForTimeoutController *) ((void *) KLong(opControllerq));
        delete pOpController;
        opControllerq->j = 0;
        return _kNULL;
    } 
    catch (...) {
        exhandler( "WaitForTimeoutController delete failed", __FILE__, __LINE__);
    }
    return _kNULL;
}


// -------------------- SKOpTimeoutController ---------------------------------
K dht_optimeoutcontroller_getmaxattempts(K opControllerq, K asyncOperationq)
{
    try {
        SKOpTimeoutController * pOpController = (SKOpTimeoutController *) ((void *) KLong(opControllerq));
        SKAsyncOperation * pAsyncOp = (SKAsyncOperation *) ((void *) KLong(asyncOperationq));
        int maxattempts = pOpController->getMaxAttempts(pAsyncOp);
        return ki(maxattempts);
    } 
    catch (...) {
        return exhandler( "OpTimeoutController getMaxAttempts failed", __FILE__, __LINE__);
    }
}

K dht_optimeoutcontroller_getrelativetimeoutmillisforattempt(K opControllerq, K asyncOperationq, K attemptIndexq)
{
    try {
        SKOpTimeoutController * pOpController = (SKOpTimeoutController *) ((void *) KLong(opControllerq));
        SKAsyncOperation * pAsyncOp = (SKAsyncOperation *) ((void *) KLong(asyncOperationq));
        int attemptIndex = (int) Kinteger(attemptIndexq);
        int relativetimeoutmillis = pOpController->getRelativeTimeoutMillisForAttempt(pAsyncOp, attemptIndex);
        return ki(relativetimeoutmillis);
    } 
    catch (...) {
        return exhandler( "OpTimeoutController getRelativeTimeoutMillisForAttempt failed", __FILE__, __LINE__);
    }
}

K dht_optimeoutcontroller_getmaxrelativetimeoutmillis(K opControllerq, K asyncOperationq)
{
    try {
        SKOpTimeoutController * pOpController = (SKOpTimeoutController *) ((void *) KLong(opControllerq));
        SKAsyncOperation * pAsyncOp = (SKAsyncOperation *) ((void *) KLong(asyncOperationq));
        int maxrelativetimeoutms = pOpController->getMaxRelativeTimeoutMillis(pAsyncOp);
        return ki(maxrelativetimeoutms);
    } 
    catch (...) {
        return exhandler( "OpTimeoutController getMaxRelativeTimeoutMillis failed", __FILE__, __LINE__);
    }
}

K dht_optimeoutcontroller_delete(K opControllerq)
{
    try {
        SKOpTimeoutController * pOpController = (SKOpTimeoutController *) ((void *) KLong(opControllerq));
        delete pOpController;
        opControllerq->j = 0;
        return _kNULL;
    } 
    catch (...) {
        exhandler( "OpTimeoutController delete failed", __FILE__, __LINE__);
    }
    return _kNULL;
}

// -------------------- SKSessionEstablishmentTimeoutController ---------------------------------//
K dht_sessiontimeoutcontroller_getmaxattempts(K sessControllerq, K psessoptsq)
{
    try {
        SKSessionEstablishmentTimeoutController * pSessController = (SKSessionEstablishmentTimeoutController *) ((void *) KLong(sessControllerq));
        SKSessionOptions * pSessOpts = (SKSessionOptions *) ((void *) KLong(psessoptsq));
        int maxattempts = pSessController->getMaxAttempts(pSessOpts);
        return ki(maxattempts);
    } 
    catch (...) {
        return exhandler( "SessionEstablishmentTimeoutController getMaxAttempts failed", __FILE__, __LINE__);
    }
}

K dht_sessiontimeoutcontroller_getrelativetimeoutmillisforattempt(K sessControllerq, K psessoptsq, K attemptIndexq)
{
    try {
        SKSessionEstablishmentTimeoutController * pSessController = (SKSessionEstablishmentTimeoutController *) ((void *) KLong(sessControllerq));
        SKSessionOptions * pSessOpts = (SKSessionOptions *) ((void *) KLong(psessoptsq));
        int attemptIndex = (int) Kinteger(attemptIndexq);
        int relativetimeoutmillis = pSessController->getRelativeTimeoutMillisForAttempt(pSessOpts, attemptIndex);
        return ki(relativetimeoutmillis);
    } 
    catch (...) {
        return exhandler( "OpTimeoutController getRelativeTimeoutMillisForAttempt failed", __FILE__, __LINE__);
    }
}

K dht_sessiontimeoutcontroller_getmaxrelativetimeoutmillis(K sessControllerq, K psessoptsq)
{
    try {
        SKSessionEstablishmentTimeoutController * pSessController = (SKSessionEstablishmentTimeoutController *) ((void *) KLong(sessControllerq));
        SKSessionOptions * pSessOpts = (SKSessionOptions *) ((void *) KLong(psessoptsq));
        int maxrelativetimeoutms = pSessController->getMaxRelativeTimeoutMillis(pSessOpts);
        return ki(maxrelativetimeoutms);
    } 
    catch (...) {
        return exhandler( "OpTimeoutController getMaxRelativeTimeoutMillis failed", __FILE__, __LINE__);
    }
}

K dht_sessiontimeoutcontroller_delete(K sessControllerq)
{
    try {
        SKSessionEstablishmentTimeoutController * pSessController = (SKSessionEstablishmentTimeoutController *) ((void *) KLong(sessControllerq));
        delete pSessController;
        sessControllerq->j = 0;
        return _kNULL;
    } 
    catch (...) {
        exhandler( "OpTimeoutController delete failed", __FILE__, __LINE__);
    }
    return _kNULL;
}


// -------------------- SKSimpleSessionEstablishmentTimeoutController ---------------------------------
K dht_simplesessiontimeoutcontroller_new(K maxAttemptsq, K attemptRelativeTimeoutMillisq, K maxRelativeTimeoutMillisq)
{
    try {
        int maxAttempts =  (int)(Kinteger(maxAttemptsq));
        int attemptRelativeTimeoutMillis =  (int)(Kinteger(attemptRelativeTimeoutMillisq));
        int maxRelativeTimeoutMillis =  (int)(Kinteger(maxRelativeTimeoutMillisq));
        SKSimpleSessionEstablishmentTimeoutController * opController = new SKSimpleSessionEstablishmentTimeoutController( maxAttempts, attemptRelativeTimeoutMillis, maxRelativeTimeoutMillis);
        return make_symbol(opController);
    } catch (...) {
        return exhandler( "SimpleSessionEstablishmentTimeoutController c-tor() failed", __FILE__, __LINE__);
    }
}

K dht_simplesessiontimeoutcontroller_parse(K def)
{
    try {
        return make_symbol( SKSimpleSessionEstablishmentTimeoutController::parse(STDSTRING(def).c_str() ) );
    }
    catch (...) {
        return exhandler( "SimpleSessionEstablishmentTimeoutController parsing failed", __FILE__, __LINE__);
    }
}

K dht_simplesessiontimeoutcontroller_getmaxattempts(K simpSessControllerq, K pSessOptsq)
{
    try {
        SKSimpleSessionEstablishmentTimeoutController * pOpController = (SKSimpleSessionEstablishmentTimeoutController *) ((void *) KLong(simpSessControllerq));
        SKSessionOptions * sessOpts = (SKSessionOptions *) ((void *) KLong(pSessOptsq));
        int maxattempts = pOpController->getMaxAttempts(sessOpts);
        return ki(maxattempts);
    } 
    catch (...) {
        return exhandler( "SimpleSessionEstablishmentTimeoutController getMaxAttempts failed", __FILE__, __LINE__);
    }
}

K dht_simplesessiontimeoutcontroller_getrelativetimeoutmillisforattempt(K simpSessControllerq, K pSessOptsq, K attemptIndexq)
{
    try {
        SKSimpleSessionEstablishmentTimeoutController * pOpController = (SKSimpleSessionEstablishmentTimeoutController *) ((void *) KLong(simpSessControllerq));
        SKSessionOptions * sessOpts = (SKSessionOptions *) ((void *) KLong(pSessOptsq));
        int attemptIndex = (int) Kinteger(attemptIndexq);
        int relativetimeoutmillis = pOpController->getRelativeTimeoutMillisForAttempt(sessOpts, attemptIndex);
        return ki(relativetimeoutmillis);
    } 
    catch (...) {
        return exhandler( "SimpleSessionEstablishmentTimeoutController getRelativeTimeoutMillisForAttempt failed", __FILE__, __LINE__);
    }
}

K dht_simplesessiontimeoutcontroller_getmaxrelativetimeoutmillis(K simpSessControllerq, K pSessOptsq)
{
    try {
        SKSimpleSessionEstablishmentTimeoutController * pOpController = (SKSimpleSessionEstablishmentTimeoutController *) ((void *) KLong(simpSessControllerq));
        SKSessionOptions * sessOpts = (SKSessionOptions *) ((void *) KLong(pSessOptsq));
        int maxrelativetimeoutms = pOpController->getMaxRelativeTimeoutMillis(sessOpts);
        return ki(maxrelativetimeoutms);
    } 
    catch (...) {
        return exhandler( "SimpleSessionEstablishmentTimeoutController getMaxRelativeTimeoutMillis failed", __FILE__, __LINE__);
    }
}

K dht_simplesessiontimeoutcontroller_maxattempts(K simpSessControllerq, K maxAttemptsq)
{
    try {
        int maxAttempts = (int) (KInt(maxAttemptsq));
        SKSimpleSessionEstablishmentTimeoutController * pOpController = (SKSimpleSessionEstablishmentTimeoutController *) ((void *) KLong(simpSessControllerq));
        return make_symbol( pOpController->maxAttempts(maxAttempts) );
    } catch (...) {
        return exhandler( "SimpleSessionEstablishmentTimeoutController maxAttempts failed", __FILE__, __LINE__);
    }
}

K dht_simplesessiontimeoutcontroller_maxrelativetimeoutmillis(K simpSessControllerq, K maxRelativeTimeoutMillisq)
{
    try {
        int maxRelativeTimeoutMillis = (int) (KInt(maxRelativeTimeoutMillisq));
        SKSimpleSessionEstablishmentTimeoutController * pOpController = (SKSimpleSessionEstablishmentTimeoutController *) ((void *) KLong(simpSessControllerq));
        return make_symbol( pOpController->maxRelativeTimeoutMillis(maxRelativeTimeoutMillis) );
    } catch (...) {
        return exhandler( "SimpleSessionEstablishmentTimeoutController maxRelativeTimeoutMillis failed", __FILE__, __LINE__);
    }
}

K dht_simplesessiontimeoutcontroller_attemptrelativetimeoutmillis(K simpSessControllerq, K attemptRelativeTimeoutMillisq)
{
    try {
        int attemptRelativeTimeoutMillis = (int) (KInt(attemptRelativeTimeoutMillisq));
        SKSimpleSessionEstablishmentTimeoutController * pOpController = (SKSimpleSessionEstablishmentTimeoutController *) ((void *) KLong(simpSessControllerq));
        return make_symbol( pOpController->attemptRelativeTimeoutMillis(attemptRelativeTimeoutMillis) );
    } catch (...) {
        return exhandler( "SimpleSessionEstablishmentTimeoutController attemptRelativeTimeoutMillis failed", __FILE__, __LINE__);
    }
}
// SKSimpleSessionEstablishmentTimeoutController * attemptRelativeTimeoutMillis(int attemptRelativeTimeoutMillis);

K dht_simplesessiontimeoutcontroller_tostring(K simpSessControllerq)
{
    try {
        SKSimpleSessionEstablishmentTimeoutController * pOpController = (SKSimpleSessionEstablishmentTimeoutController *) ((void *) KLong(simpSessControllerq));
        std::string str = pOpController->toString() ;
        K tgt = kpn( const_cast<char*>(str.c_str()), str.size() );
        return tgt;
    } 
    catch (...) {
        return exhandler( "SimpleSessionEstablishmentTimeoutController tostring failed", __FILE__, __LINE__);
    }
}
// string toString();

K dht_simplesessiontimeoutcontroller_delete(K simpSessControllerq)
{
    try {
        SKSimpleSessionEstablishmentTimeoutController * pSimpSessController = (SKSimpleSessionEstablishmentTimeoutController *) ((void *) KLong(simpSessControllerq));
        delete pSimpSessController;
        simpSessControllerq->j = 0;
        return _kNULL;
    } 
    catch (...) {
        exhandler( "SimpleSessionEstablishmentTimeoutController delete failed", __FILE__, __LINE__);
    }
    return _kNULL;
}
// virtual ~SKSimpleSessionEstablishmentTimeoutController();



//------------------------ SKAsyncSingleValueRetrieval -------------------------

//------------------------ SKSimpleNamedStopwatch ------------------------------
//------------------------ SKSimpleStopwatch -----------------------------------
//------------------------ SKStopwatch -----------------------------------------
//------------------------ SKTimerDrivenTimeSource -----------------------------
//------------------------ SKRelNanosAbsMillisTimeSource -----------------------
//------------------------ SKAbsMillisTimeSource -------------------------------
//------------------------ SKSystemTimeSource ----------------------------------
//------------------------ SKAbsMillisVersionProvider --------------------------
//------------------------ SKRelNanosTimeSource --------------------------------
//------------------------ SKClientDHTConfiguration ----------------------------
//------------------------ SKClientDHTConfigurationProvider --------------------
//------------------------ SKGridConfiguration ---------------------------------
//------------------------ SKNodeID --------------------------------------------
// 


} // extern "C"


