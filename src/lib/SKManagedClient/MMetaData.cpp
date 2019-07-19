#include "StdAfx.h"
#include "MMetaData.h"

#include <string>
using namespace std;
#include <stdint.h>
#include <stdlib.h>
#include "skconstants.h"
#include "skbasictypes.h"
#include "SKMetaData.h"


namespace SKManagedClient {



MMetaData::~MMetaData(void)
{
    this->!MMetaData();
}

MMetaData::!MMetaData(void)
{
    if(pImpl) 
    {
        delete (SKMetaData*)pImpl ; 
        pImpl = NULL;
    }
}

//protected
MMetaData::MMetaData()
{
    pImpl = NULL;
}

//internal
MMetaData::MMetaData(SKMetaData_M ^ metaData)
{
    pImpl = metaData->pMetaData;
}

SKMetaData_M ^ MMetaData::getPImpl()
{
    SKMetaData_M ^ metaData = gcnew SKMetaData_M;
    metaData->pMetaData = pImpl;
    return metaData;
}

//public
Int32 MMetaData::getStoredLength()
{
    return ((SKMetaData*)pImpl)->getStoredLength();
}

Int32 MMetaData::getUncompressedLength()
{
    return ((SKMetaData*)pImpl)->getUncompressedLength();
}

Int64 MMetaData::getVersion()
{
    Int64 ver  = ((SKMetaData*)pImpl)->getVersion();
    return ver;
}

Int64 MMetaData::getCreationTime()
{
    Int64 creationTime = ((SKMetaData*)pImpl)->getCreationTime();
    return creationTime;
}

MValueCreator ^ MMetaData::getCreator()
{
    SKValueCreator * pCreator = ((SKMetaData*)pImpl)->getCreator();
    SKValueCreator_M ^ creator = gcnew SKValueCreator_M;
    creator->pValueCreator  = pCreator;
    MValueCreator ^ vc = gcnew MValueCreator(creator);
    return vc;
}

String ^ MMetaData::getUserData()
{
    SKVal * val =  ((SKMetaData*)pImpl)->getUserData();
    String ^ str = gcnew String( (char*)(val->m_pVal), 0, val->m_len );
    sk_destroy_val( &val );
    return str;
}

String ^ MMetaData::toString(bool labeled)
{
    char * val =  ((SKMetaData*)pImpl)->toString(labeled);
    String ^ str = gcnew String( val );
    free( val );
    return str;
}

String ^ MMetaData::getChecksum()
{
    SKVal * val =  ((SKMetaData*)pImpl)->getChecksum();

    //we convert it into Hexadecimal string
    char * pStrRepresentation = (char*)malloc( sizeof(char) * (val->m_len * 2 + 1) );
    char * pTmp = pStrRepresentation;
    char * pVal = (char*)val->m_pVal;
    for( unsigned int i = 0; i < val->m_len; i++ ) {
        sprintf( pTmp, "%02X", *pVal++ );
        pTmp += 2;
    }
    pTmp='\0';

    String ^ str = gcnew String( pStrRepresentation );

    free(pStrRepresentation); 
    sk_destroy_val( &val );

    return str;
}

SKCompression_M MMetaData::getCompression() 
{
    SKCompression::SKCompression compr =  ((SKMetaData*)pImpl)->getCompression();
    return (SKCompression_M) compr;
}

SKChecksumType_M MMetaData::getChecksumType() 
{
    SKChecksumType::SKChecksumType chksm =  ((SKMetaData*)pImpl)->getChecksumType();
    return (SKChecksumType_M) chksm;

}


}