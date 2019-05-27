#include "StdAfx.h"
#include "MSecondaryTarget.h"

#include <string>
//#include <cstddef>
//#include "skbasictypes.h"
#include "SKSecondaryTarget.h"

using namespace System;
using namespace System::Net;
using namespace System::Runtime::InteropServices;

namespace SKManagedClient {

MSecondaryTarget::~MSecondaryTarget()
{
    this->!MSecondaryTarget();
}

MSecondaryTarget::!MSecondaryTarget()
{
    if(pImpl)
    {
        delete (SKSecondaryTarget*)pImpl ;
        pImpl = NULL;
    }
}

MSecondaryTarget::MSecondaryTarget(SKSecondaryTargetType_M type, String ^ target)
{
    char* key_n = (char*)(void*)Marshal::StringToHGlobalAnsi(target);
    SKSecondaryTarget * pTarget = new SKSecondaryTarget( (SKSecondaryTargetType)type, std::string(key_n) );
    pImpl = pTarget;
    Marshal::FreeHGlobal(System::IntPtr(key_n));
}

MSecondaryTarget::MSecondaryTarget(SKSecondaryTarget_M ^ secondaryTarget)
{
    pImpl = secondaryTarget->pSecondaryTarget;
}
SKSecondaryTarget_M ^ MSecondaryTarget::getPImpl()
{
    SKSecondaryTarget_M ^ secondaryTarget = gcnew SKSecondaryTarget_M;
    secondaryTarget->pSecondaryTarget = pImpl;
    return secondaryTarget;
}

SKSecondaryTargetType_M MSecondaryTarget::getType() 
{
    SKSecondaryTargetType targetType = ((SKSecondaryTarget*)pImpl)->getType();
    return (SKSecondaryTargetType_M) targetType;
}

String ^ MSecondaryTarget::getTarget() 
{
    char * targetName = ((SKSecondaryTarget*)pImpl)->getTarget();
    String ^ str = gcnew String( targetName);
    free(targetName);
    return str;
}



}
