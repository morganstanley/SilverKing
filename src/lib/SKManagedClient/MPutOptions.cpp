#include "StdAfx.h"
#include "MPutOptions.h"
#include "MOpTimeoutController.h"
#include "MSecondaryTarget.h"

#include <string>
using namespace std;
#include "skconstants.h"
#include "skbasictypes.h"
#include "SKPutOptions.h"
#include "SKOpTimeoutController.h"
#include "SKSecondaryTarget.h"

using namespace System::Runtime::InteropServices;

namespace SKManagedClient {

MPutOptions ^ MPutOptions::parse(String ^ def)
{
	SKPutOptions * ppo = SKPutOptions::parse((char *) System::Runtime::InteropServices::Marshal::StringToHGlobalAnsi(def).ToPointer());
	SKPutOptions_M ^ po = gcnew SKPutOptions_M;
	po->pPutOptions = ppo;
	return gcnew MPutOptions(po);
}

MPutOptions::!MPutOptions()
{
	if(pImpl) 
	{
		delete (SKPutOptions*)pImpl ;
		pImpl = NULL;
	}
}

MPutOptions::~MPutOptions()
{
	this->!MPutOptions();
}

MPutOptions::MPutOptions(MOpTimeoutController ^ opTimeoutController, SKCompression_M compression, 
			SKChecksumType_M checksumType, bool checksumCompressedValues, Int64 version, 
			HashSet<MSecondaryTarget^> ^ secondaryTargets, String ^ userData)
{
	SKOpTimeoutController * pOpTimeoutCtrl = (SKOpTimeoutController *) (opTimeoutController->getPImpl()->pOpTimeoutController);
	SKVal * pUserData = NULL;
	//try {

		std::set<SKSecondaryTarget*> * pTgtSet = new std::set<SKSecondaryTarget*>();
		System::Collections::Generic::IEnumerator<MSecondaryTarget^> ^ hse = secondaryTargets->GetEnumerator();
		while(hse->MoveNext()){
			MSecondaryTarget^ tgt = hse->Current;
			SKSecondaryTarget * pTgtg = (SKSecondaryTarget *) (tgt->getPImpl()->pSecondaryTarget);
			pTgtSet->insert(pTgtg);
		}

		pUserData = sk_create_val();
		char* pval = (char*)(void*)Marshal::StringToHGlobalAnsi(userData);
		int valLen = userData->Length;
		sk_set_val_zero_copy(pUserData, valLen, (void *) pval);

		SKPutOptions * pPutOpt = new SKPutOptions( 
			pOpTimeoutCtrl,
			(SKCompression::SKCompression) compression,
			(SKChecksumType::SKChecksumType) checksumType,
			checksumCompressedValues,
			version,
			pTgtSet,
			pUserData 
		);
		pImpl = pPutOpt;
	//}
	//finally {
		if(pUserData)
		{
			pUserData->m_pVal = NULL; //remove ref to pval as it should be freed in FreeHGlobal
			pUserData->m_len = 0;
			sk_destroy_val( & pUserData);
		}
		delete pTgtSet;
		Marshal::FreeHGlobal(System::IntPtr(pval));
	//}
}

MPutOptions::MPutOptions(SKPutOptions_M ^ pPutOptsImpl)
{
	pImpl = pPutOptsImpl->pPutOptions;
}

SKPutOptions_M ^ MPutOptions::getPImpl()
{
	SKPutOptions_M ^ opt = gcnew SKPutOptions_M;
	opt->pPutOptions = pImpl;
	return opt;
}


//methods
MPutOptions ^ MPutOptions::compression(SKCompression_M compression)
{
	((SKPutOptions*)pImpl)->compression( (SKCompression::SKCompression) compression);
	return this;
}

MPutOptions ^ MPutOptions::version(Int64 version)
{
	((SKPutOptions*)pImpl)->version( version);
	return this;
}

MPutOptions ^ MPutOptions::userData(String ^ userData)
{
	SKVal * pUserData = NULL;
	try {
		pUserData = sk_create_val();
		char* pval = (char*)(void*)Marshal::StringToHGlobalAnsi(userData);
		int valLen = userData->Length;
		sk_set_val_zero_copy(pUserData, valLen, (void *) pval); //share the pval

		((SKPutOptions*)pImpl)->userData( pUserData );
	}
	finally 
	{
		if(pUserData) 
		{
			pUserData->m_pVal = NULL; //remove ref to pval as it should be freed in FreeHGlobal
			pUserData->m_len = 0;
			sk_destroy_val( & pUserData);
		}

		Marshal::FreeHGlobal(System::IntPtr(pUserData));
	}

	return this;
}

MPutOptions ^ MPutOptions::checksumType(SKChecksumType_M checksumType)
{
	((SKPutOptions*)pImpl)->checksumType( (SKChecksumType::SKChecksumType) checksumType);
	return this;
}

bool MPutOptions::getChecksumCompressedValues()
{
	return ((SKPutOptions*)pImpl)->getChecksumCompressedValues();
}

SKCompression_M MPutOptions::getCompression()
{
	SKCompression::SKCompression compr =  ((SKPutOptions*)pImpl)->getCompression();
	return (SKCompression_M) compr;
}

SKChecksumType_M MPutOptions::getChecksumType() 
{
	SKChecksumType::SKChecksumType chksumType =  ((SKPutOptions*)pImpl)->getChecksumType();
	return (SKChecksumType_M) chksumType;
}

Int64 MPutOptions::getVersion()
{
	Int64 ver = ((SKPutOptions*)pImpl)->getVersion();
	return ver;
}

String ^ MPutOptions::getUserData()
{
	SKVal * pUserData  =  ((SKPutOptions*)pImpl)->getUserData();
	String ^ str = gcnew String( (char *)pUserData->m_pVal, 0, pUserData->m_len);
	sk_destroy_val( & pUserData);
	return str;
}

bool MPutOptions::equals(Object ^ other)
{
	if( this->GetType() != MPutOptions::typeid || other->GetType() != this->GetType())
		return false;
	return ((SKPutOptions*)pImpl)->equals((SKPutOptions*)(((MPutOptions^)other)->getPImpl()->pPutOptions));
}

String ^ MPutOptions::toString() 
{
	std::string stdstr =  ((SKPutOptions*)pImpl)->toString();
	String ^ str = gcnew String( stdstr.c_str(), 0, stdstr.size() );
	return str;
}

//MPutOptions ^ MPutOptions::checksumCompressedValues(bool checksumCompressedValues );

MPutOptions ^ MPutOptions::opTimeoutController(MOpTimeoutController ^ opTimeoutController){
	((SKPutOptions*)pImpl)->opTimeoutController( (SKOpTimeoutController*)(opTimeoutController->getPImpl()->pOpTimeoutController));
	return this;
}

MPutOptions ^ MPutOptions::secondaryTargets(HashSet<MSecondaryTarget^> ^ secondaryTargets){
	std::set<SKSecondaryTarget*> * pTgtSet = new std::set<SKSecondaryTarget*>();
	System::Collections::Generic::IEnumerator<MSecondaryTarget^> ^ hse = secondaryTargets->GetEnumerator();
	while(hse->MoveNext()){
		MSecondaryTarget^ tgt = hse->Current;
		SKSecondaryTarget * pTgtg = (SKSecondaryTarget *) (tgt->getPImpl()->pSecondaryTarget);
		pTgtSet->insert(pTgtg);
	}
	((SKPutOptions*)pImpl)->secondaryTargets( pTgtSet );
	delete pTgtSet;
	return this;
}

MPutOptions ^ MPutOptions::secondaryTargets(MSecondaryTarget ^ secondaryTarget){
	((SKPutOptions*)pImpl)->secondaryTargets( (SKSecondaryTarget*)(secondaryTarget->getPImpl()->pSecondaryTarget));
	return this;
}

HashSet<MSecondaryTarget^> ^ MPutOptions::getSecondaryTargets(){
	std::set<SKSecondaryTarget*> * pTgts = ((SKPutOptions*)pImpl)->getSecondaryTargets();
	std::set<SKSecondaryTarget*>::iterator it;
	HashSet< MSecondaryTarget^ > ^ secondaryTargets = gcnew HashSet< MSecondaryTarget^ >();
	for(it=pTgts->begin(); it!=pTgts->end(); it++)
	{
		SKSecondaryTarget * pTarget = *it;
		SKSecondaryTarget_M ^ target = gcnew SKSecondaryTarget_M;
		target->pSecondaryTarget = pTarget;
		MSecondaryTarget ^ secondaryTarget = gcnew MSecondaryTarget(target);
		secondaryTargets->Add(secondaryTarget);
	}
	delete pTgts; pTgts = NULL;

	return secondaryTargets;
}


}

