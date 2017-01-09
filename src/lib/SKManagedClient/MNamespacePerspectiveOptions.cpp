#include "StdAfx.h"
#include "MNamespacePerspectiveOptions.h"
#include "MVersionProvider.h"
#include "MPutOptions.h"
#include "MGetOptions.h"
#include "MWaitOptions.h"

#include <string>
using namespace std;
#include "SKNamespacePerspectiveOptions.h"
#include "skconstants.h"
#include "SKVersionProvider.h"
#include "SKPutOptions.h"
#include "SKGetOptions.h"
#include "SKWaitOptions.h"

namespace SKManagedClient {


	//Class<K> MNamespacePerspectiveOptions::getKeyClass();
	//Class<V> MNamespacePerspectiveOptions::getValueClass();

	MNamespacePerspectiveOptions ^ MNamespacePerspectiveOptions::parse(System::String ^ def) 
	{
		SKNamespacePerspectiveOptions * pNso = ((SKNamespacePerspectiveOptions *)pImpl)->parse((char *) System::Runtime::InteropServices::Marshal::StringToHGlobalAnsi(def).ToPointer());
		SKNamespacePerspectiveOptions_M ^ nso = gcnew SKNamespacePerspectiveOptions_M;
		nso->pNspOptions = pNso;
		return gcnew MNamespacePerspectiveOptions(nso);
	}

	MNamespacePerspectiveOptions::!MNamespacePerspectiveOptions() 
	{ 
		if(pImpl) 
		{
			delete (SKNamespacePerspectiveOptions*) pImpl; 
			pImpl = NULL;
		}
	} 

	MNamespacePerspectiveOptions::~MNamespacePerspectiveOptions() { 
		this->!MNamespacePerspectiveOptions(); 
	} 

	MNamespacePerspectiveOptions::MNamespacePerspectiveOptions(SKNamespacePerspectiveOptions_M ^  pNspOpt)
	{
		pImpl = pNspOpt->pNspOptions;
	}

	MNamespacePerspectiveOptions::MNamespacePerspectiveOptions(/*KeyClass k, ValueClass v, */ SKKeyDigestType_M keyDigestType, 
			MPutOptions ^ defaultPutOptions, MGetOptions ^ defaultGetOptions, 
			MWaitOptions ^ defaultWaitOptions, MVersionProvider ^ defaultVersionProvider)
	{
		SKPutOptions * pPutOpt = (SKPutOptions*) (defaultPutOptions->getPImpl()->pPutOptions);
		SKGetOptions * pGetOpt = (SKGetOptions*) (defaultGetOptions->getPImpl()->pGetOptions);
		SKWaitOptions * pWaitOpt = (SKWaitOptions*) (defaultWaitOptions->getPImpl()->pWaitOptions);
		SKVersionProvider * pVerProvider = (SKVersionProvider*) (defaultVersionProvider->getPImpl()->pVersionProvider);

		SKNamespacePerspectiveOptions * pOpt = new SKNamespacePerspectiveOptions( /*KeyClass k, ValueClass v,*/
										 (SKKeyDigestType::SKKeyDigestType) keyDigestType , pPutOpt, pGetOpt, 
										 pWaitOpt, pVerProvider);
		pImpl = (void * ) pOpt;
	}

	SKNamespacePerspectiveOptions_M ^ MNamespacePerspectiveOptions::getPImpl() 
	{
		SKNamespacePerspectiveOptions_M ^ opt = gcnew SKNamespacePerspectiveOptions_M;
		opt->pNspOptions = pImpl;
		return opt;
	}


	SKKeyDigestType_M MNamespacePerspectiveOptions::getKeyDigestType()
	{
		return (SKKeyDigestType_M) ((SKNamespacePerspectiveOptions*)pImpl)->getKeyDigestType();
	}
	
	MPutOptions ^ MNamespacePerspectiveOptions::getDefaultPutOptions()
	{
		SKPutOptions * pPut = ((SKNamespacePerspectiveOptions*)pImpl)->getDefaultPutOptions();
		SKPutOptions_M ^ put_m = gcnew SKPutOptions_M;
		put_m->pPutOptions = pPut;
		return gcnew MPutOptions(put_m);
	}

	MGetOptions ^ MNamespacePerspectiveOptions::getDefaultGetOptions()
	{
		SKGetOptions * pGet = ((SKNamespacePerspectiveOptions*)pImpl)->getDefaultGetOptions();
		SKGetOptions_M ^ get_m = gcnew SKGetOptions_M;
		get_m->pGetOptions = pGet;
		return gcnew MGetOptions(get_m);
	}

	MWaitOptions ^ MNamespacePerspectiveOptions::getDefaultWaitOptions()
	{
		SKWaitOptions * pWait = ((SKNamespacePerspectiveOptions*)pImpl)->getDefaultWaitOptions();
		SKWaitOptions_M ^ wait = gcnew SKWaitOptions_M;
		wait->pWaitOptions = pWait;
		return gcnew MWaitOptions(wait);
	}

	MVersionProvider ^ MNamespacePerspectiveOptions::getDefaultVersionProvider()
	{
		SKVersionProvider * pVerProvider = ((SKNamespacePerspectiveOptions*)pImpl)->getDefaultVersionProvider();
		SKVersionProvider_M ^ vprovider_m = gcnew SKVersionProvider_M;
		vprovider_m->pVersionProvider = pVerProvider;
		return gcnew MVersionProvider(vprovider_m);
	}
	

	MNamespacePerspectiveOptions ^ MNamespacePerspectiveOptions::keyDigestType(SKKeyDigestType_M keyDigestType)
	{
		SKNamespacePerspectiveOptions* pNspo = ((SKNamespacePerspectiveOptions*)pImpl)->keyDigestType( (SKKeyDigestType::SKKeyDigestType) keyDigestType);
		this->pImpl = (void *) pNspo;
		return this;
	}

	MNamespacePerspectiveOptions ^ MNamespacePerspectiveOptions::defaultPutOptions(MPutOptions ^ defaultPutOptions)
	{
		SKNamespacePerspectiveOptions* pNspo = ((SKNamespacePerspectiveOptions*)pImpl)->defaultPutOptions( 
			(SKPutOptions*) (defaultPutOptions->getPImpl()->pPutOptions)
		);
		this->pImpl = (void *) pNspo;
		return this;
	}

	MNamespacePerspectiveOptions ^ MNamespacePerspectiveOptions::defaultGetOptions(MGetOptions ^ defaultGetOptions)
	{
		SKNamespacePerspectiveOptions* pNspo = ((SKNamespacePerspectiveOptions*)pImpl)->defaultGetOptions( 
			(SKGetOptions*) (defaultGetOptions->getPImpl()->pGetOptions)
		);
		this->pImpl = (void *) pNspo;
		return this;
	}
	
	MNamespacePerspectiveOptions ^ MNamespacePerspectiveOptions::defaultWaitOptions(MWaitOptions ^ defaultWaitOptions)
	{
		SKNamespacePerspectiveOptions* pNspo = ((SKNamespacePerspectiveOptions*)pImpl)->defaultWaitOptions( 
			(SKWaitOptions*) (defaultWaitOptions->getPImpl()->pWaitOptions)
		);
		this->pImpl = (void *) pNspo;
		return this;
	}

	MNamespacePerspectiveOptions ^ MNamespacePerspectiveOptions::defaultVersionProvider(MVersionProvider ^ defaultVersionProvider)
	{
		SKNamespacePerspectiveOptions* pNspo = ((SKNamespacePerspectiveOptions*)pImpl)->defaultVersionProvider( 
			(SKVersionProvider*) (defaultVersionProvider->getPImpl()->pVersionProvider)
		);
		this->pImpl = (void *) pNspo;
		return this;
	}
	
	String ^ MNamespacePerspectiveOptions::toString() 
	{
		string representation = (string)((SKNamespacePerspectiveOptions*)pImpl)->toString(); 
		String ^ nspoString = gcnew String(representation.c_str());
		return nspoString;
		
	}


}