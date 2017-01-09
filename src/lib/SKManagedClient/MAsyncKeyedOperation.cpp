#include "StdAfx.h"
#include "MAsyncKeyedOperation.h"

#include <string>
using namespace std;
#include "skconstants.h"
#include "skcontainers.h"
#include "skbasictypes.h"
#include "SKAsyncKeyedOperation.h"

using namespace System::Runtime::InteropServices;

namespace SKManagedClient {


//AsyncKeyedOperation
HashSet<String^> ^ MAsyncKeyedOperation::getKeys()
{
	SKVector<string> * pKeys = ((SKAsyncKeyedOperation*)pImpl)->getKeys();
	int nKeys = pKeys->size();
	HashSet< String^ > ^ keys = gcnew HashSet< String^ >();
	for(int i=0; i<nKeys; i++)
	{
		std::string strKey = pKeys->at(i);
		String ^ key = gcnew String(strKey.c_str());
		keys->Add(key);
	}
	delete pKeys; pKeys = NULL;

	return keys;
}

HashSet<String^> ^ MAsyncKeyedOperation::getIncompleteKeys()
{
	SKVector<string> * pKeys = ((SKAsyncKeyedOperation*)pImpl)->getIncompleteKeys();
	int nKeys = pKeys->size();
	HashSet< String^ > ^ keys = gcnew HashSet< String^ >();
	for(int i=0; i<nKeys; i++)
	{
		std::string strKey = pKeys->at(i);
		String ^ key = gcnew String(strKey.c_str());
		keys->Add(key);
	}
	delete pKeys; pKeys = NULL;

	return keys;
}

SKOperationState_M MAsyncKeyedOperation::getOperationState(String ^ key)
{
	char* key_c =  NULL;
	SKOperationState::SKOperationState state  = SKOperationState::INCOMPLETE;
	try { 
		key_c = (char*)(void*)Marshal::StringToHGlobalAnsi(key);
		std::string strKey = std::string(key_c);
		state = ((SKAsyncKeyedOperation*)pImpl)->getOperationState(strKey);
	}
	finally {
		if (key_c) Marshal::FreeHGlobal(System::IntPtr(key_c));
	}
	return (SKOperationState_M) state;
}

Dictionary<String^, SKOperationState_M> ^ MAsyncKeyedOperation::getOperationStateMap()
{
	SKMap<std::string, SKOperationState::SKOperationState> * vals =  ((SKAsyncKeyedOperation*)pImpl)->getOperationStateMap();
	int nKeys = vals->size();
	Dictionary<String^, SKOperationState_M> ^ values = gcnew Dictionary<String^, SKOperationState_M>(nKeys);

    SKMap<std::string, SKOperationState::SKOperationState>::iterator it;
	for (it=vals->begin(); it!=vals->end(); vals++) {
		String ^ skey = gcnew String(it->first.c_str());
		SKOperationState::SKOperationState state = it->second;
		values->Add(skey, (SKOperationState_M)(state));
	}
	delete vals ; vals = NULL;

	return values;
}

MAsyncKeyedOperation::MAsyncKeyedOperation(){ 
	pImpl = NULL; 
}

MAsyncKeyedOperation::~MAsyncKeyedOperation()
{
	this->!MAsyncKeyedOperation();
}
MAsyncKeyedOperation::!MAsyncKeyedOperation(){ 
	if(pImpl) {
		delete (SKAsyncKeyedOperation*) pImpl;
		pImpl = NULL; 
	}
}


}

