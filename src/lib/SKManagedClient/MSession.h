#pragma once

using namespace System;

namespace SKManagedClient {

	ref class MNamespace;
	ref class MNamespaceCreationOptions;
	ref class MNamespaceOptions;
	ref class MNamespacePerspectiveOptions;
	ref class MAsyncNSPerspective;
	ref class MSyncNSPerspective;
	ref class MPutOptions;
	ref class MGetOptions;
	ref class MWaitOptions;

	//wrapper to pass (SKSession *) around
    ref class SKSession_M
    {
    public:
        void * pSkSession;
    };


	public ref class MSession
	{
	public:
		!MSession();
		virtual ~MSession();
		MNamespace ^ createNamespace(System::String ^ ns, MNamespaceOptions ^ nsOptions);
		MNamespace ^ createNamespace(System::String ^ ns);
		MNamespace ^ getNamespace(System::String ^ ns);
		MNamespaceCreationOptions ^ getNamespaceCreationOptions();
		MNamespaceOptions ^ getDefaultNamespaceOptions();
		void deleteNamespace(System::String ^ ns); 
		void recoverNamespace(System::String ^ ns);
		MPutOptions ^ getDefaultPutOptions();
		MGetOptions ^ getDefaultGetOptions();
		MWaitOptions ^ getDefaultWaitOptions();

		MAsyncNSPerspective ^ openAsyncNamespacePerspective(System::String ^ ns, 
										MNamespacePerspectiveOptions ^ nspOptions);
		MSyncNSPerspective ^ openSyncNamespacePerspective(System::String ^ ns,
										MNamespacePerspectiveOptions ^ nspOptions);
		void close();
																
	internal:
		MSession(SKSession_M ^ skSession_m);
		SKSession_M ^ getPImpl(); 

	private:
		void * pImpl;
	};

}