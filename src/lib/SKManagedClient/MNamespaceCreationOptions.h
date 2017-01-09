#pragma once
#include "SKManagedClient.h"

using namespace System;

namespace SKManagedClient {

	ref class MNamespaceOptions;  //forward decl

	ref class SKNamespaceCreationOptions_M 
	{
	internal:
		void * pNcOptions; // (SKNamespaceCreationOptions*) //
	};

	public enum struct NsCreationMode_M {
		RequireExplicitCreation,
		RequireAutoCreation,
		OptionalAutoCreation_AllowMatches,
		OptionalAutoCreation_DisallowMatches
	};

	public ref class MNamespaceCreationOptions
	{
	public:
		static MNamespaceCreationOptions ^ parse(String ^ def);
		static MNamespaceCreationOptions ^ defaultOptions();
		~MNamespaceCreationOptions();
		!MNamespaceCreationOptions();
		MNamespaceCreationOptions(NsCreationMode_M mode, String ^ regex, MNamespaceOptions ^ defaultNSOptions);

		bool canBeExplicitlyCreated(String ^ ns);
		bool canBeAutoCreated(String ^ ns);
		MNamespaceOptions ^ getDefaultNamespaceOptions();
		String ^ toString();

		//impl
	internal:
		MNamespaceCreationOptions(SKNamespaceCreationOptions_M ^ pNcOptions);
		SKNamespaceCreationOptions_M ^ getPImpl();

	private:
		void * pImpl;
	};

}