#pragma once
using namespace System;

namespace SKManagedClient {

	ref class MNamespace;
	ref class MNamespacePerspectiveOptions;
	ref class MVersionConstraint;
	ref class MVersionProvider;

	public interface class MBaseNSPerspective 
	{
	public:
		virtual String ^ getName();
		virtual MNamespace ^ getNamespace();
		virtual MNamespacePerspectiveOptions ^ getOptions();
		virtual void setOptions(MNamespacePerspectiveOptions ^ nspOptions);
		virtual void setDefaultRetrievalVersionConstraint(MVersionConstraint ^ vc);
		virtual void setDefaultVersionProvider(MVersionProvider ^ versionProvider);
		virtual void setDefaultVersion(Int64 version);
		virtual void close();

	};

}

