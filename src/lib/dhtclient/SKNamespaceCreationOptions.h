#ifndef SKNAMESPACECREATIONOPTIONS_H
#define SKNAMESPACECREATIONOPTIONS_H

#include "skconstants.h"

class SKNamespaceOptions;

typedef enum NsCreationMode_t {
		/** Users must always explicitly create a namespace before use. */
		RequireExplicitCreation,
	   /** 
		* Users must never explicitly create a namespace. Namespaces will be
		* automatically created using DHT-wide specified NamespaceOptions. 
		*/
	   RequireAutoCreation,
	   /**
		* Users may either explicitly create namespaces or 
		* optionally autocreate namespaces. Autocreation will only be allowed 
		* for namespaces matching the given regular expression.
		*/
	   OptionalAutoCreation_AllowMatches,
	   /**
		* Users may either explicitly create namespaces or 
		* optionally autocreate namespaces. Autocreation will only be disallowed
		* for namespaces matching the given regular expression.
		*/
	   OptionalAutoCreation_DisallowMatches
} NsCreationMode ;

class SKNamespaceCreationOptions 
{
public:
    SKAPI static SKNamespaceCreationOptions * parse(const char * def);
	SKAPI static SKNamespaceCreationOptions * defaultOptions();
    SKAPI ~SKNamespaceCreationOptions();
    SKAPI SKNamespaceCreationOptions(NsCreationMode mode, const char * regex, SKNamespaceOptions * defaultNSOptions);
    SKAPI bool canBeExplicitlyCreated(const char * ns);
    SKAPI bool canBeAutoCreated(const char * ns);
    SKAPI SKNamespaceOptions * getDefaultNamespaceOptions();
	SKAPI char * toString() const;

	//impl
	SKNamespaceCreationOptions(void * pNamespaceCreationOptions);
	void * getPImpl();

private:
	void * pImpl;
};

#endif // SKNAMESPACECREATIONOPTIONS_H

