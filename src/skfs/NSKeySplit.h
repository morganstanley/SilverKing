// NSKeySplit.h

#ifndef _NS_KEY_SPLIT_H_
#define _NS_KEY_SPLIT_H_

/////////////
// includes

#include "SRFSConstants.h"


//////////
// types

typedef struct NSKeySplit {
    char    namespaceStorage[SRFS_MAX_NAMESPACE_LENGTH];
    char    *ns;
    char    *key;
} NSKeySplit;


///////////////
// prototypes

NSKeySplit *nks_new(char *path);
void nks_init(NSKeySplit *nks, char *path);
void nks_copy(NSKeySplit *dest, NSKeySplit *source);
void nks_delete(NSKeySplit **nks);
int nks_convert_to_attrib(NSKeySplit *nks);

#endif
