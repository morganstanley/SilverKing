// AttrReadRequest.h

#ifndef _ATTR_READ_REQUEST_H_
#define _ATTR_READ_REQUEST_H_

/////////////
// includes

/////////////
// includes

#include "AttrReader.h"
#include "Util.h"


//////////
// types

typedef struct AttrReadRequest {
	AttrReader	*attrReader;
	char *path;
} AttrReadRequest;


///////////////
// prototypes

AttrReadRequest *arr_new(AttrReader *attrReader, char *path);
void arr_delete(AttrReadRequest **arr);
void arr_display(AttrReadRequest *arr, LogLevel level);

#endif
