// DirDataReadRequest.h

#ifndef _DIR_DATA_READ_REQUEST_H_
#define _DIR_DATA_READ_REQUEST_H_

/////////////
// includes

/////////////
// includes

#include "DirDataReader.h"
#include "Util.h"


//////////
// types

typedef enum {DDRR_Initial, DDRR_Update} DDRR_Type;

typedef struct DirDataReadRequest {
    DirDataReader    *dirDataReader;
    char *path;
    DDRR_Type    type;
} DirDataReadRequest;


///////////////
// prototypes

DirDataReadRequest *ddrr_new(DirDataReader *dirDataReader, char *path, DDRR_Type type);
void ddrr_delete(DirDataReadRequest **ddrr);
void ddrr_display(DirDataReadRequest *ddrr, LogLevel level);

#endif
