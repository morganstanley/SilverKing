// PartialBlockReader.h

#ifndef _PARTIAL_BLOCK_READER_H_
#define _PARTIAL_BLOCK_READER_H_

/////////////
// includes

#include "AttrReader.h"
#include "FileAttr.h"
#include "FileBlockReader.h"
#include "G2TaskOutputReader.h"
#include "SKFSOpenFile.h"


//////////
// types

typedef struct PartialBlockReader {
	AttrReader			*ar;
	FileBlockReader		*fbr;
	G2TaskOutputReader	*g2tor;
} PartialBlockReader;


///////////////
// prototypes

PartialBlockReader *pbr_new(AttrReader *ar, FileBlockReader *fbr, G2TaskOutputReader *g2tor);
void pbr_delete(PartialBlockReader **pbr);
int pbr_read(PartialBlockReader *pbr, const char *path, char *dest, size_t readSize, off_t readOffset, SKFSOpenFile *sof);
int pbr_read_given_attr(PartialBlockReader *pbr, const char *path, char *dest, size_t readSize, off_t readOffset, FileAttr *fa, int presumeBlocksInDHT, int maxBlocksReadAhead = 131072, int useNFSReadAhead = FALSE);

#endif
