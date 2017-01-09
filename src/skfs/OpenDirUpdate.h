// OpenDirUpdate.h

#ifndef _OPEN_DIR_UPDATE_H_
#define _OPEN_DIR_UPDATE_H_

/////////////
// includes

#include <stdint.h>


////////////
// defines

#define ODU_T_ADDITION	0
#define ODU_T_DELETION	1


//////////
// types

typedef struct OpenDirUpdate {
	uint32_t	type;
	uint64_t	version;
	char		*name;
} OpenDirUpdate;


///////////////
// prototypes

void odu_init(OpenDirUpdate *odu, uint32_t type, uint64_t version, char *name);
void odu_delete(OpenDirUpdate **odu);
void odu_modify(OpenDirUpdate *odu, uint32_t type, uint64_t version);

#endif
