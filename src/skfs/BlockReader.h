// BlockReader.h

#ifndef _BLOCK_READER_H_
#define _BLOCK_READER_H_

/*
 Distributed speculative read ahead implementation.
 Not for resolving typical reads.
*/

/////////////
// includes

#include "AttrReader.h"
#include "PartialBlockReader.h"
#include "Util.h"

#include <netinet/in.h> // struct sockaddr_in
#include <pthread.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>


///////////////////
// public defines

#define BR_DEFAULT_PORT 33777


//////////
// types

typedef struct BlockReader {
    uint16_t    port;
    PartialBlockReader *pbr;
    AttrReader  *ar;
    struct sockaddr_in myaddr;  // address of the server
    int         running;
	pthread_t   thread;
    int fd;
    int remoteAddressIndex;
    int    numRemoteAddresses;
    uint32_t    *remoteAddresses;
	pthread_spinlock_t	addrLock;
    // addresses of others...
} BlockReader;


//////////////////////
// public prototypes

BlockReader *br_new(int port, PartialBlockReader *pbr, AttrReader *ar);
void br_stop(BlockReader *br);
void br_request_remote_read(BlockReader *br, char *path, FileAttr *fa, size_t readSize, off_t readOffset, int maxReadAhead);
void br_read_addresses(BlockReader *br, char *remoteAddressFile);
void br_set_remote_addresses(BlockReader *br, int numRemoteAddresses, uint32_t *remoteAddresses);

#endif
