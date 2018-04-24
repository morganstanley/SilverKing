// BlockReader.c

/*
 Distributed speculative read ahead implementation.
 Not for resolving typical reads.
*/

/////////////
// includes

#include "BlockReader.h"
#include "FileAttr.h"
#include "Util.h"

#include <errno.h>
#include <ifaddrs.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>     
#include <sys/socket.h> // socket creating and binding
#include <sys/types.h>

////////////
// defines

#define BR_MAX_MESSAGE_SIZE (SRFS_MAX_PATH_LENGTH + sizeof(FileAttr) + sizeof(size_t) + sizeof(off_t) + sizeof(int))
#define BR_TYPE_OFFSET    0
#define BR_LENGTH_OFFSET  (BR_TYPE_OFFSET + sizeof(uint32_t)) // use uint32 for alignment
#define BR_FA_OFFSET  (BR_LENGTH_OFFSET + sizeof(size_t))
#define BR_READ_OFFSET_OFFSET (BR_FA_OFFSET + sizeof(FileAttr))
#define BR_READ_SIZE_OFFSET (BR_READ_OFFSET_OFFSET + sizeof(off_t))
#define BR_MAX_READAHEAD_OFFSET (BR_READ_SIZE_OFFSET + sizeof(int))
#define BR_PATH_OFFSET (BR_MAX_READAHEAD_OFFSET + sizeof(int))

#define BR_MT_READ_BLOCK    0

#define BR_SOCKET_BUFFER_SIZE   (16 * 1024 * 1024)

#define BR_TMP_NUM_ADDRESSES    1024
#define _IP_ADDRESS_BYTES   4


/////////////
// protocol

/*
 message type
 message length
 FileAttr
 read offset
 read length
 path
*/


///////////////////////
// private prototypes

void br_read_block(BlockReader *br, char *path, FileAttr *fa, size_t readSize, off_t readOffset, int maxReadAhead);
size_t br_message_length_for_path(char *path);
static void *br_run(void *_br);


//////////////////////
// implementation

BlockReader *br_new(int port, PartialBlockReader *pbr, AttrReader *ar) {
    BlockReader *br;

	br = (BlockReader *)mem_alloc(1, sizeof(BlockReader));
    br->port = port;
    br->pbr = pbr;
    br->ar = ar;
    
    if ((br->fd = socket(AF_INET, SOCK_DGRAM, 0)) < 0){
        srfsLog(LOG_WARNING, "errno %d %x", errno, errno);
        fatalError("BlockReader failed to create socket");
    }
    
    int socketBufferSize = BR_SOCKET_BUFFER_SIZE;
    if (setsockopt(br->fd, SOL_SOCKET, SO_RCVBUF, &socketBufferSize, sizeof(int)) == -1) {
        srfsLog(LOG_WARNING, "Failed to set socket buffer size %s %d", __FILE__, __LINE__);
    }    

    memset((char *)&br->myaddr, 0, sizeof(struct sockaddr_in));
    br->myaddr.sin_family = AF_INET;
    br->myaddr.sin_addr.s_addr = htonl(INADDR_ANY);
    br->myaddr.sin_port = htons(port);

    if (bind(br->fd, (struct sockaddr *)&br->myaddr, sizeof(br->myaddr)) < 0){
        srfsLog(LOG_WARNING, "errno %d %x", errno, errno);
        fatalError("BlockReader failed to bind to port");
    } else {
        srfsLog(LOG_WARNING, "BlockReader bound to port %d", port);
    }
    
    br->running = TRUE;
    pthread_create(&br->thread, NULL, br_run, br);
    
    br->numRemoteAddresses = 0;
    br->remoteAddresses = NULL;
	pthread_spin_init(&br->addrLock, 0);
  
    return br;
}

void br_stop(BlockReader *br) {
    br->running = FALSE;
}

void br_server_loop(BlockReader *br) {
    char buf[BR_MAX_MESSAGE_SIZE];
    struct sockaddr_in claddr;  // address of the client
    long recvlen;

    srfsLog(LOG_WARNING, "br server loop start");
    while (br->running) {
        srfsLog(LOG_INFO, "br calling recvfrom()");
        recvlen = recvfrom(br->fd, buf, BR_MAX_MESSAGE_SIZE, 0, NULL, 0);
        if (recvlen < 0) {
            srfsLog(LOG_WARNING, "errno %d %x", errno, errno);
            srfsLog(LOG_WARNING, "Ignoring recvfrom() error");
        } else {
            int         *type;
            size_t      *length;
            FileAttr    *fa;
            off_t       *readOffset;
            size_t      *readSize;
            int         *maxReadAhead;
            char        *path;
            
            srfsLog(LOG_INFO, "br received %ld", recvlen);
            type = (int *)(buf + BR_TYPE_OFFSET);
            length = (size_t *)(buf + BR_LENGTH_OFFSET);
            fa = (FileAttr *)(buf + BR_FA_OFFSET);
            readOffset = (off_t *)(buf + BR_READ_OFFSET_OFFSET);
            readSize = (size_t *)(buf + BR_READ_SIZE_OFFSET);
            maxReadAhead = (int *)(buf + BR_MAX_READAHEAD_OFFSET);
            path = (char *)(buf + BR_PATH_OFFSET);
            
            br_read_block(br, path, fa, *readSize, *readOffset, *maxReadAhead);
        }
    }
}

static uint32_t br_read_ip(char *ip) {
    uint32_t    b[_IP_ADDRESS_BYTES];
    int         bi;
    char        *tok;
    uint32_t    _ip;
    
    memset(b, 0, _IP_ADDRESS_BYTES * sizeof(uint32_t));
    bi = 0;
    tok = ip;
    while (bi < _IP_ADDRESS_BYTES) {
        char    *curTok;
        
        curTok = tok;
        if (bi < _IP_ADDRESS_BYTES - 1) {
            char    *next;
            
            next = tok;
            strsep(&next, ".");
            if (next != NULL) {
                tok = next;
            } else {
                return 0;
            }
        }
        b[bi++] = atoi(curTok);
    }
    _ip = (b[0] << 24) | (b[1] << 16) | (b[2] << 8) | b[3];
    srfsLog(LOG_INFO, "%s %x", ip, _ip);
    return _ip;
}

static uint32_t br_ntoh(uint32_t n) {
    return ((n & 0xff) << 24) | ((n & 0xff00) << 8) | ((n & 0xff0000) >> 8) | (((n & 0xff000000) >> 24) & 0xff);
}

static int br_find_ip(uint32_t ip, struct ifaddrs *ifap) {
    while (ifap != NULL) {
        struct sockaddr_in  *sin;
        
        sin = (struct sockaddr_in *)ifap->ifa_addr;
        srfsLog(LOG_INFO, "comparing %x %x %x", sin->sin_addr.s_addr, br_ntoh(sin->sin_addr.s_addr), ip);
        if (br_ntoh(sin->sin_addr.s_addr) == (unsigned long)ip) {
            srfsLog(LOG_INFO, "br_find_ip match %x", ip);
            return TRUE;
        }
        ifap = ifap->ifa_next;
    }
    return FALSE;
}

void br_read_addresses(BlockReader *br, char *remoteAddressFile) {
    FILE    *f;
    
    srfsLog(LOG_WARNING, "br_read_addresses %s", remoteAddressFile);
    f = fopen(remoteAddressFile, "r");
    if (f == NULL) {
        srfsLog(LOG_WARNING, "br_read_addresses failed");
    } else {
        char    *line;
        ssize_t n;
        size_t dummy;
        int     reading;
        int     numRead;
        uint32_t *tmp;
        uint32_t tmpSize;
        int     numRemoteAddresses;
        uint32_t *remoteAddresses;
        struct ifaddrs *ifap;
        
        ifap = NULL;
        if (getifaddrs(&ifap) < 0) {
            ifap = NULL;
            srfsLog(LOG_WARNING, "getifaddrs failed");
        }
        line = NULL;
        dummy = 0;
        tmpSize = BR_TMP_NUM_ADDRESSES;
        tmp = (uint32_t *)mem_alloc(tmpSize, sizeof(uint32_t));
        numRead = 0;
        reading = TRUE;
        while (reading) {
            srfsLog(LOG_INFO, "reading numRead %d", numRead);
            n = getline(&line, &dummy, f);
            srfsLog(LOG_INFO, "reading %d", n);
            if (n <= 0) {
                reading = FALSE;
                srfsLog(LOG_INFO, "reading error %d", errno);
            } else {
                uint32_t    ip;
                
                srfsLog(LOG_INFO, "reading line %s", line);
                ip = br_read_ip(line);
                free(line);
                line = NULL;
                if (ip != 0) {
                    if (!br_find_ip(ip, ifap)) {
                        srfsLog(LOG_INFO, "Found remote IP");
                        if (numRead + 1 > tmpSize) {
                            int oTmpSize;
                            
                            oTmpSize = tmpSize;
                            tmpSize += BR_TMP_NUM_ADDRESSES;
                            mem_realloc((void**)&tmp, oTmpSize, tmpSize, sizeof(uint32_t));
                        }
                        tmp[numRead++] = ip;
                    } else {
                        srfsLog(LOG_INFO, "Skipping local IP");
                    }
                } else {
                    srfsLog(LOG_INFO, "Couldn't read IP");
                }                
            }
        }
        fclose(f);
        if (ifap != NULL) {
            freeifaddrs(ifap);
            ifap = NULL;
        }
        
        numRemoteAddresses = numRead;
        remoteAddresses = (uint32_t *)mem_dup(tmp, numRead * sizeof(uint32_t));
        mem_free((void**)&tmp);
        br_set_remote_addresses(br, numRemoteAddresses, remoteAddresses);        
    }
}

void br_set_remote_addresses(BlockReader *br, int numRemoteAddresses, uint32_t *remoteAddresses) {
    srfsLog(LOG_WARNING, "br_set_remote_addresses %d", numRemoteAddresses);
    for (int i = 0; i < numRemoteAddresses; i++) {
        srfsLog(LOG_WARNING, "%d\t%x", i, remoteAddresses[i]);
    }
    if (numRemoteAddresses >= 0 && remoteAddresses != NULL) {
        uint32_t *oldRemoteAddresses;

        // lock
        pthread_spin_lock(&br->addrLock);
        if (br->remoteAddresses != NULL) {
            oldRemoteAddresses = br->remoteAddresses;
        } else {
            oldRemoteAddresses = NULL;
        }
        br->numRemoteAddresses = numRemoteAddresses;
        br->remoteAddresses = remoteAddresses;
        br->remoteAddressIndex = 0;
        pthread_spin_unlock(&br->addrLock);
        // unlock
        if (oldRemoteAddresses != NULL) {
            mem_free((void **)&oldRemoteAddresses);
        }
    } else {
        srfsLog(LOG_WARNING, "Invalid parameters to br_set_remote_addresses");
    }
}

void br_request_remote_read(BlockReader *br, char *path, FileAttr *fa, size_t readSize, off_t readOffset, int maxReadAhead) {
    char buf[BR_MAX_MESSAGE_SIZE];
    struct sockaddr_in serverAddr;
    int         *_type;
    size_t      *_length;
    FileAttr    *_fa;
    off_t       *_readOffset;
    size_t      *_readSize;
    int         *_maxReadAhead;
    char        *_path;
    uint32_t    remoteAddress;
    
    srfsLog(LOG_INFO, "br br_request_remote_read %s", path);
    socklen_t slen = sizeof(struct sockaddr_in);

    memset((char*)&serverAddr, 0, sizeof(struct sockaddr_in));
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(br->port);
    
    // lock
    pthread_spin_lock(&br->addrLock);
    if (br->remoteAddresses == NULL) {
        remoteAddress = 0x7f000001;
    } else {
        br->remoteAddressIndex = (br->remoteAddressIndex + 1) % br->numRemoteAddresses;
        remoteAddress = br->remoteAddresses[br->remoteAddressIndex];
    }
    pthread_spin_unlock(&br->addrLock);
    // unlock
    //srfsLog(LOG_WARNING, "Sending remote request to %x", remoteAddress);
    serverAddr.sin_addr.s_addr = htonl(remoteAddress);

    _type = (int *)(buf + BR_TYPE_OFFSET);
    _length = (size_t *)(buf + BR_LENGTH_OFFSET);
    _fa = (FileAttr *)(buf + BR_FA_OFFSET);
    _readOffset = (off_t *)(buf + BR_READ_OFFSET_OFFSET);
    _readSize = (size_t *)(buf + BR_READ_SIZE_OFFSET);
    _maxReadAhead = (int *)(buf + BR_MAX_READAHEAD_OFFSET);
    _path = (char *)(buf + BR_PATH_OFFSET);
    
    memset(buf, 0, BR_MAX_MESSAGE_SIZE);
    *_type = BR_MT_READ_BLOCK;
    *_length = br_message_length_for_path(path);
    *_fa = *fa;
    *_readOffset = readOffset;
    *_readSize = readSize;
    *_maxReadAhead = maxReadAhead;
    strcpy(_path, path);

    if (sendto(br->fd, buf, *_length, 0, (struct sockaddr *)&serverAddr, /*slen*/sizeof(struct sockaddr)) < 0) {
        srfsLog(LOG_WARNING, "errno %d %x", errno, errno);
        srfsLog(LOG_WARNING, "%d %d", sizeof(struct sockaddr), sizeof(struct sockaddr_in));
        srfsLog(LOG_WARNING, "%d %d %d %d %d", *_type, *_length, *_readOffset, *_readSize, *_maxReadAhead);
        srfsLog(LOG_WARNING, "BlockReader failed to send message");
    } else {
        srfsLog(LOG_INFO, "br_request_remote_read sent OK");
    }
}

void br_read_block(BlockReader *br, char *path, FileAttr *fa, size_t readSize, off_t readOffset, int maxReadAhead) {
    srfsLog(LOG_INFO, "br_read_block %s %d %d %d", path, readSize, readOffset, maxReadAhead);
    ar_ensure_path_fid_associated(br->ar, path, &fa->fid);
    pbr_read_given_attr(br->pbr, path, NULL, readSize, readOffset, fa, FALSE, maxReadAhead, TRUE);
}

size_t br_message_length_for_path(char *path) {
    return BR_PATH_OFFSET + strlen(path) + 1;
}

static void *br_run(void *_br) {
	BlockReader	*br;
	int		curThreadIndex;
	
	br = (BlockReader *)_br;
    br_server_loop(br);
}
