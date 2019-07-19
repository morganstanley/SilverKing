// NSKeySplit.c

//////////////////
// includes

#include "NSKeySplit.h"
#include "Util.h"

#include <string.h>


///////////////////
// implementation

NSKeySplit *nks_new(char *path) {
    NSKeySplit    *nks;

    nks = (NSKeySplit *)mem_alloc(1, sizeof(NSKeySplit));
    nks_init(nks, path);
    if (nks->ns == NULL) {
        nks_delete(&nks);
        return NULL;
    } else {
        return nks;
    }
}

void nks_init(NSKeySplit *nks, char *path) {
    srfsLog(LOG_FINE, "nks_init %s", path);
    nks->key = strchr(path + 1, '/');
    if (nks->key == NULL) {
        strcpy(nks->namespaceStorage, path);
        nks->ns = "/";
        nks->key = nks->namespaceStorage;
        srfsLog(LOG_FINE, "nks_new mapping NULL key to /");
    } else {
        int namespaceLength;

        namespaceLength = nks->key - path;
        if (namespaceLength > SRFS_MAX_NAMESPACE_LENGTH) {
            srfsLog(LOG_WARNING, "namespaceLength > MAX_NAMESPACE_LENGTH\n", __FILE__, __LINE__);
            nks->ns = NULL;
            nks->key = NULL;
            return;
        }
        memcpy(nks->namespaceStorage, path, namespaceLength);
        nks->namespaceStorage[namespaceLength] = '\0';
        nks->ns = nks->namespaceStorage;
    }
    srfsLog(LOG_FINE, "namespace: %s\tkey: %s\n", nks->ns, nks->key);
}

void nks_copy(NSKeySplit *dest, NSKeySplit *source) {
    memset(dest, 0, sizeof(NSKeySplit));
    dest->ns = dest->namespaceStorage;
    dest->key = dest->namespaceStorage + strlen(source->ns) + 1;
    strcpy(dest->ns, source->ns);
    strcpy(dest->key, source->key);
}

/**
 * Delete an NSKeySplit instance
 */
void nks_delete(NSKeySplit **nks) {
    if (nks != NULL && *nks != NULL) {
        mem_free((void **)nks);
    } else {
        fatalError("bad ptr passed to nks_delete");
    }
}

int nks_convert_to_attrib(NSKeySplit *nks) {
    int    length;

    length = strlen(nks->ns) + 1;
    if (length >= SRFS_MAX_NAMESPACE_LENGTH) {
        srfsLog(LOG_WARNING, "length > SRFS_MAX_NAMESPACE_LENGTH");
        return -1;
    } else {
        nks->namespaceStorage[length - 1] = '_';
        nks->namespaceStorage[length] = '\0';
        return 0;
    }
}
