/**
*
* $Header: $
* $Change: $
* $DateTime: $
*/

#ifndef SKBASICTYPES_H
#define SKBASICTYPES_H

#include "skconstants.h"
#include <stdint.h>
#include <limits.h>
#include <cstdio>
#include <stdint.h>


#ifdef __cplusplus
extern "C" {
#endif

struct SKVal
{
  size_t                m_len;
  void                  *m_pVal;
  SKOperationState::SKOperationState     m_rc;
};
typedef SKVal SKVal;
typedef SKVal SKVal_t;
typedef SKVal * SKValPtr;

/** Creates a SKVal structure. */
SKAPI SKVal* sk_create_val();

/** free any value present, and clear any error codes */
SKAPI void sk_clear_val(SKVal* pVal_);

/** 
 * Destroys a SKVal structure. This is the one and only method that
 * should be used to destroy SKVals.
*/
SKAPI void sk_destroy_val(SKVal** pVal);

/** 
 * Move val from src to dst.
 * if dst is NULL, clean up members of src
*/
SKAPI void sk_move_val(SKVal *pSrc_, SKVal *pDst_);


/**
 *  Sets external value to be kept in SKVal structure
*/
SKAPI void sk_set_val(SKVal *pVal_, int size_, void *src_);

/**
 *  Sets external value to be kept in SKVal structure.
 * Directly use the pointer provided and take charge of
 * it's deallocation.
*/
SKAPI void sk_set_val_zero_copy(SKVal *pVal_, int size_, void *src_);

/**
 * Returns true if is a valid value from server.
 * Either SKOpResult::SUCCEEDED with a value, or SKOpResult::NO_SUCH_VALUE.
*/
// DHTCAPI bool dht_is_valid_val(SKVal *pVal_);  //TODO: SKOpResult::NO_SUCH_VALUE

/**
 * Returns true if is a valid existing value from server.
 * SK.e. SKOpResult::SUCCEEDED with a value.
*/
SKAPI bool sk_is_valid_existing_val(SKVal *pVal_);

SKAPI void skFatalError(const char *msg, const char *file, int line);
SKAPI void *skMemAlloc(size_t nmemb, size_t size, const char *file, int line);
SKAPI void skMemFree(void **ptr, const char *file, int line);
SKAPI void *skMemDup(const void *source, int size, const char *file, int line);
SKAPI char *skStrDup(const char *source, const char *file, int line);
SKAPI void print_stacktrace(const char* source, FILE *out = stderr, const unsigned int max_frames = 64 );

#ifdef __cplusplus
}
#endif



#endif   //SKBASICTYPES_H
