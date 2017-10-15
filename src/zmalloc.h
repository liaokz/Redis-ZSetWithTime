/* Zmalloc functions redefinition.
 * Redefine the zmalloc functions by using the RedisModule_* functions.
 */

#ifndef __ZSET_TS_ZMALLOC_H
#define __ZSET_TS_ZMALLOC_H

#include <redismodule.h>

#define zmalloc(size) RedisModule_Alloc(size)
#define zcalloc(size) RedisModule_Calloc(1, size)
#define zrealloc(ptr, size) RedisModule_Realloc(ptr, size)
#define zfree(ptr) RedisModule_Free(ptr)

#endif // __ZSET_TS_ZMALLOC_H
