#ifndef __ZSET_TS_RDB_H
#define __ZSET_TS_RDB_H

#include "redismodule.h"

#define ZSETTS_ENCODING_VERSION 0

void ZSetTsRDBSave(RedisModuleIO *io, void *value);
void *ZSetTsRDBLoad(RedisModuleIO *io, int encver);
void ZSetTsAOFRewrite(RedisModuleIO *aof, RedisModuleString *key, void *value);

#endif // __ZSET_TS_RDB_H
