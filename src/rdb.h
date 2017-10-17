#ifndef __ZSET_TS_RDB_H
#define __ZSET_TS_RDB_H

#include "redismodule.h"

#define ZSETTS_ENCODING_VERSION 0

void zsetTsRDBSave(RedisModuleIO *io, void *value);
void *zsetTsRDBLoad(RedisModuleIO *io, int encver);
void zsetTsAOFRewrite(RedisModuleIO *aof, RedisModuleString *key, void *value);

#endif // __ZSET_TS_RDB_H
