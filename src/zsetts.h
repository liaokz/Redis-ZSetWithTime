#ifndef __ZSET_TS_ZSETTS_H
#define __ZSET_TS_ZSETTS_H

#include "redismodule.h"

int zaddCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int zincrbyCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

#endif // __ZSET_TS_ZSETTS_H
