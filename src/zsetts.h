#ifndef __ZSET_TS_ZSETTS_H
#define __ZSET_TS_ZSETTS_H

#include "redismodule.h"
#include "rmutil/sds.h"
#include "dict.h"

typedef struct zskiplistNode {
    sds ele;
    double score;
    long long timestamp;
    struct zskiplistNode *backward;
    struct zskiplistLevel {
        struct zskiplistNode *forward;
        unsigned int span;
    } level[];
} zskiplistNode;

typedef struct zskiplist {
    struct zskiplistNode *header, *tail;
    unsigned long length;
    int level;
} zskiplist;

typedef struct zset {
    dict *dict;
    zskiplist *zsl;
} zset;

void freeZsetObject(void *o);

zset *createZsetObject(void);
zskiplistNode *zslInsert(zskiplist *zsl, double score, long long timestamp, sds ele);

int zaddCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int zincrbyCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int zremCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int zremrangebyrankCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int zremrangebyscoreCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int zcardCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int zscoreCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int zscoretsCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int zrankCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int zrevrankCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int zrangeCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int zrevrangeCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int zrangebyscoreCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int zrevrangebyscoreCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int zcountCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

#endif // __ZSET_TS_ZSETTS_H
