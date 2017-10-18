#include "rdb.h"
#include "zsetts.h"

void zsetTsRDBSave(RedisModuleIO *io, void *value)
{
    zset *zs = (zset *)value;
    zskiplist *zsl = zs->zsl;

    RedisModule_SaveUnsigned(io, zsl->length);

    zskiplistNode *zn = zsl->tail;
    while (zn != NULL) {
        RedisModule_SaveStringBuffer(io,(const char*)zn->ele,sdslen(zn->ele));
        RedisModule_SaveDouble(io,zn->score);
        zn = zn->backward;
    }
}

void *zsetTsRDBLoad(RedisModuleIO *io, int encver)
{
    if (encver != ZSETTS_ENCODING_VERSION)
    {
        RedisModule_LogIOError(io, "error", "data encoding ver:%d, expecting ver:%d",
                               encver, ZSETTS_ENCODING_VERSION);
        return NULL;
    }

    uint64_t zsetlen;
    zset *zs;

    zsetlen = (unsigned int)RedisModule_LoadUnsigned(io);
    zs = createZsetObject();

    while(zsetlen--) {
        sds sdsele;
        double score;
        zskiplistNode *znode;

        size_t l = 0;
        char *cele = RedisModule_LoadStringBuffer(io, &l);
        sdsele = sdsnewlen(cele, l);
        score = RedisModule_LoadDouble(io);

        znode = zslInsert(zs->zsl,score,sdsele);
        dictAdd(zs->dict,sdsele,&znode->score);
    }

    return zs;
}

void zsetTsAOFRewrite(RedisModuleIO *aof, RedisModuleString *key, void *value)
{
    zset *zs = (zset *)value;
    zskiplist *zsl = zs->zsl;
    char buf[64];

    zskiplistNode *zn = zsl->tail;
    while (zn != NULL) {
        snprintf(buf, sizeof(buf), "%f", zn->score);
        RedisModule_EmitAOF(aof,"zts.zadd","scb",
                key,buf,(const char*)zn->ele,sdslen(zn->ele));
        zn = zn->backward;
    }
}
