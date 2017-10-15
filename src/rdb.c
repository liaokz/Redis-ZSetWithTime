#include "rdb.h"

void ZSetTsRDBSave(RedisModuleIO *io, void *value)
{

}

void *ZSetTsRDBLoad(RedisModuleIO *io, int encver)
{
    if (encver != ZSETTS_ENCODING_VERSION)
    {
        RedisModule_LogIOError(io, "error", "data encoding ver:%d, expecting ver:%d",
                               encver, ZSETTS_ENCODING_VERSION);
        return NULL;
    }
    return NULL;
}

void ZSetTsAOFRewrite(RedisModuleIO *aof, RedisModuleString *key, void *value)
{

}
