#include "redismodule.h"
#include "rmutil/util.h"
#include "rmutil/strings.h"
#include "rmutil/test_util.h"

#include "rdb.h"
#include "zsetts.h"

RedisModuleType *ZSetTsType;

void ZSetTsFreeCallback(void *value) {
    RedisModule_Free(value);
}

int RedisModule_OnLoad(RedisModuleCtx *ctx) {

  // Register the module itself
  if (RedisModule_Init(ctx, "zset-with-time", 1, REDISMODULE_APIVER_1) ==
      REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  // Register the data type
  RedisModuleTypeMethods tm = {
    .version = REDISMODULE_TYPE_METHOD_VERSION,
    .rdb_load = ZSetTsRDBLoad,
    .rdb_save = ZSetTsRDBSave,
    .aof_rewrite = ZSetTsAOFRewrite,
    .free = ZSetTsFreeCallback
  };

  ZSetTsType = RedisModule_CreateDataType(ctx, "ZSetWithT", ZSETTS_ENCODING_VERSION, &tm);
  if (ZSetTsType == NULL) return REDISMODULE_ERR;

  // register the commands
  RMUtil_RegisterWriteCmd(ctx, "zts.zadd", zaddCommand);
  RMUtil_RegisterWriteCmd(ctx, "zts.zincrby", zincrbyCommand);

  return REDISMODULE_OK;
}
