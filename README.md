# Redis-ZSetWithTime
## Feature
An enhanced zset structure with timestamps as a redis module.  
Members in this zset are firstly ranked by score(from min to max), and then ranked by timestamp(**from max to min**) when thier scores are equal, and last ranked by the lexical order(from min to max) of member names when their scores and timestamps are equal respectively. So `zrevrank` command will return a higher rank for the member with a larger score or with an identical score but inserted earlier.  
The source code is mostly extract from [Redis project](https://github.com/antirez/redis) of version 4.0.1. Some of the low level functions are replaced by the similar APIs of [RedisModulesSDK](https://github.com/RedisLabs/RedisModulesSDK). And all the command functions are reimplemented as redis module command handlers but completely compatible with their original logic and feature. 

## Command
The commands supported are listed below. All of them should be called with `zts.*` prefix, e.g `zts.zadd`.  

| Command | Note |
| ------- | ----------- |
| zadd    | Add `ts` option to specify the timetamp of the member. See below. |
| zincrby |  |
| zrem    |  |
| zremrangebyrank |  |
| zremrangebyscore |  |
| zcard   |  |
| zcount  |  |
| zscore  |  |
| *zscorets* | Newly added. Get score and timestamp of a member. Returns empty array if member not exist. |
| zrank   |  |
| zrevrank |  |
| zrange  | Add `withtimestamps` option to retrieve the timestamps. |
| zrevrange | Add `withtimestamps` option to retrieve the timestamps. |
| zrangebyscore | Add `withtimestamps` option to retrieve the timestamps. |
| zrevrangebyscore | Add `withtimestamps` option to retrieve the timestamps. |
| ~~zinterstore~~ |  |
| ~~zunionstore~~ |  |
| ~~zlexcount~~ |  |
| ~~zrangebylex~~ |  |
| ~~zrevrangebylex~~ |  |
| ~~zremrangebylex~~ |  |
| ~~zscan~~ |  |

- **zts.zadd**  
  By default, the timestamp will be set according to the time of the server when the member is inserted. With the `ts` option, timestamp value can be specified in the format `zts.zadd key [ts] score timestamp member`, e.g `zts.zadd myzts ts 1 10000 a`.  
  **Note**: TS and INCR options at the same time are not compatible.
