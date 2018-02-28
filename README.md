# Redis-ZSetWithTime
[![Build Status](https://travis-ci.org/liaokz/Redis-ZSetWithTime.svg?branch=master)](https://travis-ci.org/liaokz/Redis-ZSetWithTime) [![license](https://img.shields.io/github/license/mashape/apistatus.svg)](#) [![GitHub last commit](https://img.shields.io/github/last-commit/google/skia.svg)](#)  
An enhanced zset structure with timestamps as a redis module.  

## Features
Members in this zset are firstly ranked by score(from min to max), and then ranked by timestamp(**from max to min**) when thier scores are equal, and last ranked by the lexical order(from min to max) of member names when their scores and timestamps are equal respectively. So `zrevrank` command will return a higher rank for the member with a larger score or with a score identical to others but inserted earlier.  
The implementation of the *dict* and *zskiplist* are extracted from [Redis project](https://github.com/antirez/redis) of version 4.0.1. Some of the low level functions only available within Redis are replaced by the similar APIs of [RedisModulesSDK](https://github.com/RedisLabs/RedisModulesSDK). And all of the command functions are reimplemented as redis module command handlers but completely compatible with their original logic and features. 

## Commands
The commands supported are listed below. All of them should be called with `zts.*` prefix, e.g `zts.zadd`.  

| Command | Note |
| ------- | ----------- |
| zadd    | Add `ts` option to specify the timestamp of the member. See below. |
| zincrby |  |
| zrem    |  |
| zremrangebyrank |  |
| zremrangebyscore |  |
| zcard   |  |
| zcount  |  |
| zscore  |  |
| *zscorets* | Newly added. Get score and timestamp of a member. See below. |
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
  
### zts.zadd
By default, the timestamp will be set according to the time of the server when the member is inserted. With the `ts` option, timestamp value can be specified in the format `zts.zadd key [ts] score timestamp member`, e.g `zts.zadd myzts ts 1 1510798920243 a` will insert an element with score of 1 and timestamp of 1510798920243.  
One important thing is that, if write the default `zts.zadd` command into the AOF file directly, when loading back the data the timestamp will regenerated and be differ from the orignal one. So the default `zts.zadd` command will be converted to a `zts.zadd` command with the `ts` option associated with the current timestamp in the AOF.
  
**Note**: TS and INCR options at the same time are not compatible.  
  
**Example1**：default insertion
```
redis> zts.zadd myzsetts 1 a
(integer) 1
redis> zts.zadd myzsetts 2 b
(integer) 1
redis> zts.zadd myzsetts 2 c
(integer) 1
redis> zts.zadd myzsetts 2 d
(integer) 1
redis> zts.zadd myzsetts 3 e
(integer) 1
redis> zts.zrevrange myzsetts 0 -1 withscores withtimestamps
 1) "e"
 2) "3"
 3) (integer) 1510798942553
 4) "b"
 5) "2"
 6) (integer) 1510798924037
 7) "c"
 8) "2"
 9) (integer) 1510798932081
10) "d"
11) "2"
12) (integer) 1510798938362
13) "a"
14) "1"
15) (integer) 1510798920243
```
  
**Example2**：insert with timestamps
```
redis> zts.zadd myzsetts ts 1 10000 a
(integer) 1
redis> zts.zadd myzsetts ts 1 9000 b
(integer) 1
redis> zts.zadd myzsetts ts 1 11000 c
(integer) 1
redis> zts.zrevrange myzsetts 0 -1 withscores withtimestamps
1) "b"
2) "1"
3) (integer) 9000
4) "a"
5) "1"
6) (integer) 10000
7) "c"
8) "1"
9) (integer) 11000
```

### zts.zscorets
New command for retrieve score and timestamp of a member. Note that while `zscore` returns nil for non-exist members, this command returns *empty array*.  
  
**Example**：
```
redis> zts.zadd myzsetts 1 a
(integer) 1
redis> zts.zscorets myzsetts a
1) "1"
2) (integer) 1510798920243
redis> zts.zscorets myzsetts g
(empty list or set)
```

## License
Redis-ZSetWithTime is licensed under MIT, see LICENSE file.
