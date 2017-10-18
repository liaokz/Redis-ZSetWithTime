#include "zsetts.h"
#include <stdlib.h>
#include <math.h>
#include <string.h>
#include <assert.h>
#include "zmalloc.h"

#define serverAssert(x) assert(x)

extern RedisModuleType *ZSetTsType;

static char *nullbulk = "-1";

sds sdsFromRedisModuleString(RedisModuleString *s)
{
    size_t l;
    const char *c = RedisModule_StringPtrLen(s, &l);
    return sdsnewlen(c, l);
}

/*-----------------------------------------------------------------------------
 *  zsetDictType related definition from redis 4.0
 *----------------------------------------------------------------------------*/
/* Error codes */
#define C_ERR                   -1

#define ZSKIPLIST_MAXLEVEL 32 /* Should be enough for 2^32 elements */
#define ZSKIPLIST_P 0.25      /* Skiplist P = 1/4 */

/* Input flags. */
#define ZADD_NONE 0
#define ZADD_INCR (1<<0)    /* Increment the score instead of setting it. */
#define ZADD_NX (1<<1)      /* Don't touch elements not already existing. */
#define ZADD_XX (1<<2)      /* Only touch elements already exisitng. */

/* Output flags. */
#define ZADD_NOP (1<<3)     /* Operation not performed because of conditionals.*/
#define ZADD_NAN (1<<4)     /* Only touch elements already exisitng. */
#define ZADD_ADDED (1<<5)   /* The element was new and was added. */
#define ZADD_UPDATED (1<<6) /* The element already existed, score updated. */

/* Flags only used by the ZADD command but not by zsetAdd() API: */
#define ZADD_CH (1<<16)      /* Return num of elements added or updated. */

/* Struct to hold a inclusive/exclusive range spec by score comparison. */
typedef struct {
    double min, max;
    int minex, maxex; /* are min or max exclusive? */
} zrangespec;

/* Struct to hold an inclusive/exclusive range spec by lexicographic comparison. */
typedef struct {
    sds min, max;     /* May be set to shared.(minstring|maxstring) */
    int minex, maxex; /* are min or max exclusive? */
} zlexrangespec;

uint64_t dictSdsHash(const void *key) {
    return dictGenHashFunction((unsigned char*)key, sdslen((char*)key));
}

int dictSdsKeyCompare(void *privdata, const void *key1,
        const void *key2)
{
    int l1,l2;
    DICT_NOTUSED(privdata);

    l1 = sdslen((sds)key1);
    l2 = sdslen((sds)key2);
    if (l1 != l2) return 0;
    return memcmp(key1, key2, l1) == 0;
}

/* Sorted sets hash (note: a skiplist is used in addition to the hash table) */
dictType zsetDictType = {
    dictSdsHash,               /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictSdsKeyCompare,         /* key compare */
    NULL,                      /* Note: SDS string shared & freed by skiplist */
    NULL                       /* val destructor */
};

/* Hash table parameters */
#define HASHTABLE_MIN_FILL        10      /* Minimal hash table fill 10% */

int htNeedsResize(dict *dict) {
    long long size, used;

    size = dictSlots(dict);
    used = dictSize(dict);
    return (size > DICT_HT_INITIAL_SIZE &&
            (used*100/size < HASHTABLE_MIN_FILL));
}

/*-----------------------------------------------------------------------------
 *  rewrite necessary functions from redis 4.0
 *----------------------------------------------------------------------------*/

zskiplist *zslCreate(void);

zset *createZsetObject(void) {
    zset *zs = zmalloc(sizeof(*zs));

    zs->dict = dictCreate(&zsetDictType,NULL);
    zs->zsl = zslCreate();
    return zs;
}

void zslFree(zskiplist *zsl);
void freeZsetObject(void *o) {
    zset *zs = (zset *)o;
    dictRelease(zs->dict);
    zslFree(zs->zsl);
    zfree(zs);
}

/*-----------------------------------------------------------------------------
 * Skiplist implementation of the low level API from redis 4.0
 *----------------------------------------------------------------------------*/

/* Create a skiplist node with the specified number of levels.
 * The SDS string 'ele' is referenced by the node after the call. */
zskiplistNode *zslCreateNode(int level, double score, sds ele) {
    zskiplistNode *zn =
        zmalloc(sizeof(*zn)+level*sizeof(struct zskiplistLevel));
    zn->score = score;
    zn->ele = ele;
    return zn;
}

/* Create a new skiplist. */
zskiplist *zslCreate(void) {
    int j;
    zskiplist *zsl;

    zsl = zmalloc(sizeof(*zsl));
    zsl->level = 1;
    zsl->length = 0;
    zsl->header = zslCreateNode(ZSKIPLIST_MAXLEVEL,0,NULL);
    for (j = 0; j < ZSKIPLIST_MAXLEVEL; j++) {
        zsl->header->level[j].forward = NULL;
        zsl->header->level[j].span = 0;
    }
    zsl->header->backward = NULL;
    zsl->tail = NULL;
    return zsl;
}

/* Free the specified skiplist node. The referenced SDS string representation
 * of the element is freed too, unless node->ele is set to NULL before calling
 * this function. */
void zslFreeNode(zskiplistNode *node) {
    sdsfree(node->ele);
    zfree(node);
}

/* Free a whole skiplist. */
void zslFree(zskiplist *zsl) {
    zskiplistNode *node = zsl->header->level[0].forward, *next;

    zfree(zsl->header);
    while(node) {
        next = node->level[0].forward;
        zslFreeNode(node);
        node = next;
    }
    zfree(zsl);
}

/* Returns a random level for the new skiplist node we are going to create.
 * The return value of this function is between 1 and ZSKIPLIST_MAXLEVEL
 * (both inclusive), with a powerlaw-alike distribution where higher
 * levels are less likely to be returned. */
int zslRandomLevel(void) {
    int level = 1;
    while ((random()&0xFFFF) < (ZSKIPLIST_P * 0xFFFF))
        level += 1;
    return (level<ZSKIPLIST_MAXLEVEL) ? level : ZSKIPLIST_MAXLEVEL;
}

/* Insert a new node in the skiplist. Assumes the element does not already
 * exist (up to the caller to enforce that). The skiplist takes ownership
 * of the passed SDS string 'ele'. */
zskiplistNode *zslInsert(zskiplist *zsl, double score, sds ele) {
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    unsigned int rank[ZSKIPLIST_MAXLEVEL];
    int i, level;

    serverAssert(!isnan(score));
    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        /* store rank that is crossed to reach the insert position */
        rank[i] = i == (zsl->level-1) ? 0 : rank[i+1];
        while (x->level[i].forward &&
                (x->level[i].forward->score < score ||
                    (x->level[i].forward->score == score &&
                    sdscmp(x->level[i].forward->ele,ele) < 0)))
        {
            rank[i] += x->level[i].span;
            x = x->level[i].forward;
        }
        update[i] = x;
    }
    /* we assume the element is not already inside, since we allow duplicated
     * scores, reinserting the same element should never happen since the
     * caller of zslInsert() should test in the hash table if the element is
     * already inside or not. */
    level = zslRandomLevel();
    if (level > zsl->level) {
        for (i = zsl->level; i < level; i++) {
            rank[i] = 0;
            update[i] = zsl->header;
            update[i]->level[i].span = zsl->length;
        }
        zsl->level = level;
    }
    x = zslCreateNode(level,score,ele);
    for (i = 0; i < level; i++) {
        x->level[i].forward = update[i]->level[i].forward;
        update[i]->level[i].forward = x;

        /* update span covered by update[i] as x is inserted here */
        x->level[i].span = update[i]->level[i].span - (rank[0] - rank[i]);
        update[i]->level[i].span = (rank[0] - rank[i]) + 1;
    }

    /* increment span for untouched levels */
    for (i = level; i < zsl->level; i++) {
        update[i]->level[i].span++;
    }

    x->backward = (update[0] == zsl->header) ? NULL : update[0];
    if (x->level[0].forward)
        x->level[0].forward->backward = x;
    else
        zsl->tail = x;
    zsl->length++;
    return x;
}

/* Internal function used by zslDelete, zslDeleteByScore and zslDeleteByRank */
void zslDeleteNode(zskiplist *zsl, zskiplistNode *x, zskiplistNode **update) {
    int i;
    for (i = 0; i < zsl->level; i++) {
        if (update[i]->level[i].forward == x) {
            update[i]->level[i].span += x->level[i].span - 1;
            update[i]->level[i].forward = x->level[i].forward;
        } else {
            update[i]->level[i].span -= 1;
        }
    }
    if (x->level[0].forward) {
        x->level[0].forward->backward = x->backward;
    } else {
        zsl->tail = x->backward;
    }
    while(zsl->level > 1 && zsl->header->level[zsl->level-1].forward == NULL)
        zsl->level--;
    zsl->length--;
}

/* Delete an element with matching score/element from the skiplist.
 * The function returns 1 if the node was found and deleted, otherwise
 * 0 is returned.
 *
 * If 'node' is NULL the deleted node is freed by zslFreeNode(), otherwise
 * it is not freed (but just unlinked) and *node is set to the node pointer,
 * so that it is possible for the caller to reuse the node (including the
 * referenced SDS string at node->ele). */
int zslDelete(zskiplist *zsl, double score, sds ele, zskiplistNode **node) {
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    int i;

    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        while (x->level[i].forward &&
                (x->level[i].forward->score < score ||
                    (x->level[i].forward->score == score &&
                     sdscmp(x->level[i].forward->ele,ele) < 0)))
        {
            x = x->level[i].forward;
        }
        update[i] = x;
    }
    /* We may have multiple elements with the same score, what we need
     * is to find the element with both the right score and object. */
    x = x->level[0].forward;
    if (x && score == x->score && sdscmp(x->ele,ele) == 0) {
        zslDeleteNode(zsl, x, update);
        if (!node)
            zslFreeNode(x);
        else
            *node = x;
        return 1;
    }
    return 0; /* not found */
}

int zslValueGteMin(double value, zrangespec *spec) {
    return spec->minex ? (value > spec->min) : (value >= spec->min);
}

int zslValueLteMax(double value, zrangespec *spec) {
    return spec->maxex ? (value < spec->max) : (value <= spec->max);
}

/* Returns if there is a part of the zset is in range. */
int zslIsInRange(zskiplist *zsl, zrangespec *range) {
    zskiplistNode *x;

    /* Test for ranges that will always be empty. */
    if (range->min > range->max ||
            (range->min == range->max && (range->minex || range->maxex)))
        return 0;
    x = zsl->tail;
    if (x == NULL || !zslValueGteMin(x->score,range))
        return 0;
    x = zsl->header->level[0].forward;
    if (x == NULL || !zslValueLteMax(x->score,range))
        return 0;
    return 1;
}

/* Find the first node that is contained in the specified range.
 * Returns NULL when no element is contained in the range. */
zskiplistNode *zslFirstInRange(zskiplist *zsl, zrangespec *range) {
    zskiplistNode *x;
    int i;

    /* If everything is out of range, return early. */
    if (!zslIsInRange(zsl,range)) return NULL;

    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        /* Go forward while *OUT* of range. */
        while (x->level[i].forward &&
            !zslValueGteMin(x->level[i].forward->score,range))
                x = x->level[i].forward;
    }

    /* This is an inner range, so the next node cannot be NULL. */
    x = x->level[0].forward;
    serverAssert(x != NULL);

    /* Check if score <= max. */
    if (!zslValueLteMax(x->score,range)) return NULL;
    return x;
}

/* Find the last node that is contained in the specified range.
 * Returns NULL when no element is contained in the range. */
zskiplistNode *zslLastInRange(zskiplist *zsl, zrangespec *range) {
    zskiplistNode *x;
    int i;

    /* If everything is out of range, return early. */
    if (!zslIsInRange(zsl,range)) return NULL;

    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        /* Go forward while *IN* range. */
        while (x->level[i].forward &&
            zslValueLteMax(x->level[i].forward->score,range))
                x = x->level[i].forward;
    }

    /* This is an inner range, so this node cannot be NULL. */
    serverAssert(x != NULL);

    /* Check if score >= min. */
    if (!zslValueGteMin(x->score,range)) return NULL;
    return x;
}

/* Delete all the elements with score between min and max from the skiplist.
 * Min and max are inclusive, so a score >= min || score <= max is deleted.
 * Note that this function takes the reference to the hash table view of the
 * sorted set, in order to remove the elements from the hash table too. */
unsigned long zslDeleteRangeByScore(zskiplist *zsl, zrangespec *range, dict *dict) {
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    unsigned long removed = 0;
    int i;

    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        while (x->level[i].forward && (range->minex ?
            x->level[i].forward->score <= range->min :
            x->level[i].forward->score < range->min))
                x = x->level[i].forward;
        update[i] = x;
    }

    /* Current node is the last with score < or <= min. */
    x = x->level[0].forward;

    /* Delete nodes while in range. */
    while (x &&
           (range->maxex ? x->score < range->max : x->score <= range->max))
    {
        zskiplistNode *next = x->level[0].forward;
        zslDeleteNode(zsl,x,update);
        dictDelete(dict,x->ele);
        zslFreeNode(x); /* Here is where x->ele is actually released. */
        removed++;
        x = next;
    }
    return removed;
}

/* Delete all the elements with rank between start and end from the skiplist.
 * Start and end are inclusive. Note that start and end need to be 1-based */
unsigned long zslDeleteRangeByRank(zskiplist *zsl, unsigned int start, unsigned int end, dict *dict) {
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    unsigned long traversed = 0, removed = 0;
    int i;

    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        while (x->level[i].forward && (traversed + x->level[i].span) < start) {
            traversed += x->level[i].span;
            x = x->level[i].forward;
        }
        update[i] = x;
    }

    traversed++;
    x = x->level[0].forward;
    while (x && traversed <= end) {
        zskiplistNode *next = x->level[0].forward;
        zslDeleteNode(zsl,x,update);
        dictDelete(dict,x->ele);
        zslFreeNode(x);
        removed++;
        traversed++;
        x = next;
    }
    return removed;
}

/* Find the rank for an element by both score and key.
 * Returns 0 when the element cannot be found, rank otherwise.
 * Note that the rank is 1-based due to the span of zsl->header to the
 * first element. */
unsigned long zslGetRank(zskiplist *zsl, double score, sds ele) {
    zskiplistNode *x;
    unsigned long rank = 0;
    int i;

    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        while (x->level[i].forward &&
            (x->level[i].forward->score < score ||
                (x->level[i].forward->score == score &&
                sdscmp(x->level[i].forward->ele,ele) <= 0))) {
            rank += x->level[i].span;
            x = x->level[i].forward;
        }

        /* x might be equal to zsl->header, so test if obj is non-NULL */
        if (x->ele && sdscmp(x->ele,ele) == 0) {
            return rank;
        }
    }
    return 0;
}

/* Finds an element by its rank. The rank argument needs to be 1-based. */
zskiplistNode* zslGetElementByRank(zskiplist *zsl, unsigned long rank) {
    zskiplistNode *x;
    unsigned long traversed = 0;
    int i;

    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        while (x->level[i].forward && (traversed + x->level[i].span) <= rank)
        {
            traversed += x->level[i].span;
            x = x->level[i].forward;
        }
        if (traversed == rank) {
            return x;
        }
    }
    return NULL;
}

/* Populate the rangespec according to the objects min and max. */
static int zslParseRange(RedisModuleString *min, RedisModuleString *max, zrangespec *spec) {
    char *eptr;
    spec->minex = spec->maxex = 0;

    /* Parse the min-max interval. If one of the values is prefixed
     * by the "(" character, it's considered "open". For instance
     * ZRANGEBYSCORE zset (1.5 (2.5 will match min < x < max
     * ZRANGEBYSCORE zset 1.5 2.5 will instead match min <= x <= max */
    {
        size_t l;
        const char *c = RedisModule_StringPtrLen(min, &l);
        if (c[0] == '(') {
            spec->min = strtod(c+1,&eptr);
            if (eptr[0] != '\0' || isnan(spec->min)) return C_ERR;
            spec->minex = 1;
        } else {
            spec->min = strtod(c,&eptr);
            if (eptr[0] != '\0' || isnan(spec->min)) return C_ERR;
        }
    }
    {
        size_t l;
        const char *c = RedisModule_StringPtrLen(max, &l);
        if (c[0] == '(') {
            spec->max = strtod(c+1,&eptr);
            if (eptr[0] != '\0' || isnan(spec->max)) return C_ERR;
            spec->maxex = 1;
        } else {
            spec->max = strtod(c,&eptr);
            if (eptr[0] != '\0' || isnan(spec->max)) return C_ERR;
        }
    }

    return REDISMODULE_OK;
}

/*-----------------------------------------------------------------------------
 * Common sorted set API
 *----------------------------------------------------------------------------*/

unsigned int zsetLength(const zset *zs) {
    return zs->zsl->length;
}

/* Return (by reference) the score of the specified member of the sorted set
 * storing it into *score. If the element does not exist C_ERR is returned
 * otherwise C_OK is returned and *score is correctly populated.
 * If 'zobj' or 'member' is NULL, C_ERR is returned. */
int zsetScore(zset *zs, sds member, double *score) {
    if (!zs || !member) return C_ERR;

    dictEntry *de = dictFind(zs->dict, member);
    if (de == NULL) return C_ERR;
    *score = *(double*)dictGetVal(de);
    return REDISMODULE_OK;
}

/* Add a new element or update the score of an existing element in a sorted
 * set, regardless of its encoding.
 *
 * The set of flags change the command behavior. They are passed with an integer
 * pointer since the function will clear the flags and populate them with
 * other flags to indicate different conditions.
 *
 * The input flags are the following:
 *
 * ZADD_INCR: Increment the current element score by 'score' instead of updating
 *            the current element score. If the element does not exist, we
 *            assume 0 as previous score.
 * ZADD_NX:   Perform the operation only if the element does not exist.
 * ZADD_XX:   Perform the operation only if the element already exist.
 *
 * When ZADD_INCR is used, the new score of the element is stored in
 * '*newscore' if 'newscore' is not NULL.
 *
 * The returned flags are the following:
 *
 * ZADD_NAN:     The resulting score is not a number.
 * ZADD_ADDED:   The element was added (not present before the call).
 * ZADD_UPDATED: The element score was updated.
 * ZADD_NOP:     No operation was performed because of NX or XX.
 *
 * Return value:
 *
 * The function returns 1 on success, and sets the appropriate flags
 * ADDED or UPDATED to signal what happened during the operation (note that
 * none could be set if we re-added an element using the same score it used
 * to have, or in the case a zero increment is used).
 *
 * The function returns 0 on erorr, currently only when the increment
 * produces a NAN condition, or when the 'score' value is NAN since the
 * start.
 *
 * The commad as a side effect of adding a new element may convert the sorted
 * set internal encoding from ziplist to hashtable+skiplist.
 *
 * Memory managemnet of 'ele':
 *
 * The function does not take ownership of the 'ele' SDS string, but copies
 * it if needed. */
int zsetAdd(zset *zs, double score, sds ele, int *flags, double *newscore) {
    /* Turn options into simple to check vars. */
    int incr = (*flags & ZADD_INCR) != 0;
    int nx = (*flags & ZADD_NX) != 0;
    int xx = (*flags & ZADD_XX) != 0;
    *flags = 0; /* We'll return our response flags. */
    double curscore;

    /* NaN as input is an error regardless of all the other parameters. */
    if (isnan(score)) {
        *flags = ZADD_NAN;
        return 0;
    }

    /* Update the sorted set according to its encoding. */
    zskiplistNode *znode;
    dictEntry *de;

    de = dictFind(zs->dict,ele);
    if (de != NULL) {
        /* NX? Return, same element already exists. */
        if (nx) {
            *flags |= ZADD_NOP;
            return 1;
        }
        curscore = *(double*)dictGetVal(de);

        /* Prepare the score for the increment if needed. */
        if (incr) {
            score += curscore;
            if (isnan(score)) {
                *flags |= ZADD_NAN;
                return 0;
            }
            if (newscore) *newscore = score;
        }

        /* Remove and re-insert when score changes. */
        if (score != curscore) {
            zskiplistNode *node;
            serverAssert(zslDelete(zs->zsl,curscore,ele,&node));
            znode = zslInsert(zs->zsl,score,node->ele);
            /* We reused the node->ele SDS string, free the node now
             * since zslInsert created a new one. */
            node->ele = NULL;
            zslFreeNode(node);
            /* Note that we did not removed the original element from
             * the hash table representing the sorted set, so we just
             * update the score. */
            dictGetVal(de) = &znode->score; /* Update score ptr. */
            *flags |= ZADD_UPDATED;
        }
        return 1;
    } else if (!xx) {
        ele = sdsdup(ele);
        znode = zslInsert(zs->zsl,score,ele);
        serverAssert(dictAdd(zs->dict,ele,&znode->score) == DICT_OK);
        *flags |= ZADD_ADDED;
        if (newscore) *newscore = score;
        return 1;
    } else {
        *flags |= ZADD_NOP;
        return 1;
    }

    return 0; /* Never reached. */
}

/* Delete the element 'ele' from the sorted set, returning 1 if the element
 * existed and was deleted, 0 otherwise (the element was not there). */
int zsetDel(zset *zs, sds ele) {
    dictEntry *de;
    double score;

    de = dictUnlink(zs->dict,ele);
    if (de != NULL) {
        /* Get the score in order to delete from the skiplist later. */
        score = *(double*)dictGetVal(de);

        /* Delete from the hash table and later from the skiplist.
         * Note that the order is important: deleting from the skiplist
         * actually releases the SDS string representing the element,
         * which is shared between the skiplist and the hash table, so
         * we need to delete from the skiplist as the final step. */
        dictFreeUnlinkedEntry(zs->dict,de);

        /* Delete from skiplist. */
        int retval = zslDelete(zs->zsl,score,ele,NULL);
        serverAssert(retval);

        if (htNeedsResize(zs->dict)) dictResize(zs->dict);
        return 1;
    }
    return 0; /* No such element found. */
}

/* Given a sorted set object returns the 0-based rank of the object or
 * -1 if the object does not exist.
 *
 * For rank we mean the position of the element in the sorted collection
 * of elements. So the first element has rank 0, the second rank 1, and so
 * forth up to length-1 elements.
 *
 * If 'reverse' is false, the rank is returned considering as first element
 * the one with the lowest score. Otherwise if 'reverse' is non-zero
 * the rank is computed considering as element with rank 0 the one with
 * the highest score. */
long zsetRank(zset *zs, sds ele, int reverse) {
    unsigned long llen = zsetLength(zs);
    unsigned long rank;

    zskiplist *zsl = zs->zsl;
    dictEntry *de;
    double score;

    de = dictFind(zs->dict,ele);
    if (de != NULL) {
        score = *(double*)dictGetVal(de);
        rank = zslGetRank(zsl,score,ele);
        /* Existing elements always have a rank. */
        serverAssert(rank != 0);
        if (reverse)
            return llen-rank;
        else
            return rank-1;
    }
    return -1;
}

/*-----------------------------------------------------------------------------
 * Sorted set commands
 *----------------------------------------------------------------------------*/

/* This generic command implements both ZADD and ZINCRBY. */
int zaddGenericCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
        int flags)
{
    static char *nanerr = "resulting score is not a number (NaN)";
    RedisModuleKey *key = NULL;
    zset *zobj = NULL;
    sds ele;
    double score = 0, *scores = NULL;
    int j, elements;
    int scoreidx = 0;
    /* The following vars are used in order to track what the command actually
     * did during the execution, to reply to the client and to trigger the
     * notification of keyspace change. */
    int added = 0;      /* Number of new elements added. */
    int updated = 0;    /* Number of elements with updated score. */
    int processed = 0;  /* Number of elements processed, may remain zero with
                           options like XX. */
    int ret = 0;

    RedisModule_AutoMemory(ctx);

    /* Parse options. At the end 'scoreidx' is set to the argument position
     * of the score of the first score-element pair. */
    scoreidx = 2;
    while(scoreidx < argc) {
        size_t l;
        const char *opt = RedisModule_StringPtrLen(argv[scoreidx], &l);
        if (!strcasecmp(opt,"nx")) flags |= ZADD_NX;
        else if (!strcasecmp(opt,"xx")) flags |= ZADD_XX;
        else if (!strcasecmp(opt,"ch")) flags |= ZADD_CH;
        else if (!strcasecmp(opt,"incr")) flags |= ZADD_INCR;
        else break;
        scoreidx++;
    }

    /* Turn options into simple to check vars. */
    int incr = (flags & ZADD_INCR) != 0;
    int nx = (flags & ZADD_NX) != 0;
    int xx = (flags & ZADD_XX) != 0;
    int ch = (flags & ZADD_CH) != 0;

    /* After the options, we expect to have an even number of args, since
     * we expect any number of score-element pairs. */
    elements = argc-scoreidx;
    if (elements % 2 || !elements) {
        return RedisModule_WrongArity(ctx);
    }
    elements /= 2; /* Now this holds the number of score-element pairs. */

    /* Check for incompatible options. */
    if (nx && xx) {
        return RedisModule_ReplyWithError(ctx,
            "XX and NX options at the same time are not compatible");
    }

    if (incr && elements > 1) {
        return RedisModule_ReplyWithError(ctx,
            "INCR option supports a single increment-element pair");
    }

    /* Start parsing all the scores, we need to emit any syntax error
     * before executing additions to the sorted set, as the command should
     * either execute fully or nothing at all. */
    scores = zmalloc(sizeof(double)*elements);
    for (j = 0; j < elements; j++) {
        if ((ret=RedisModule_StringToDouble(argv[scoreidx+j*2],&scores[j]))
            != REDISMODULE_OK) {
            ret = RedisModule_ReplyWithError(ctx,"value is not a valid float");
            goto cleanup;
        }
    }

    /* Lookup the key and create the sorted set if does not exist. */
    key = RedisModule_OpenKey(ctx,argv[1], REDISMODULE_READ|REDISMODULE_WRITE);
    if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
        if (xx) goto reply_to_client; /* No key + XX option: nothing to do. */
        zobj = createZsetObject();
        RedisModule_ModuleTypeSetValue(key,ZSetTsType,zobj);
    } else {
        if (RedisModule_ModuleTypeGetType(key) != ZSetTsType) {
            ret = RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
            goto cleanup;
        }
        zobj = (zset *)RedisModule_ModuleTypeGetValue(key);
    }

    for (j = 0; j < elements; j++) {
        double newscore;
        score = scores[j];
        int retflags = flags;

        ele = sdsFromRedisModuleString(argv[scoreidx+1+j*2]);
        int retval = zsetAdd(zobj, score, ele, &retflags, &newscore);
        sdsfree(ele);
        if (retval == 0) {
            ret = RedisModule_ReplyWithError(ctx,nanerr);
            goto cleanup;
        }
        if (retflags & ZADD_ADDED) added++;
        if (retflags & ZADD_UPDATED) updated++;
        if (!(retflags & ZADD_NOP)) processed++;
        score = newscore;
    }

reply_to_client:
    if (incr) { /* ZINCRBY or INCR option. */
        if (processed)
            ret = RedisModule_ReplyWithDouble(ctx,score);
        else
            ret = RedisModule_ReplyWithError(ctx,nullbulk);
    } else { /* ZADD. */
        ret = RedisModule_ReplyWithLongLong(ctx,ch ? added+updated : added);
    }

cleanup:
    /*if (key) {
        RedisModule_CloseKey(key);
    }*/
    zfree(scores);
    return ret;
}

int zaddCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return zaddGenericCommand(ctx, argv, argc, ZADD_NONE);
}

int zincrbyCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return zaddGenericCommand(ctx, argv, argc, ZADD_INCR);
}

int zremCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModuleKey *key = NULL;
    zset *zobj;
    int deleted = 0, j;

    RedisModule_AutoMemory(ctx);

    key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ|REDISMODULE_WRITE);
    if (key == NULL || RedisModule_ModuleTypeGetType(key) != ZSetTsType)
        return RedisModule_ReplyWithLongLong(ctx,0);

    zobj = (zset *)RedisModule_ModuleTypeGetValue(key);

    for (j = 2; j < argc; j++) {
        sds ele = sdsFromRedisModuleString(argv[j]);
        if (zsetDel(zobj,ele)) deleted++;
        sdsfree(ele);
        if (zsetLength(zobj) == 0) {
            RedisModule_DeleteKey(key);
            break;
        }
    }

    return RedisModule_ReplyWithLongLong(ctx,deleted);
}

int zcardCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModuleKey *key = NULL;
    zset *zobj = NULL;

    RedisModule_AutoMemory(ctx);

    key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
    if (key == NULL || RedisModule_ModuleTypeGetType(key) != ZSetTsType)
        return RedisModule_ReplyWithLongLong(ctx,0);

    zobj = (zset *)RedisModule_ModuleTypeGetValue(key);
    return RedisModule_ReplyWithLongLong(ctx,zsetLength(zobj));
}

int zscoreCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModuleKey *key = NULL;
    zset *zobj = NULL;
    sds ele;
    double score;
    int retval;

    RedisModule_AutoMemory(ctx);

    key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
    if (key == NULL || RedisModule_ModuleTypeGetType(key) != ZSetTsType)
        return RedisModule_ReplyWithNull(ctx);

    zobj = (zset *)RedisModule_ModuleTypeGetValue(key);

    ele = sdsFromRedisModuleString(argv[2]);
    retval = zsetScore(zobj,ele,&score);
    sdsfree(ele);
    if (retval == C_ERR) {
        return RedisModule_ReplyWithNull(ctx);
    }
    return RedisModule_ReplyWithDouble(ctx,score);
}

int zrankGenericCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
        int reverse) {
    RedisModuleKey *key = NULL;
    sds ele;
    zset *zobj;
    long rank;

    RedisModule_AutoMemory(ctx);

    key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
    if (key == NULL || RedisModule_ModuleTypeGetType(key) != ZSetTsType)
        return RedisModule_ReplyWithNull(ctx);

    zobj = (zset *)RedisModule_ModuleTypeGetValue(key);

    ele = sdsFromRedisModuleString(argv[2]);
    rank = zsetRank(zobj,ele,reverse);
    sdsfree(ele);
    if (rank >= 0) {
        return RedisModule_ReplyWithLongLong(ctx,rank);
    }
    return RedisModule_ReplyWithNull(ctx);
}

int zrankCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return zrankGenericCommand(ctx, argv, argc, 0);
}

int zrevrankCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return zrankGenericCommand(ctx, argv, argc, 1);
}

int zrangeGenericCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
        int reverse) {
    RedisModuleKey *key = NULL;
    zset *zs = NULL;
    int withscores = 0;
    long long start;
    long long end;
    int llen;
    int rangelen;

    RedisModule_AutoMemory(ctx);

    if ((RedisModule_StringToLongLong(argv[2], &start) != REDISMODULE_OK) ||
        (RedisModule_StringToLongLong(argv[3], &end) != REDISMODULE_OK)) {
        return RedisModule_ReplyWithNull(ctx);
    }

    if (argc == 5) {
        size_t l;
        const char *opt = RedisModule_StringPtrLen(argv[4], &l);
        if (!strcasecmp(opt,"withscores")) {
            withscores = 1;
        } else {
            return RedisModule_WrongArity(ctx);
        }
    } else if (argc > 5) {
        return RedisModule_WrongArity(ctx);
    }

    key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
    if (key == NULL || RedisModule_ModuleTypeGetType(key) != ZSetTsType)
        return RedisModule_ReplyWithArray(ctx, 0);

    zs = (zset *)RedisModule_ModuleTypeGetValue(key);

    /* Sanitize indexes. */
    llen = zsetLength(zs);
    if (start < 0) start = llen+start;
    if (end < 0) end = llen+end;
    if (start < 0) start = 0;

    /* Invariant: start >= 0, so this test will be true when end < 0.
     * The range is empty when start > end or start >= length. */
    if (start > end || start >= llen) {
        return RedisModule_ReplyWithArray(ctx, 0);
    }
    if (end >= llen) end = llen-1;
    rangelen = (end-start)+1;

    /* Return the result in form of a multi-bulk reply */
    RedisModule_ReplyWithArray(ctx, withscores ? (rangelen*2) : rangelen);

    zskiplist *zsl = zs->zsl;
    zskiplistNode *ln;
    sds ele;

    /* Check if starting point is trivial, before doing log(N) lookup. */
    if (reverse) {
        ln = zsl->tail;
        if (start > 0)
            ln = zslGetElementByRank(zsl,llen-start);
    } else {
        ln = zsl->header->level[0].forward;
        if (start > 0)
            ln = zslGetElementByRank(zsl,start+1);
    }

    while(rangelen--) {
        //serverAssertWithInfo(c,zobj,ln != NULL);
        ele = ln->ele;
        RedisModule_ReplyWithStringBuffer(ctx,ele,sdslen(ele));
        if (withscores)
            RedisModule_ReplyWithDouble(ctx,ln->score);
        ln = reverse ? ln->backward : ln->level[0].forward;
    }

    return 0;
}

int zrangeCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return zrangeGenericCommand(ctx,argv,argc,0);
}

int zrevrangeCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return zrangeGenericCommand(ctx,argv,argc,1);
}
