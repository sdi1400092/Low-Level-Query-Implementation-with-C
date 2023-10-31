#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <inttypes.h>

#define CACHE 1536 

typedef struct{
    int32_t key;
    int32_t payload;
} tuple;
/**
* Type definition for a relation.
* It consists of an array of tuples and a size of the relation.
*/

typedef struct{
    tuple *tuples;
    uint32_t num_tuples;
} relation;

/**
* Type definition for a relation.
* It consists of an array of tuples and a size of the relation.
*/

typedef struct {
    int *conto;
    int num_of_conto;
    int relnum;
    int *rowid;
    int count;
} result;

typedef struct{
    int32_t *key;
    int num;
} Hist;

typedef struct {
    tuple *tup;
} bucket;

typedef struct {
    int hash_value;
    uint32_t *rowid;
    int count;
    int32_t payload;
    int bitmap[4];
} HT_bucket;

typedef struct {
    HT_bucket *ht_buckets;
} HT;

/** Partitioned Hash Join**/
result* PartitionedHashJoin(relation, relation);

result Compare(relation,char ,int);

result *updateResult1(result *, result *, int);

result *updateResult(result *, result *, int);