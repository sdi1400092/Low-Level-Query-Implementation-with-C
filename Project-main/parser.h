#include "join.h"
#include <ctype.h>

typedef struct {
    int *rel_name;
    int num_rels;
} rela;

typedef struct{
    int rel1;
    int rel2;
    int column1;
    int column2;
    char action;
    int number;
} query;

typedef struct{
    int num_of_queries;
    query* q;
} predicate;

typedef struct {
    int num_sums;
    int rel;
    int column;
} sums;

relation Parser(char *, int);

relation *Mapping(char *);

relation GetColumn(result, relation);

rela ParseRelations(char *);

predicate ParsePredicates( char *);

sums *ParseProjections(char *);