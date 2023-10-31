#include "parser.h"
#include <sys/mman.h>
#include <fcntl.h>

relation Parser(char *filename, int column){

    relation R;
    R.num_tuples = 0;
    R.tuples = malloc(sizeof(tuple));
    FILE *ptr;
    ptr = fopen(filename, "r");
    ssize_t chars;
    size_t len = 0;

    char *line;
    char *token;
    int counter;

    while(chars = getline(&line, &len, ptr) != -1){

        counter = 1;
        // line[chars+1] = '\0';
        token = strtok(line, "|");

        while(counter < column && token != NULL){
            
            token = strtok(NULL, "|");
            counter++;
        }

        R.num_tuples++;
        R.tuples = realloc(R.tuples, R.num_tuples*sizeof(tuple));
        R.tuples[R.num_tuples-1].payload = atoi(token);
        R.tuples[R.num_tuples-1].key = R.num_tuples;

    }

    return R;
    
}

relation *Mapping(char *filename) {
    
    relation *rel;

    FILE *f = fopen(filename, "r");
    if(f == NULL){
        printf("open failed\n");
    }

    fseek(f, 0L, SEEK_END);
    long int size = ftell(f);
    fseek(f, 0L, SEEK_SET);
    int fd = fileno(f);
    

    int *p = mmap(NULL, size*sizeof(int), PROT_READ, MAP_PRIVATE, fd, 0);
    if(p == MAP_FAILED){
        printf("map failed\n");
    }

    int numtuples = p[0], numcolumn = p[2];

    rel = (relation *) malloc(numcolumn*sizeof(relation));
    for(int i=0 ; i<numcolumn ; i++){
        rel[i].tuples = malloc(numtuples*sizeof(tuple));
    }

    int j = 0;
    for(int i=4 ; i<=2*numtuples+2 ; i+=2){
        for(int z=0 ; z<numcolumn ; z++){
            rel[z].tuples[j].payload = p[i+z*2*numtuples];
            rel[z].tuples[j].key = j+1;
        }
        j++;
    }

    for(int z=0 ; z<numcolumn ; z++){
        rel[z].num_tuples = numtuples;
    }


    int err = munmap(p, 2*sizeof(int));
    if(err != 0) {
        printf("unmap failed\n");
    }

    fclose(f);

    return rel;
}

relation GetColumn(result res, relation rel) {

    relation temp;
    if(res.relnum != -1){
        temp.tuples = malloc(res.count*sizeof(tuple));
        temp.num_tuples = res.count;
        for(int i=0 ; i < res.count ; i++){
            temp.tuples[i] = rel.tuples[res.rowid[i]-1];
        }

        // free(rel.tuples);
        // printf("returning %d\n", res.count);
        return temp;

    } else {
        // printf("get column\n");
        return rel;
    }
}

rela ParseRelations(char *rels){
    rela temp;
    temp.rel_name = malloc(sizeof(int));
    temp.num_rels = 0;
    char *token;
    int counter=0;

    token=strtok(rels," ");

    while(token!=NULL){
        counter++;
        temp.rel_name=realloc(temp.rel_name,counter*sizeof(int));
        temp.rel_name[counter-1]=atoi(token);
        token=strtok(NULL," ");
        temp.num_rels++;
    }

    return temp;


}

predicate ParsePredicates( char *pred){
    int i;
    predicate temp;
    temp.q=(query *) malloc(sizeof(query));
    int counter=0;
    char c;

    char *token= strtok(pred,"&");
    while(token!=NULL){
        counter++;
        temp.q=realloc(temp.q,counter*sizeof(query));
        c = token[0];
        temp.q[counter-1].rel1=atoi(&c);
        c = token[2];
        temp.q[counter-1].column1=atoi(&c);
        temp.q[counter-1].action=token[3];

        if(token[5] == '.'){
            c = token[4];
            temp.q[counter-1].rel2=atoi(&c);
            c = token[6];
            temp.q[counter-1].column2=atoi(&c);
            temp.q[counter-1].number=-1;
        }
        else{
            token+=4;
            temp.q[counter-1].number=atoi(token);
        }
        token=strtok(NULL,"&");
    }
    temp.num_of_queries=counter;
    return temp;
}

sums *ParseProjections(char *proj) {

    sums *sum = malloc(sizeof(sums));
    int counter = 0;
    char *token, c;
    sum[0].num_sums = 0;
    
    token = strtok(proj, " ");
    while(token != NULL){
        counter++;
        sum = realloc(sum, counter*sizeof(sums));

        c = token[0];
        sum[counter-1].rel = atoi(&c);
        c=token[2];
        sum[counter-1].column = atoi(&c);

        token = strtok(NULL, " ");

        sum[0].num_sums++;
    }

    return sum;

}