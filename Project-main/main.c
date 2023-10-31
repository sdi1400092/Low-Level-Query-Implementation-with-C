#include "parser.h"
#include <unistd.h>

#define columnR 1
#define columnS 1

int main(int argc, char **argv){
    
    char relname[10];

    relation **relations;
    relations = (relation **)malloc(sizeof(relation));

    int rel_counter=0;
    while(fgets(relname,10,stdin)!=NULL){
        relname[strlen(relname)-1] = '\0';
        rel_counter++;
        relations = (relation **) realloc(relations, rel_counter*sizeof(relation));
        relations[rel_counter-1] = Mapping(relname);
    }

    sleep(1);
    
    FILE *f = fopen(argv[1], "r");
    if(f == NULL){
        printf("open failed\n");
    }

    char *line;
    ssize_t chars;
    ssize_t len=0;
    result *res, *tempres;
    int Querycounter = 0, Questioncounter = 1, counter1 = 0, counter2 = 0;
    char* rel_names;
    rela r;
    int* rels;

    char* predicates;
    predicate preds;

    char* projections;
    sums *sum;
    int relsize;
    int sumsize;
    uint64_t **finalsum;
    finalsum = (uint64_t **) malloc(sizeof(uint64_t));
    relation tempr;
    int *size;
    size = malloc(sizeof(int));

    printf("Reading Queries...\n");
    sleep(1);
    while(chars=getline(&line, &len, f)!=-1){
        if(strcmp(line,"F\n")!=0){

            if(Querycounter == 0) printf("\nStarting Section of Queries #%d\n\n", Questioncounter);
            Querycounter++;

            // parsing the queries from the file small.work
            rel_names= strtok(line, "|");
            predicates= strtok(NULL,"|");
            projections= strtok(NULL,"|");
            projections[strlen(projections)-1]='\0';

            r=ParseRelations(rel_names);
            preds=ParsePredicates(predicates);
            sum = ParseProjections(projections);

            rels = r.rel_name;
            relsize = r.num_rels;
            sumsize = sum[0].num_sums;

            // putting the filters first in the queries
            query temp;
            int i, j = 0;
            for(i=0 ; i<preds.num_of_queries ; i++){
                if(preds.q[i].number != -1){
                    temp = preds.q[j];
                    preds.q[j] = preds.q[i];
                    preds.q[i] = temp;
                    j++;
                }
            }

            res = (result *) malloc(relsize*sizeof(result));
            for(int j=0 ; j<relsize ; j++){
                res[j].conto=malloc(sizeof(int));
                res[j].num_of_conto = 0;
                res[j].count = 0;
                res[j].relnum = -1;
            }

            relation relS, relR;

            for(i=0 ; i<preds.num_of_queries ; i++){
                if(preds.q[i].number != -1){
                    // Filter
                    relS = GetColumn(res[preds.q[i].rel1], relations[rels[preds.q[i].rel1]][preds.q[i].column1]);
                    res[preds.q[i].rel1] = Compare(relS, preds.q[i].action, preds.q[i].number);
                    res[preds.q[i].rel1].num_of_conto = 0;
                    res[preds.q[i].rel1].relnum = preds.q[i].rel1;
                } else {
                    // Join
                    relS = GetColumn(res[preds.q[i].rel1], relations[rels[preds.q[i].rel1]][preds.q[i].column1]);
                    relR = GetColumn(res[preds.q[i].rel2], relations[rels[preds.q[i].rel2]][preds.q[i].column2]);

                    // calling PartitionedHashJoin and store results in tempres
                    printf("la\n");
                    tempres = PartitionedHashJoin(relS, relR);
                    printf("lala\n");

                    // storing number of relation and the array that its results are connected to in tempres
                    tempres[0].relnum = preds.q[i].rel1;
                    tempres[1].relnum = preds.q[i].rel2;
                    tempres[0].conto=malloc(sizeof(int));
                    tempres[1].conto=malloc(sizeof(int));
                    tempres[0].num_of_conto=1;
                    tempres[1].num_of_conto=1;
                    tempres[0].conto[0]=preds.q[i].rel2;
                    tempres[1].conto[0]=preds.q[i].rel1;

                    // storing the connected to data from the old results in the new one
                    if((res[tempres[0].relnum].relnum != -1) && (res[tempres[0].relnum].num_of_conto > 1)){
                        tempres[0].num_of_conto+=res[tempres[0].relnum].num_of_conto;
                        tempres[0].conto= realloc(tempres[0].conto,sizeof(int)*tempres[0].num_of_conto);
                        for(j=1 ; j<tempres[0].num_of_conto ; j++){
                            tempres[0].conto[j]= res[tempres[0].relnum].conto[j-1];
                        }
                    }

                    if((res[tempres[1].relnum].relnum != -1) && (res[tempres[1].relnum].num_of_conto > 1)){
                        tempres[1].num_of_conto+=res[tempres[1].relnum].num_of_conto;
                        tempres[1].conto= realloc(tempres[1].conto,sizeof(int)*tempres[1].num_of_conto);
                        for(j=1 ; j<tempres[1].num_of_conto ; j++){
                            tempres[1].conto[j]= res[tempres[1].relnum].conto[j-1];
                        }
                    }

                    // calling updateResult to store the new results to the old ones and update the rest
                    // columns that were connected to the two newest arrays that took part in the Join
                    res = updateResult(tempres, res, relsize);
                }
            
            }
            printf("1\n");

            finalsum = (uint64_t **) realloc(finalsum, (counter2+1)*sizeof(uint64_t));
            size = realloc(size, (counter2+1)*sizeof(int));
            finalsum[counter2] = malloc(sumsize*sizeof(uint64_t));
            for(int a=0 ; a<sumsize ; a++){
                finalsum[counter2][a] = 0;
                tempr = GetColumn(res[sum[a].rel], relations[rels[sum[a].rel]][sum[a].column]);
                for(int b=0 ; b<tempr.num_tuples ; b++){
                    finalsum[counter2][a] += tempr.tuples[b].payload;
                }
                printf("results = %ld\n", finalsum[counter2][a]);
            }
            for(int a=0 ; a<sumsize ; a++){
                if(finalsum[counter2][a] == 0){
                    for(int b=0 ; b<sumsize ; b++){
                        finalsum[counter2][b] = 0;
                    }
                    break;
                }
            }
            
            size[counter2] = sumsize;
            counter2++;

            free(res);

        } else {
            // if "F" was found print results then read the next section of queries
            printf("Section ended, printing results...\n");

            counter1 = 1;
            for(int a=0 ; a<counter2 ; a++){
                printf("Query %d results are:", counter1);
                for(int b=0 ; b<size[a] ; b++){
                    if(finalsum[a][b] != 0) printf(" %ld", finalsum[a][b]);
                    else printf(" NULL");
                }
                printf("\n");
                counter1++;
            }
            counter2 = 0;
            free(size);
            free(finalsum);

            Querycounter = 0;
            Questioncounter++;
            sleep(1);
        }

        if(strcmp(line,"F\n")!=0){
            free(sum);
            free(r.rel_name);
        }

    }

    printf("All done...(not correctly but 'Oooh Well ;)'\nBye Bye now\n");

    return 0;
}

