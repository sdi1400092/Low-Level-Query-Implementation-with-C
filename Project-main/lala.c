#include "parser.h"

int main(void){

    relation *rel1, *rel2;
    rel1 = Mapping("r1");
    rel2 = Mapping("r11");

    result res1, res2;
    res1.relnum = -1;
    res2.relnum = -1;
    relation relS, relR;
    relS = GetColumn(res1, rel1[0]);
    relR = GetColumn(res2, rel2[1]);

    result *res;
    res = PartitionedHashJoin(relS, res1, relR, res2);
    printf("%d\n", res[0].count);

    return 0;

}