#include "acutest.h"
#include "join.h"


void test_power(void){
    TEST_CHECK_(power(2,3)==(8),"test_power(%d,%d)==%d",2,3,(8));
}

void test_hashing(void){
    TEST_CHECK_(Hashing(100,17)==(15),"test_hashing(%d,%d)==%d",100,17,(15));
}

TEST_LIST={
    {"power function:",test_power},
    {"Hashing function: ",test_hashing},
    {0}
};