#include "join.h"
#include <unistd.h>
#include <fcntl.h>
#include <time.h>

#define H 4
#define n1 4
#define n2 8

void Binary(int num, int **bin, int n){
    int i, *binary;
    binary=(int *)malloc(n*sizeof(int));
    for(i=0;i<n;i++){
        binary[i]=num%2;
        num=num/2;
    }
    *bin=binary;
}

int power(int base, int exp){
  int result = 1;
  while(exp!=0){
    result *= base;
    --exp;
  }
  return result;
}

void Partition(Hist **hist, relation *rel, int n){
    int i,*bin, j, count, count2 = 0;
    bucket *buckets;
    buckets = (bucket *)malloc(power(2,n) * sizeof(bucket));

    for(i=0 ; i<power(2,n) ; i++){
        buckets[i].tup = malloc(rel->num_tuples*sizeof(tuple));
    }
    bin=(int *)malloc(n*sizeof(int));
    for(i=0;i<rel->num_tuples;i++){
        Binary(rel->tuples[i].payload, &bin, n);
        for(int z=0 ; z<power(2,n) ; z++){
            count = 0;
            for(j=0 ; j<n ; j++){
                if(bin[j] == (*hist)[z].key[j]) count++;
            }
            if(count == n){
                // vazoume ta stoixeia sto antistoixo bucket tou R'
                buckets[z].tup[(*hist)[z].num].key = rel->tuples[i].key;
                buckets[z].tup[(*hist)[z].num].payload = rel->tuples[i].payload;
                (*hist)[z].num++;
                break;
            }
        }
    }
    

    // put the elements back to R'
    count=0;
    for(i=0 ; i<power(2,n) ; i++){
        for(j=0 ; j<(*hist)[i].num ; j++){
            rel->tuples[count] = buckets[i].tup[j];
            count++;
        }
    }
}

int Hopscotch(HT *hashtable, int payload, int hash_value, int rowid, int len){
    int offset = 0, flagbit = 0, flag = 0, newoffset;

    while(offset<H){

        newoffset = hash_value;

        if(hashtable->ht_buckets[newoffset].bitmap[offset] == 1){
            if(hash_value+offset >= len) newoffset = hash_value+offset-len;
            else newoffset = hash_value+offset;
            flagbit++;
            if(hashtable->ht_buckets[newoffset].payload == payload){
                hashtable->ht_buckets[newoffset].count++;
                hashtable->ht_buckets[newoffset].rowid = realloc(hashtable->ht_buckets[newoffset].rowid, hashtable->ht_buckets[newoffset].count*sizeof(int));
                hashtable->ht_buckets[newoffset].rowid[hashtable->ht_buckets[newoffset].count-1] = rowid;
                return 0; //success
            }
        }
        offset++;
    }
    if(flagbit == H) return -1; //rehash
    if(flagbit != H){
        offset = 0;
        while(offset<H){
            
            newoffset = hash_value;

            if(hashtable->ht_buckets[newoffset].bitmap[offset] == 0){
                if(hash_value+offset >= len) newoffset = hash_value+offset-len;
                else newoffset = hash_value+offset;
                if(hashtable->ht_buckets[newoffset].payload == -1){
                    hashtable->ht_buckets[newoffset].count++;
                    hashtable->ht_buckets[newoffset].rowid = realloc(hashtable->ht_buckets[newoffset].rowid, (hashtable->ht_buckets[newoffset].count)*sizeof(int));
                    hashtable->ht_buckets[newoffset].rowid[hashtable->ht_buckets[newoffset].count-1] = rowid;
                    hashtable->ht_buckets[newoffset].payload = payload;
                    hashtable->ht_buckets[newoffset].hash_value = hash_value;
                    hashtable->ht_buckets[hash_value].bitmap[offset] = 1;
                    return 0; // success
                }
            }
            offset++;
        }

        if(offset == H){

            // finding the first empty bucket in the HT
            int counter=0;
            offset = hash_value+H;
            while(counter < len){
                if(hashtable->ht_buckets[offset].payload == -1) break;
                offset++;
                if(offset > len) offset = offset-len-1;
                counter++;
            }
            
            // if the counter == len it means it did a full traverse of the HT and not empty was found
            if(counter == len) {
                return -1;
            } // rehash

            // searches the bitmap of the bucket left of the empty slot to see if we can slide its elements to the right
            while(1){
                offset--;
                if(offset < 0) return -1; //rehash
                int i = 0;
                while(i<H){
                    if(hashtable->ht_buckets[hashtable->ht_buckets[offset].hash_value].bitmap[i] == 1) break;
                    i++;
                }
                
                if(i == H) {
                    return -1;
                }
                if(i < H-1) {
                    i++;
                    while(i<H){
                        if(hashtable->ht_buckets[hashtable->ht_buckets[offset].hash_value].bitmap[i] == 0) {
                            if(hashtable->ht_buckets[hashtable->ht_buckets[offset].hash_value+i].payload == -1){
                                // h geitonia mporei na kanei olisthisi
                                while(i>=0){
                                    memcpy(&(hashtable)->ht_buckets[hashtable->ht_buckets[offset].hash_value+i], &(hashtable)->ht_buckets[hashtable->ht_buckets[offset].hash_value+i-1], sizeof(HT_bucket));
                                    hashtable->ht_buckets[hashtable->ht_buckets[offset].hash_value+i-1].payload = -1;
                                    hashtable->ht_buckets[hashtable->ht_buckets[offset].hash_value].bitmap[i-1] = 0;
                                    hashtable->ht_buckets[hashtable->ht_buckets[offset].hash_value+i-1].count = 0;
                                    // free(hashtable->ht_buckets[hashtable->ht_buckets[offset].hash_value+i-1].rowid);
                                    if(hashtable->ht_buckets[offset].hash_value+i-1 - hash_value < H) {
                                        // there is now space for new insertion in HT so clear that bucket and recursive call of hopscotch
                                        if(Hopscotch(hashtable, payload, hash_value, rowid, len) == -1) {
                                            return -1;
                                        } // rehash
                                        else return 0;
                                    }
                                    i--;
                                }
                                break;
                            }
                        }
                        i++;
                    }
                }
            }
        }
    }
}

int Hashing(int payload, int len){
    return payload % len;
}

HT HashTableFunction(relation rel, int start, int length, int *size){

    HT HashTable;
    HashTable.ht_buckets = malloc((*size) *sizeof(HT_bucket));
    for(int i=0 ; i<*size ; i++){
        HashTable.ht_buckets[i].hash_value = i;
        HashTable.ht_buckets[i].count = 0;
        HashTable.ht_buckets[i].payload = -1;
        HashTable.ht_buckets[i].rowid = malloc(sizeof(int));
        for(int j=0;j<H;j++){
            HashTable.ht_buckets[i].bitmap[j]=0;
        }
    }

    int temp;

    // running hash function for the elements of current partion of R 
    // and creating hash table
    for(int i=start ; i<start+length ; i++){

        temp = Hashing(rel.tuples[i].payload, *size);
        int exitvalue = Hopscotch(&HashTable, rel.tuples[i].payload, temp, rel.tuples[i].key, *size);
        if( exitvalue == -1){
            // change size of HT
            // free(HashTable.ht_buckets);
            (*size) = 2*(*size);
            HashTable = HashTableFunction(rel, start, length, size);
            return HashTable;
        } else continue;

    }

    return HashTable;
    
}

result* Probe(HT hashtable, relation relS, int len, int hashlen, int start){

    // int fd = open("results.txt", O_APPEND | O_CREAT | O_RDWR, 777);
    // dup2(fd, STDOUT_FILENO);

    result *r;
    r = (result *)malloc(2*sizeof(result));
    r[0].rowid = malloc(sizeof(int));
    r[1].rowid = malloc(sizeof(int));
    r[0].count = 0;
    r[1].count = 0;
    r[0].relnum = -1;
    r[1].relnum = -1;

    int hashS, z;

    for(int i=start ; i<start+len ; i++){
        hashS = Hashing(relS.tuples[i].payload, hashlen);
        for(int j=0 ; j<H ; j++){
            if(hashS+j > hashlen) hashS = hashS+j - hashlen;
            if(hashtable.ht_buckets[hashS].bitmap[j] == 1) {
                if((hashtable.ht_buckets[hashS+j].hash_value == hashS )&&(hashtable.ht_buckets[hashS+j].payload==relS.tuples[i].payload)) {
                    z = 0;
                    while(z < hashtable.ht_buckets[hashS+j].count){
                        z++;
                        r[0].rowid = realloc(r[0].rowid, (r[0].count+1)*sizeof(int));
                        r[1].rowid = realloc(r[1].rowid, (r[1].count+1)*sizeof(int));
                        r[0].rowid[r[0].count] = hashtable.ht_buckets[hashS+j].rowid[hashtable.ht_buckets[hashS].count-1];
                        r[1].rowid[r[1].count] = relS.tuples[i].key;
                        r[0].count++;
                        r[1].count++;
                        // printf("rowidR (%d) = rowidS (%d)\n",hashtable.ht_buckets[hashS+j].rowid[hashtable.ht_buckets[hashS].count-1], relS.tuples[i].key);
                    }
                    break;
                }
            }
        }
    }

    // close(fd);

    return r;
}

result* PartitionedHashJoin(relation relS, relation resR){
    
    // result *res;
    // res = malloc(2*sizeof(result));
    // res[0].relnum = -1;
    // res[1].relnum = -1;
    // res[0].count = 0;
    // res[1].count = 0;
    // res[0].rowid = malloc(sizeof(int));
    // res[1].rowid = malloc(sizeof(int));

    // for(int a=0 ; a<relS.num_tuples ; a++){
    //     for(int b=0 ; b<resR.num_tuples ; b++){
    //         // if(relS.num_tuples == 3573 && resR.num_tuples == 123793) printf("la\n");
    //         if(relS.tuples[a].payload == resR.tuples[b].payload){
    //             // if(relS.num_tuples == 3573 && resR.num_tuples == 123793) printf("lala\n");
    //             res[0].rowid = realloc(res[0].rowid, (res[0].count+1)*sizeof(int));
    //             res[1].rowid = realloc(res[1].rowid, (res[1].count+1)*sizeof(int));
    //             res[0].rowid[res[0].count] = relS.tuples[a].key;
    //             res[1].rowid[res[1].count] = resR.tuples[b].key;
    //             res[0].count++;
    //             res[1].count++;
    //         }
    //     }
    // }
    
    // return res;    
    
    int flagR=0,flagS=0, j;
    Hist *histS, *histR;
    clock_t start, end;
    char str[500];

    // doing partition process for table S
    if(sizeof(tuple)*(relS.num_tuples)> CACHE){
        histS= (Hist *) malloc(power(2,n1)*sizeof(Hist));
        for(j=0;j<power(2,n1);j++){
            histS[j].key = (int *) malloc(n1*sizeof(int));
            histS[j].num = 0;
            Binary(j, &(histS[j].key), n1);
        }

        //call partition function for relS
        Partition(&histS, &relS, n1);


        flagS=n1;
        for(j=0 ; j<power(2,n1) ; j++){
            if(sizeof(tuple)*histS[j].num > CACHE){
                break;
            }
        }
        
        if(j == power(2, n1)){
        //2nd partition

            for(j=0 ; j<power(2,n1) ; j++){
                free(histS[j].key);
            }
            free(histS);
            histS= (Hist *) malloc(power(2,n2)*sizeof(Hist));
            for(j=0;j<power(2,n2);j++){
                histS[j].key = (int *) malloc(n2*sizeof(int));
                histS[j].num = 0;
                Binary(j, &histS[j].key, n2);
            }

            Partition(&histS, &relS, n2);

            flagS=n2;
            
        }
    }
    // doing partiotion process for table R
    if(sizeof(tuple)*(resR.num_tuples)> CACHE){
        histR= (Hist *) malloc(power(2,n1)*sizeof(Hist));
        for(j=0;j<power(2,n1);j++){
            histR[j].key = (int *) malloc(n1*sizeof(int));
            histR[j].num = 0;
            Binary(j, &histR[j].key, n1);
        }

        //call partition function for relS
        Partition(&histR, &resR, n1);

        flagR = n1;
        //call partition function for resR
        for(j=0 ; j<power(2,n1) ; j++){
            if(sizeof(tuple)*histR[j].num > CACHE){
                break;
            }
        }
        if(j==power(2,n1)){
            //2nd partition
            for(j=0 ; j<power(2,n1) ; j++){
                free(histR[j].key);
            }
            free(histR);
            histR= (Hist *) malloc(power(2,n2)*sizeof(Hist));
            for(j=0;j<power(2,n2);j++){
                histR[j].key = (int *) malloc(n2*sizeof(int));
                histR[j].num = 0;
                Binary(j, &histR[j].key, n2);
            }

            Partition(&histR, &resR, n2);

            flagR = n2;
        }
    }
    // making prefix hists for both tables
    Hist *psumS, *psumR;

    if(flagS != 0){
        psumS=(Hist *) malloc(power(2,flagS)*sizeof(Hist));
        psumS->key = (int *) malloc(flagS*sizeof(int));
        psumS->key = histS->key;
        psumS[0].num=0;
        for(int i =1; i<power(2,flagS); i++){
            psumS[i].num=histS[i-1].num+psumS[i-1].num;
        }
    }
    if(flagR != 0) {
        psumR=(Hist *) malloc(power(2,flagR)*sizeof(Hist));
        psumR->key = (int *) malloc(flagR*sizeof(int));
        psumR->key = histR->key;
        psumR[0].num=0;
        for(int i =1; i<power(2,flagR) ; i++){
            psumR[i].num = histR[i-1].num + psumR[i-1].num;
        }
    }

    // double the last n and do hash(i)= i % n
    HT HashTable;
    result *r, *results;
    results = (result *)malloc(2*sizeof(result));
    results[0].count = 0;
    results[1].count = 0;
    results[0].rowid = malloc(sizeof(int));
    results[1].rowid = malloc(sizeof(int));
    int i, size;
    double timeHash = 0, timeProbe = 0;

    if(flagR != flagS) printf("error\n");
    if(flagR == 0 && flagS != 0){
        size = resR.num_tuples;
        HashTable = HashTableFunction(resR, 0, resR.num_tuples, &size);
        for(i=0 ; i<flagS ; i++){
            r = Probe(HashTable, relS, histS[i].num, size, psumS[i].num);
            results[0].rowid = realloc(results[0].rowid, (results[0].count+r[0].count)*sizeof(int));
            results[1].rowid = realloc(results[1].rowid, (results[1].count+r[1].count)*sizeof(int));
            for(int z=0 ; z<r[0].count ; z++){
                results[0].rowid[results[0].count] = r[0].rowid[z];
                results[1].rowid[results[1].count] = r[1].rowid[z];
                results[0].count++;
                results[1].count++;
            }
        }
        return results;
    } else if(flagS == 0 && flagR != 0){
        for(i=0 ; i<power(2, flagR) ; i++){
            size = histR[i].num;
            HashTable = HashTableFunction(resR, psumR[i].num, histR[i].num, &size);
            r = Probe(HashTable, relS, relS.num_tuples, size, 0);
            results[0].rowid = realloc(results[0].rowid, (results[0].count+r[0].count)*sizeof(int));
            results[1].rowid = realloc(results[1].rowid, (results[1].count+r[1].count)*sizeof(int));
            for(int z=0 ; z<r[0].count ; z++){
                results[0].rowid[results[0].count] = r[0].rowid[z];
                results[1].rowid[results[1].count] = r[1].rowid[z];
                results[0].count++;
                results[1].count++;
            }
        }
        return results;
    } else if(flagR == 0 && flagS == 0){
        size = resR.num_tuples;
        HashTable = HashTableFunction(resR, 0, resR.num_tuples, &size);
        r = Probe(HashTable, relS, relS.num_tuples, size, 0);
        return r;
    }

    for(i=0 ; i<power(2,flagR) ; i++){
        if(histR[i].num != 0){

            size = histR[i].num;
            HashTable = HashTableFunction(resR, psumR[i].num, histR[i].num, &size);
            if(flagR<flagS){
                for(j=0 ; j< power(2, flagS)/power(2, flagR) - 1 ; j++){
                    r = Probe(HashTable, relS, histS[j].num, size, psumS[j].num);
                    results[0].rowid = realloc(results[0].rowid, (results[0].count+r[0].count)*sizeof(int));
                    results[1].rowid = realloc(results[1].rowid, (results[1].count+r[1].count)*sizeof(int));
                    for(int z=0 ; z<r[0].count ; z++){
                        results[0].rowid[results[0].count] = r[0].rowid[z];
                        results[1].rowid[results[1].count] = r[1].rowid[z];
                        results[0].count++;
                        results[1].count++;
                    }
                }
            } else if(flagR > flagS) {
                r = Probe(HashTable, relS, histS[i % power(2, flagS)].num, size, psumS[i % power(2, flagS)].num);
                results[0].rowid = realloc(results[0].rowid, (results[0].count+r[0].count)*sizeof(int));
                results[1].rowid = realloc(results[1].rowid, (results[1].count+r[1].count)*sizeof(int));
                for(int z=0 ; z<r[0].count ; z++){
                    results[0].rowid[results[0].count] = r[0].rowid[z];
                    results[1].rowid[results[1].count] = r[1].rowid[z];
                    results[0].count++;
                    results[1].count++;
                }
            } else {
                r = Probe(HashTable, relS, histS[i].num, size, psumS[i].num);
                results[0].rowid = realloc(results[0].rowid, (results[0].count+r[0].count)*sizeof(int));
                results[1].rowid = realloc(results[1].rowid, (results[1].count+r[1].count)*sizeof(int));
                for(int z=0 ; z<r[0].count ; z++){
                    results[0].rowid[results[0].count] = r[0].rowid[z];
                    results[1].rowid[results[1].count] = r[1].rowid[z];
                    results[0].count++;
                    results[1].count++;
                }
            }
        }
    }

    return results;

}

result Compare(relation rel,char operator,int num){
    result res;
    res.rowid= malloc(sizeof(int));
    res.count=0;
    if(operator == '>') {
        for(int i=0;i<rel.num_tuples;i++){
            if(rel.tuples[i].payload > num){
                res.count++;
                res.rowid=realloc(res.rowid,(res.count+1)*sizeof(int));
                res.rowid[res.count-1]=rel.tuples[i].key;
            }
        }
    } else if(operator == '<') {
        for(int i=0;i<rel.num_tuples;i++){
            if(rel.tuples[i].payload < num){
                res.count++;
                res.rowid=realloc(res.rowid,(res.count+1) *sizeof(int));
                res.rowid[res.count-1]=rel.tuples[i].key;
            }
        }
    } else {
        for(int i=0;i<rel.num_tuples;i++){
            if(rel.tuples[i].payload == num){
                res.count++;
                res.rowid=realloc(res.rowid,(res.count+1) *sizeof(int));
                res.rowid[res.count-1]=rel.tuples[i].key;
            }
        }
    }

    return res;
}

result *updateResult(result *new_res, result *old_res, int x){

    result *tmp= malloc(x*sizeof(result));

    int i,j,k,z=0;
    for(i=0;i<x;i++){
        tmp[i].relnum=-1;
        tmp[i].num_of_conto = 0;
        tmp[i].conto = malloc(sizeof(int));
    }

    tmp[new_res[0].relnum] = new_res[0];
    tmp[new_res[1].relnum] = new_res[1];

    for(int p=0 ; p<2 ; p++){
        for(i=1;i<new_res[p].num_of_conto;i++){
            // old_res[new_res[0].conto[i]]
            z=new_res[p].conto[i];
            tmp[z].rowid=malloc(new_res[p].count*sizeof(int));

            tmp[z].relnum=z;

            for(j=0;j<new_res[p].count;j++){
                for(k=0;k<old_res[z].count;k++){
                    if(new_res[p].rowid[j]==old_res[new_res[p].relnum].rowid[k]){
                        tmp[z].rowid[j]=old_res[z].rowid[k];
                        j++;
                    }
                }
            }
        }
    }

    for(i=0 ; i<x ; i++){
        if(tmp[i].relnum == -1 && old_res[i].relnum != -1){
            tmp[i] = old_res[i];
        }
    }

    return tmp;

}