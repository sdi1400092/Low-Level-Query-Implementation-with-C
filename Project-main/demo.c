#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <fcntl.h>
#include "join.h"

int main(void) {

   relation rel[3];

   int fd = open("r0", O_RDWR);
   if(fd == -1) {
      printf("open failed\n");
      return 1;
   }

   FILE *f = fopen("r0", "r");
   fseek(f, 0, SEEK_END);
   int size = ftell(f);
   fseek(f, 0, SEEK_SET);
   fclose(f);

   int *p = mmap(NULL, size*sizeof(int), PROT_READ, MAP_PRIVATE, fd, 0);

   int numtuples = p[0], numcolumn = p[2];

   int j = 0;
   for(int i=4 ; i<=2*numtuples+2 ; i+=2){
      // printf("%d | %d | %d\n", p[i], p[i+numtuples*2], p[i+4*numtuples]);
      rel[0].tuples[j] = p[i];
      rel[1].tuples[j] = p[i+numtuples*2];
      rel[2].tuples[j] = p[i+4*numtuples];
      j++;
   }

   int err = munmap(p, 2*sizeof(int));
   if(err != 0) {
      printf("unmap failed\n");
      return 1;
   }

   close(fd);

    return 0;

}