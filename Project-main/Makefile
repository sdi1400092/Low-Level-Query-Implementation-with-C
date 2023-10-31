EXEC1 = main.c

OBJ1 = main.o

OBJ2 = join.o

OBJ3 = parcer.o

OBJ4 = test.o

PROGRAM = ergasia

MODULE1 = join.c

MODULE2 = parcer.c

TEST = test.c

TEXT= time.txt

PARAMETER = small.work

INPUT = small.init

CC = gcc

PROGRAM: $(OBJ1) $(OBJ2) $(OBJ3)
	$(CC) -o $(PROGRAM) $(OBJ1) $(OBJ2) $(OBJ3)

$(OBJ1): $(EXEC1)
	$(CC) -c $(EXEC1)

$(OBJ2): $(MODULE1)
	$(CC) -c $(MODULE1)

$(OBJ3): $(MODULE2)
	$(CC) -c $(MODULE2)

run: $(PROGRAM)
	./$(PROGRAM) $(PARAMETER) < $(INPUT)

test:
	$(CC) -c $(TEST)
	$(CC) -c $(MODULE1)
	$(CC) -o test $(OBJ4) $(OBJ2)

time: $(TEXT)
	cat $(TEXT)

.PHONY: clean

clean:
	rm -f $(PROGRAM) *.o results.txt test time.txt