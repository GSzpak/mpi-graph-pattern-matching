CC=mpicc
CFLAGS=-O3 -Wall
LFLAGS=-O3

HEADERS = distribution.h match.h utils.h graph.h
OBJECTS = distribution.o match.o utils.o graph.o gpm-par.o

default: gpm-par.exe

%.o: %.c $(HEADERS)
	$(CC) $(CFLAGS) -c $< -o $@

gpm-par.exe: $(OBJECTS)
	$(CC) $(OBJECTS) $(LFLAGS) -o $@

clean:
	-rm -f $(OBJECTS)
	-rm -f gpm-par.exe

