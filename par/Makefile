CC=mpicc
FLAGS=-Wall

HEADERS = common.h graph.h
OBJECTS = common.o graph.o gpm-par.o

default: gpm-par.exe

%.o: %.c $(HEADERS)
	$(CC) $(FLAGS) -c $< -o $@

gpm-par.exe: $(OBJECTS)
	$(CC) $(OBJECTS) $(FLAGS) -o $@

clean:
	-rm -f $(OBJECTS)
	-rm -f gpm-par.exe

