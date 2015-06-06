CC=mpicc
FLAGS=-Wall

HEADERS = match.h utils.h graph.h
OBJECTS = match.o utils.o graph.o gpm-par.o

default: gpm-par.exe

%.o: %.c $(HEADERS)
	$(CC) $(FLAGS) -c $< -o $@

gpm-par.exe: $(OBJECTS)
	$(CC) $(OBJECTS) $(FLAGS) -o $@

clean:
	-rm -f $(OBJECTS)
	-rm -f gpm-par.exe

