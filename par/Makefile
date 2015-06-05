CC=mpicc
FLAGS=-Wall

HEADERS = utils.h graph.h
OBJECTS = utils.o graph.o gpm-par.o

default: gpm-par.exe

%.o: %.c $(HEADERS)
	$(CC) $(FLAGS) -c $< -o $@

gpm-par.exe: $(OBJECTS)
	$(CC) $(OBJECTS) $(FLAGS) -o $@

clean:
	-rm -f $(OBJECTS)
	-rm -f gpm-par.exe

