#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <stddef.h>
#include <mpi.h>

#include "match.h"

/*
 * Creates datatype for struct Match
 */
void createMPIMatchDatatype(MPI_Datatype *match)
{
    int blocklengths[2] = {1, MAX_MATCH_SIZE + 1};
    MPI_Datatype types[2] = {MPI_INT, MPI_INT};
    MPI_Aint offsets[2] = {
        offsetof(Match, matchedNodes),
        offsetof(Match, matches),
    };

    MPI_Type_create_struct(2, blocklengths, offsets, types, match);
    MPI_Type_commit(match);
}

void prepareMatch(Match *match)
{
    match->matchedNodes = 0;
    memset(match->matches, -1, sizeof(match->matches));
}

int patternNumToGraphNum(Match* match, int patternNodeNum)
{
    return match->matches[patternNodeNum];
}

int matchContains(Match* match, int node)
{
    int i;
    for (i = 1; i <= MAX_MATCH_SIZE; ++i) {
       if (match->matches[i] == node) {
           return 1;
       }
    }
    return 0;
}

void printMatch(Match* match, FILE* out)
{
    int i;
    fprintf(out, "%d", match->matches[1]);
    for (i = 2; i <= match->matchedNodes; ++i) {
       fprintf(out, " %d", match->matches[i]);
    }
    fprintf(out, "\n");
}

void addNode(Match* match, int patternNode, int graphNode)
{
    match->matches[patternNode] = graphNode;
    match->matchedNodes++;
}

void removeNode(Match* match, int patternNode)
{
    match->matches[patternNode] = -1;
    match->matchedNodes--;
}
