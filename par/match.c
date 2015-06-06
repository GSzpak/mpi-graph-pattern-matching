#include <stdio.h>
#include <string.h>

#include "match.h"


void prepareMatch(Match *match, int startingProcId)
{
    match->startingProcId = startingProcId;
    match->matchedNodes = 0;
    memset(match->matches, -1, sizeof(match->matches));
}

int patternToGraphNode(Match* match, int node)
{
    return match->matches[node];
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

Match addNode(Match* match, int graphNode, int patternNode)
{
    Match newMatch;
    int i;
    for (i = 0; i <= MAX_MATCH_SIZE; ++i) {
        newMatch.matches[i] = match->matches[i];
    }
    newMatch.matches[patternNode] = graphNode;
    newMatch.matchedNodes = match->matchedNodes + 1;
    return newMatch;
}