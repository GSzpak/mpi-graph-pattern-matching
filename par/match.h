#ifndef MATCH_H
#define MATCH_H

#include "graph.h"

#define MAX_MATCH_SIZE 10

typedef struct {
    int matchedNodes;
    int nextGraphNode;
    int nextPatternNode;
    // `matches[i] contains data graph node id of `i` node in pattern or -1 if the
    // node is not yet matched.
    int matches[MAX_MATCH_SIZE + 1];
} Match;


/*
 * Creates datatype for struct Match
 */
void createMPIMatchDatatype();
void prepareMatch(Match *match);
int patternNumToGraphNum(Match* match, int patternNodeNum);
int matchContains(Match* match, int node);
void printMatch(Match* match, FILE* out);
void addNode(Match* match, int patternNode, int graphNode);
void removeNode(Match* match, int patternNode);


#endif //MATCH_H