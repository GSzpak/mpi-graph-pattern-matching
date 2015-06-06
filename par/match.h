#ifndef MATCH_H
#define MATCH_H

#define MAX_MATCH_SIZE 10

typedef struct {
    int matchedNodes;
    int startingProcId;
    // `matches[i] contains data graph node id of `i` node in pattern or -1 if the
    // node is not yet matched.
    int matches[MAX_MATCH_SIZE + 1];
} Match;


void prepareMatch(Match *match, int startingProcId);
int patternToGraphNode(Match* match, int node);
int matchContains(Match* match, int node);
void printMatch(Match* match, FILE* out);
Match addNode(Match* match, int graphNode, int patternNode);

#endif //MATCH_H