#ifndef DISTRIBUTION_H
#define DISTRIBUTION_H


#define ROOT 0
#define LOCAL_READ_BUF_SIZE 64
#define MAX_NUM_OF_NODES 10000000
#define MAX_PATTERN_NODES 10
// Maximum pattern encoding size
// Check distributePattern function for more information
#define MAX_PATTERN_SIZE 1 + MAX_PATTERN_NODES * (2 + 2 * MAX_PATTERN_NODES)
// Each process has 200MB for its nodes and edges
#define SIZE_AVAILABLE 209715200
// MPI tags
#define NUM_NODES_TAG 51
#define NODE_OUT_EDGES_TAG 52
#define NODE_IN_EDGES_TAG 53


void countDegrees(FILE *inFile, Graph *graph);
void prepareGraphInRoot(FILE *inFile, Graph *graph);
void preparePatternInRoot(FILE *inFile, Graph *pattern);
void assignNodeToProc(Graph *graph, int numOfProcs, int **numOfNodesForProc);
void distributeGraph(FILE *inFile, Graph *graph, int *numOfNodesForProc,
    int numOfProcs);
void prepareGraphInWorker(Graph *graph);
void receiveOutEdges(int rank, int numOfProcs, Graph *graph);
void exchangeInEdges(int rank, int numOfProcs, Graph *graph);
void readPattern(FILE *inFile, Graph *pattern);
void broadcastPattern(Graph *pattern);
void receivePattern(Graph *pattern);


#endif //DISTRIBUTION_H
