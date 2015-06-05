#ifndef GRAPH_H
#define GRAPH_H

/**
 * Data structure for sorting nodes by sum of degrees
 */
typedef struct {
    int num;
    int *outEdges;
    int *inEdges;
    int outDegree;
    int inDegree;
} Node;

/**
 * Directed graph data structure.
 */
typedef struct {
    int numOfNodes;
    // nodesInGraph[i] == 1, when node i is in the graph,
    // and 0 otherwise
    int *nodesInGraph;
    // FIXME: compress and add array of indices
    // Array of nodes
    // In worker - node i is at index i
    // In root - used to sort nodes by (inDegree + outDegree)
    // to distribute nodes possibly evenly
    // If node is not in graph, then its num == -1
    Node *nodes;
    // Map node -> process
    int *procForNode;
} Graph;


void prepareGraph(Graph *graph);
void printNodeDebug(Node *node);
void printGraphDebug(Graph *graph);
void freeNode(Node *node);
void freeGraph(Graph *graph);

#endif //GRAPH_H
