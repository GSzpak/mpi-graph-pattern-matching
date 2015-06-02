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
    // Array of nodes
    // In worker - node i is at index i
    // In root - used to sort nodes by (inDegree + outDegree)
    // to distribute nodes possibly evenly
    // If node is not in graph, then its num == -1
    Node *nodes;
    // Map node -> process
    int *procForNode;
    // int maxNodeWithOutEdgesId;
} Graph;

void printNodeDebug(Node *node);
void printGraphDebug(Graph *graph);

#endif //GRAPH_H
