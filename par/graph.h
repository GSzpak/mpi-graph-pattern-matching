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
    // Number of nodes held by given worker
    int myPartNumOfNodes;
    // Index of node i in nodes array. Equal to -1, if node is not in graph
    int *nodeIndex;
    // Array of nodes. In root, node `i` is at index i - root does not
    // have to use nodeIndex array
    Node *nodes;
    // Map node -> process
    int *procForNode;
} Graph;


Node *getNode(Graph *graph, int num);
int getNodeSize(Node *node);
/*
 * Compares nodes by (inDegree + outDegree)
 */
int nodeComparator(const void *elem1, const void *elem2);
void prepareGraph(Graph *graph, int numOfNodes, int myPartNumOfNodes);
void printNodeDebug(Node *node);
void printGraphDebug(Graph *graph);
void freeNode(Node *node);
void freeGraph(Graph *graph);

#endif //GRAPH_H
