#ifndef GRAPH_H
#define GRAPH_H

/**
 * Directed grapg node data structure
 */
typedef struct {
    int num;
    int *outEdges;
    int *inEdges;
    int outDegree;
    int inDegree;
} Node;

/**
 * Directed graph data structure
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


/*
 * Compares nodes by (inDegree + outDegree)
 */
int nodeComparator(const void *elem1, const void *elem2);
int containsOutEdge(Node *node, int destNodeNum);
int containsInEdge(Node *node, int sourceNodeNum);
Node *getNode(Graph *graph, int num);
int getNodeSize(Node *node);
void prepareGraph(Graph *graph, int numOfNodes, int myPartNumOfNodes);
void printNodeDebug(Node *node);
void printGraphDebug(Graph *graph);
void freeNode(Node *node);
void freeGraph(Graph *graph);
void undirectedDfs(int source, Graph *graph, int *dfsOrder, int *parents);
int isInGraph(Graph *graph, int nodeNum);
void copyNodeToBuffer(Graph *graph, int nodeNum, int *buffer, int *bufIndex);
void readReceivedNode(Node *node, int nodeNum, int *nodeBuffer,
    int *bufferActIndex);
void reproduceNode(Node *node, int nodeNum, int *inEdgesBuffer, int inDegree,
    int *outEdgesBuffer, int outDegree);


#endif //GRAPH_H
