#ifndef GRAPH_H
#define GRAPH_H

/**
 * Directed graph data structure.
 */
typedef struct {
    // For each node an array of outgoing edges
    int **outEdges;
    // For each node an array of outgoing edges
    int **inEdges;
    // Out-degrees for nodes or -1 if the node is not in the graph.
    int *outDegrees;
    // Out-degrees for nodes or -1 if the node is not in the graph.
    int *inDegrees;
    // Map numberOfNode -> numberOfProcess
    int *procForNode;
    int numOfNodes;
    // int maxNodeWithOutEdgesId;
} Graph;

/*
Graph* readGraph(FILE* f);

void printGraph(Graph* g);

void freeGraph(Graph* graph);

Graph* reverseGraph(Graph* graph);

int dfs(int node, int nextId, int parentNode, Graph* graph, Graph* reversed,
        int* numbering, int* parent, int throughReverseEdge);
*/
#endif //GRAPH_H
