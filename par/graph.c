#include <stdio.h>
#include <stdlib.h>

#include "graph.h"
#include "common.h"


void printNodeDebug(Node *node)
{
    int i;
    printf("Node %d, outDegree %d, inDegree %d\n", 
        node->num, node->outDegree, node->inDegree);
    /*
    printf("Out edges: ");
    for (i = 0; i < node->outDegree; ++i) {
        printf("%d ", node->outEdges[i]);
    }
    printf("\nIn edges: ");
    for (i = 0; i < node->inDegree; ++i) {
        printf("%d ", node->inEdges[i]);
    }
    printf("\n");
    */
}

void printGraphDebug(Graph *graph)
{
    int i;
    printf("Number of nodes: %d\n", graph->numOfNodes);
    for(i = 1; i <= graph->numOfNodes; ++i) {
        //if (graph->nodesInGraph[i]) {
            printNodeDebug(&graph->nodes[i]);
        //}
    }
}

void freeNode(Node *node) {
    free(node->outEdges);
    free(node->inEdges);
    node->outEdges = NULL;
    node->inEdges = NULL;
    node->outDegree = 0;
    node->inDegree = 0;
    node->num = -1;
}

void freeGraph(Graph *graph) {
    int i;
    for(i = 0; i <= graph->numOfNodes; ++i) {
        freeNode(&graph->nodes[i]);
    }
    free(graph->nodesInGraph)
    free(graph->nodes);
    free(graph->procForNode);
    graph->numOfNodes = 0;
}