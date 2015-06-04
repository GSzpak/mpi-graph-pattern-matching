#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "graph.h"
#include "common.h"


// TODO: move procForNode outside
// FIXME: numOfNodes in workers
void prepareGraph(Graph *graph)
{
    int i;
    graph->nodesInGraph = (int *) malloc(sizeof(int) * (graph->numOfNodes + 1)); 
    graph->nodes = (Node *) malloc(sizeof(Node) * (graph->numOfNodes + 1));
    graph->procForNode = (int *) malloc(sizeof(int) * (graph->numOfNodes + 1));
    memset(graph->nodesInGraph, 0, sizeof(int) * (graph->numOfNodes + 1));
    memset(graph->nodes, 0, sizeof(Node) * (graph->numOfNodes + 1));
    memset(graph->procForNode, 0, sizeof(int) * (graph->numOfNodes + 1));
    for (i = 1; i <= graph->numOfNodes; ++i) {
        graph->nodes[i].num = i;
    }
}

void printNodeDebug(Node *node)
{
    int i;
    printf("Node %d, outDegree %d, inDegree %d\n", 
        node->num, node->outDegree, node->inDegree);
    printf("Out edges: ");
    for (i = 0; i < node->outDegree; ++i) {
        printf("%d ", node->outEdges[i]);
    }
    printf("\nIn edges: ");
    for (i = 0; i < node->inDegree; ++i) {
        printf("%d ", node->inEdges[i]);
    }
    printf("\n");
}

void printGraphDebug(Graph *graph)
{
    int i;
    printf("Number of nodes: %d\n", graph->numOfNodes);
    for(i = 1; i <= graph->numOfNodes; ++i) {
        if (graph->nodesInGraph[i]) {
            printNodeDebug(&graph->nodes[i]);
        }
        if (graph->procForNode[i] != 0) {
            printf("Node %d in process %d\n", i, graph->procForNode[i]);    
        }
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
        //if (graph->nodesInGraph[i] == 1) {
            freeNode(&graph->nodes[i]);
        //}
    }
    free(graph->nodesInGraph);
    free(graph->nodes);
    free(graph->procForNode);
    graph->nodesInGraph = NULL;
    graph->nodes = NULL;
    graph->procForNode = NULL;
    graph->numOfNodes = 0;
}