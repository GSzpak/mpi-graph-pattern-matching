#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "graph.h"
#include "common.h"


Node *getNode(Graph *graph, int num)
{
    int nodeIndex = graph->nodeIndex[num];
    return &graph->nodes[nodeIndex];
}

int getNodeSize(Node *node)
{
    return sizeof(Node) + (node->inDegree + node->outDegree) * sizeof(int);
}

void prepareGraph(Graph *graph, int numOfNodes, int myPartNumOfNodes)
{
    int i;
    graph->numOfNodes = numOfNodes;
    graph->myPartNumOfNodes = myPartNumOfNodes;
    graph->nodeIndex = (int *) malloc(sizeof(int) * (graph->numOfNodes + 1)); 
    memset(graph->nodeIndex, -1, sizeof(int) * (graph->numOfNodes + 1));
    graph->nodes = (Node *) malloc(sizeof(Node) * graph->myPartNumOfNodes);
    memset(graph->nodes, 0, sizeof(Node) * graph->myPartNumOfNodes);
    graph->procForNode = (int *) malloc(sizeof(int) * (graph->numOfNodes + 1));
    memset(graph->procForNode, -1, sizeof(int) * (graph->numOfNodes + 1));
    for (i = 0; i < graph->myPartNumOfNodes; ++i) {
        graph->nodes[i].inEdges = NULL;
        graph->nodes[i].outEdges = NULL;
    }
}

void printNodeDebug(Node *node)
{
    int i;
    printf("Node %d, outDegree %d, inDegree %d\n", 
        node->num, node->outDegree, node->inDegree);
    if (node->outEdges != NULL) {
        printf("Out edges: ");
        for (i = 0; i < node->outDegree; ++i) {
            printf("%d ", node->outEdges[i]);
        }
        printf("\n");
    }
    if (node->inEdges != NULL) {
        printf("In edges: ");
        for (i = 0; i < node->inDegree; ++i) {
            printf("%d ", node->inEdges[i]);
        }    
        printf("\n");
    }
}

void printGraphDebug(Graph *graph)
{
    int i;
    printf("Overall number of nodes: %d\n", graph->numOfNodes);
    printf("My number of nodes: %d\n", graph->myPartNumOfNodes);
    for(i = 0; i < graph->myPartNumOfNodes; ++i) {
        printNodeDebug(&graph->nodes[i]);
    }
    for(i = 1; i <= graph->numOfNodes; ++i) {
        if (graph->procForNode[i] != -1) {
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
    for(i = 0; i < graph->myPartNumOfNodes; ++i) {
        freeNode(&graph->nodes[i]);
    }
    free(graph->nodeIndex);
    free(graph->nodes);
    free(graph->procForNode);    
    graph->nodeIndex = NULL;
    graph->nodes = NULL;
    graph->procForNode = NULL;
    graph->numOfNodes = 0;
    graph->myPartNumOfNodes = 0;
}