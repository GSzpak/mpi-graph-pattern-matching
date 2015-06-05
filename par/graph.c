#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "graph.h"


Node *getNode(Graph *graph, int num)
{
    int nodeIndex = graph->nodeIndex[num];
    return &graph->nodes[nodeIndex];
}

int getNodeSize(Node *node)
{
    return sizeof(Node) + (node->inDegree + node->outDegree) * sizeof(int);
}

int nodeComparator(const void *elem1, const void *elem2)
{
    Node *node1 = (Node *) elem1;
    Node *node2 = (Node *) elem2;
    int sumOfDegrees1 = node1->inDegree + node1->outDegree;
    int sumOfDegrees2 = node2->outDegree + node2->outDegree;
    if (sumOfDegrees1 > sumOfDegrees2) {
        return 1;
    } else if (sumOfDegrees1 == sumOfDegrees2) {
        return 0;
    } else {
        return -1;
    }
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
/*
int dfs(int node, int nextId, int parentNode, Graph* graph, Graph* reversed,
        int* numbering, int* parent, int viaReverseEdge) {
    int currentId = nextId;
    if (viaReverseEdge) {
        numbering[node] = -currentId;
    } else {
        numbering[node] = currentId;
    }
    parent[node] = parentNode;
    for (int i = 0; i < graph->outDegrees[node]; i++) {
        if (numbering[graph->edges[node][i]] == 0) {
            currentId = dfs(graph->edges[node][i], currentId + 1, node, graph,
                            reversed, numbering, parent, 0);
        }
    }
    for (int i = 0; i < reversed->outDegrees[node]; i++) {
        if (numbering[reversed->edges[node][i]] == 0) {
            currentId = dfs(reversed->edges[node][i], currentId + 1, node, graph,
                            reversed, numbering, parent, 1);
        }
    }
    return currentId;
}
*/