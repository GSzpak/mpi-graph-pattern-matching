#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>
#include <assert.h>

#include "graph.h"
#include "common.h"

#define MAX_NUM_OF_NODES 10000000
#define SIZE_AVAILABLE 20
#define ROOT 0
#define META_INF_SIZE 3


static const int NUM_NODES_TAG = 49;
static const int NODE_EDGES_TAG = 50;


void prepareGraph(Graph *graph)
{
    graph->nodesInGraph = (int *) malloc(sizeof(int) * (graph->numOfNodes + 1));
    graph->nodes = (Node *) malloc(sizeof(Node) * (graph->numOfNodes + 1));
    graph->procForNode = (int *) malloc(sizeof(int) * (graph->numOfNodes + 1));
    memset(graph->nodesInGraph, 0, sizeof(int) * (graph->numOfNodes + 1));
    memset(graph->nodes, 0, sizeof(Node) * (graph->numOfNodes + 1));
    memset(graph->procForNode, 0, sizeof(int) * (graph->numOfNodes + 1));
}

// Called only in root
void countDegrees(FILE *inFile, Graph *graph)
{
    int actNode, actOutDeg, actNeighbour, i, j;
    for (i = 0; i < graph->numOfNodes; ++i) {
        fscanf(inFile, "%d %d\n", &actNode, &actOutDeg);
        for (j = 0; j < actOutDeg; ++j) {
            fscanf(inFile, "%d\n", &actNeighbour);
            graph->nodes[actNeighbour].inDegree += 1;
            graph->nodes[actNode].outDegree += 1;
        }
    }
}

// compare nodes by (inDegree + outDegree)
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

int getNodeSize(Node *node)
{
    return (node->inDegree + node->outDegree + 2) * sizeof(int);
}

// Assigns nodes to processes evenly by (inDegree + outDegree)
void assignNodeToProc(Graph *graph, int numOfProcs, 
    int **numOfNodesForProc)
{
    int i, actNodeNum, actProc, forward, actSize;
    Node *actNode;
    int remainingSpace[numOfProcs];
    *numOfNodesForProc = (int *) malloc(sizeof(int) * numOfProcs);
    memset(*numOfNodesForProc, 0, sizeof(int) * numOfProcs);
       
    for (i = 0; i < numOfProcs; ++i) {
        remainingSpace[i] = SIZE_AVAILABLE;
    }
    qsort((void *) &graph->nodes, graph->numOfNodes, 
        sizeof(Node), nodeComparator);

    i = graph->numOfNodes;
    actProc = 0;
    // boolean flag inidicating if we are going forward or backward while
    // choosing the next process
    forward = 1;
    while (i > 0) {
        actNode = &graph->nodes[i];
        actNodeNum = actNode->num;
        actSize = getNodeSize(actNode);
        if (actProc != ROOT && actSize >= remainingSpace[actProc]) {
            graph->procForNode[actNodeNum] = actProc;
            (*numOfNodesForProc)[actProc]++;
            remainingSpace[actProc] -= actSize;
            i--;
        }
        if (actProc == 0 && !forward) {
            forward = 1;
        } else if (actProc == numOfProcs - 1 && forward) {
            forward = 0;
        } else {
            actProc = forward ? actProc + 1 : actProc - 1;
        }
    }
}

void prepareForDistribution(int *numOfNodesForProc, int numOfProcs)
{
    int actProc;
    MPI_Request requests[numOfProcs - 1];
    // Informs all processes how many nodes they will receive 
    for (actProc = 0; actProc < numOfProcs; ++actProc) {
        if (actProc != ROOT) {
            MPI_Isend(numOfNodesForProc + actProc, 1, MPI_INT, actProc,
                NUM_NODES_TAG, MPI_COMM_WORLD, requests + actProc);
        }
    }
    MPI_Waitall(numOfProcs - 1, requests, MPI_STATUSES_IGNORE);
}

// Called in root
// TODO: send in edges
void distributeGraph(FILE *inFile, Graph *graph, int *numOfNodesForProc,
    int numOfProcs)
{
    prepareForDistribution(numOfNodesForProc, numOfProcs);
    int actProc, actNode, actOutDeg, i, j, temp;
    // Buffer used to send node's outgoing edges to a process
    // At first index will be sent number of node, 
    // then the outgoing edges
    int tempBuf[graph->numOfNodes + 1];
    MPI_Request request;

    fscanf(inFile, "%d\n", &temp);
    assert(temp == graph->numOfNodes);

    // Assumes, that input is encoded correctly
    for (i = 1; i <= graph->numOfNodes; ++i) {
        fscanf(inFile, "%d %d\n", &actNode, &actOutDeg);
        actProc = graph->procForNode[actNode];
        if (i > 1) {
            // Wait for previous send to complete
            MPI_Wait(&request, MPI_STATUSES_IGNORE);
        }
        tempBuf[0] = actNode;
        for (j = 0; j < actOutDeg; ++j) {
            fscanf(inFile, "%d\n", tempBuf + j + 1);
        }
        MPI_Isend(tempBuf, actOutDeg + 1, MPI_INT, actProc,
            NODE_EDGES_TAG, MPI_COMM_WORLD, &request);
    }
    // Wait for last send to complete
    MPI_Wait(&request, MPI_STATUSES_IGNORE);
}

// Called in workers
void receiveGraph(Graph *graph)
{
    int numOfNodes, i, receivedCount, actNode, actOutDeg;
    MPI_Status status;

    MPI_Recv(&numOfNodes, 1, MPI_INT, ROOT, NUM_NODES_TAG,
        MPI_COMM_WORLD, &status);
    
    // Buffer to receive node with outgoing edges
    int tempBuf[MAX_NUM_OF_NODES + 1];
    for (i = 0; i < numOfNodes; ++i) {
        MPI_Recv(tempBuf, MAX_NUM_OF_NODES + 1, MPI_INT, ROOT,
            NODE_EDGES_TAG, MPI_COMM_WORLD, &status);
        MPI_Get_count(&status, MPI_INT, &receivedCount);
        actOutDeg = receivedCount - 1;
        actNode = tempBuf[0];
        graph->nodesInGraph[actNode] = 1;
        graph->nodes[actNode].num = actNode;
        graph->nodes[actNode].outDegree = actOutDeg;
        graph->nodes[actNode].outEdges = (int *) malloc(sizeof(int) * actOutDeg);
        memset(graph->nodes[actNode].outEdges, 0, sizeof(int) * actOutDeg);
    }
}

int main(int argc, char **argv)
{
    if (argc != 3) {
        error("Wrong number of arguments.");
    }

    int rank;
    int numOfProcs;
    Graph graph;
    Graph pattern;
    char *inFileName = argv[1];
    char *outFileName = argv[2];
    FILE *inFile = NULL;
    FILE *outFile = NULL;
    // array informing, how many nodes will be sent to each process
    // used only in root
    int *numOfNodesForProc = NULL;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &numOfProcs);

    if (rank == ROOT) {
        inFile = fopen(inFileName, "r");
        if (inFile == NULL) {
            MPI_Finalize();
            error("Can't open input file %s\n", inFileName);
        }
        outFile = fopen(outFileName, "w");
        if (outFile == NULL) {
            error("Can't open output file.\n");
        }
    }

    if (rank == ROOT) {
        fscanf(inFile, "%d\n", &graph.numOfNodes);
        MPI_Bcast(&graph.numOfNodes, 1, MPI_INT, ROOT, MPI_COMM_WORLD);
        prepareGraph(&graph);
        countDegrees(inFile, &graph);
        assignNodeToProc(&graph, numOfProcs, &numOfNodesForProc);
        rewind(inFile);
        distributeGraph(inFile, &graph, numOfNodesForProc, numOfProcs);
        //readAndBroadcastPattern(inFile, &pattern);
        fclose(inFile);
    } else {
        MPI_Bcast(&graph.numOfNodes, 1, MPI_INT, ROOT, MPI_COMM_WORLD);
        prepareGraph(&graph);
        receiveGraph(&graph);
        //receivePattern(&pattern);
    }

    // patternMatch()

    if (rank == ROOT) {
        fclose(outFile);
    }

    MPI_Finalize();

    return 0;
}