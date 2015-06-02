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
static const int SEND_NODE_META_TAG = 50;
static const int SEND_NODE_NEIGHBOURS_TAG = 51;


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

int getNodeSize(Graph *graph, int numOfNode)
{
    Node *node = &graph->nodes[numOfNode];
    return (node->inDegree + node->outDegree + 2) * sizeof(int);
}

// Assigns nodes to processes evenly by (inDegree + outDegree)
void prepareForDistribution(Graph *graph, int numOfProcs, int **numOfNodesForProc)
{
    int i, actNodeNum, actProc, forward, actSize;
    int remainingSpace[numOfProcs];
    *numOfNodesForProc = (int *) malloc(sizeof(int) * numOfProcs);
    memset(*numOfNodesForProc, 0, sizeof(int) * numOfProcs);
       
    for (i = 0; i < numOfProcs; ++i) {
        remainingSpace[i] = SIZE_AVAILABLE;
    }
    qsort((void *) &graph->nodes, graph->numOfNodes, 
        sizeof(Node), nodeComparator);

    actNodeNum = graph->numOfNodes;
    actProc = 0;
    // boolean flag inidicating if we are going forward or backward while
    // choosing the next process
    forward = 1;
    while (actNodeNum > 0) {
        actSize = getNodeSize(graph, actNodeNum);
        if (actProc != ROOT && actSize >= remainingSpace[actProc]) {
            graph->procForNode[actNodeNum] = actProc;
            (*numOfNodesForProc)[actProc]++;
            remainingSpace[actProc] -= actSize;
            actNodeNum--;
        }
        if (actProc == 0 && !forward) {
            actProc = 1;
            forward = 1;
        } else if (actProc == numOfProcs - 1 && forward) {
            actProc = numOfProcs - 2;
            forward = 0;
        } else {
            actProc = forward ? actProc + 1 : actProc - 1;
        }
    }
}

// called only in root
void distributeGraph(FILE *inFile, Graph *graph, int *numOfNodesForProc)
{
    int i;
    /*
    MPI_Isend(
                rows + (rowsPerProc - 1) * numPointsPerDim,
                numPointsPerDim,
                MPI_DOUBLE,
                lower,
                MPI_SEND_UPPER,
                MPI_COMM_WORLD,
                &requests[2]
            );
    */
}
int main(int argc, char **argv)
{
    if (argc != 3) {
        error("Wrong number of arguments.");
    }

    int rank;
    int numOfProcs;
    Graph graph;
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
        prepareForDistribution(&graph, numOfProcs, &numOfNodesForProc);
        rewind(inFile);
    } else {
        MPI_Bcast(&graph.numOfNodes, 1, MPI_INT, ROOT, MPI_COMM_WORLD);
        prepareGraph(&graph);
    }

    //partitionGraph(inFile, &graph, rank);
    if (rank == ROOT) {
        fclose(inFile);
    }




    if (rank == ROOT) {
        fclose(outFile);
    }

    MPI_Finalize();

    return 0;
}