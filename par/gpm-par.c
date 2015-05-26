#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>
#include <assert.h>

#include "graph.h"
#include "common.h"

#define MAX_NUM_OF_NODES 10000000
#define MAX_NODES_SIZE_PER_PROC 20
#define ROOT 0
#define FIRST_WORKER ROOT + 1
#define META_INF_SIZE 3


static const int SEND_NODE_META_TAG = 50;
static const int SEND_NODE_NEIGHBOURS_TAG = 51;


void prepareGraphInRoot(FILE *inFile, Graph *graph)
{
    graph->outEdges = NULL;
    graph->inEdges = NULL;
    fscanf(inFile, "%d\n", &graph->numOfNodes);
    graph->inDegrees = (int *) malloc(sizeof(int) * (graph->numOfNodes + 1));
    graph->outDegrees = (int *) malloc(sizeof(int) * (graph->numOfNodes + 1));
    graph->procForNode = (int *) malloc(sizeof(int) * (graph->numOfNodes + 1));
    memset(graph->inDegrees, 0, sizeof(int) * (graph->numOfNodes + 1));
    memset(graph->outDegrees, 0, sizeof(int) * (graph->numOfNodes + 1));
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
            graph->inDegrees[actNeighbour] += 1;
            graph->outDegrees[actNode] += 1;
        }
    }
}

int getNodeSize(int node, int *inDegrees, int *outDegrees)
{
    return (inDegrees[node] + outDegrees[node]) * sizeof(int);
}

void prepareGraphInWorker(Graph *graph)
{
    int i;
    graph->outEdges = (int **) malloc(sizeof(int *) * (graph->numOfNodes + 1));
    graph->inEdges = (int **) malloc(sizeof(int *) * (graph->numOfNodes + 1));
    graph->outDegrees = (int *) malloc(sizeof(int) * (graph->numOfNodes + 1));
    graph->inDegrees = (int *) malloc(sizeof(int) * (graph->numOfNodes + 1));
    graph->procForNode = (int *) malloc(sizeof(int) * (graph->numOfNodes + 1));
    for (i = 0; i < graph->numOfNodes + 1; ++i) {
        graph->outEdges[i] = NULL;
        graph->inEdges[i] = NULL;
        graph->outDegrees[i] = -1;
        graph->inDegrees[i] = -1;
        graph->procForNode[i] = -1;
    }
}

void sendNode(FILE *inFile, int destProc, int node, int outDeg,
    int *neighboursBuf)
{
    int i;
    
    for (i = 0; i < outDeg; ++i) {
        fscanf(inFile, "%d\n", neighboursBuf + i);
    }
    // TODO: check send semantics
    MPI_Send(neighboursBuf, outDeg, MPI_INT, destProc,
        SEND_NODE_NEIGHBOURS_TAG, MPI_COMM_WORLD);
}

void receiveNode(Graph *graph, int node, int outDeg)
{
    MPI_Status status;
    graph->outEdges[node] = (int *) malloc(sizeof(int) * outDeg);
    graph->outDegrees[node] = outDeg;
    MPI_Recv(graph->outEdges[node], outDeg, MPI_INT, ROOT, SEND_NODE_META_TAG,
        MPI_COMM_WORLD, &status);
}

void shareMetaData(int rank, int *actDestProc, int *actNode, int *actOutDeg,
    Graph *graph, int *metaBuf)
{
    // Message "process actDestProc is receiving actNode"
    if (rank == ROOT) {
        metaBuf[0] = *actDestProc;
        metaBuf[1] = *actNode;
        metaBuf[2] = *actOutDeg;
    }
    MPI_Bcast(metaBuf, META_INF_SIZE, MPI_INT, ROOT, MPI_COMM_WORLD);
    if (rank != ROOT) {
        *actDestProc = metaBuf[0];
        *actNode = metaBuf[1];
        *actOutDeg = metaBuf[2];    
    }
    graph->procForNode[*actNode] = *actDestProc;
}

void partitionGraph(FILE *inFile, Graph *graph, int rank)
{
    int actDestProc, actNode, actOutDeg, actNodeSize, numOfNodes, i;
    int *metaBuf = NULL;
    int *neighboursBuf = NULL;

    metaBuf = (int *) malloc(sizeof(int) * META_INF_SIZE);
    if (rank == ROOT) {
        fscanf(inFile, "%d\n", &numOfNodes);
        assert(numOfNodes == graph->numOfNodes);
        neighboursBuf = (int *) malloc(sizeof(int) * numOfNodes);
    }

    actDestProc = FIRST_WORKER;
    int spaceInProcRemaining = MAX_NODES_SIZE_PER_PROC;
    for (i = 0; i < graph->numOfNodes; ++i) {
        if (rank == ROOT) {
            fscanf(inFile, "%d %d\n", &actNode, &actOutDeg);
            actNodeSize = getNodeSize(actNode, graph->inDegrees,
                graph->outDegrees);
            if (actNodeSize > spaceInProcRemaining) {
                actDestProc++;
                spaceInProcRemaining = MAX_NODES_SIZE_PER_PROC;
            }
            spaceInProcRemaining -= actNodeSize;
        }

        shareMetaData(rank, &actDestProc, &actNode, &actOutDeg, graph, metaBuf);
        
        if (rank == ROOT) {
            sendNode(inFile, actDestProc, actNode, actOutDeg, neighboursBuf);
        } else if (rank == actDestProc) {
            receiveNode(graph, actNode, actOutDeg);
        }
    }

    free(metaBuf);
    if (rank == ROOT) {
        free(neighboursBuf);
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
    char *inFileName = argv[1];
    char *outFileName = argv[2];
    FILE *inFile = NULL;
    FILE *outFile = NULL;

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
        prepareGraphInRoot(inFile, &graph);
        countDegrees(inFile, &graph);
        MPI_Bcast(&graph.numOfNodes, 1, MPI_INT, ROOT, MPI_COMM_WORLD);
        rewind(inFile);
    } else {
        MPI_Bcast(&graph.numOfNodes, 1, MPI_INT, ROOT, MPI_COMM_WORLD);
        prepareGraphInWorker(&graph);
    }

    partitionGraph(inFile, &graph, rank);
    if (rank == ROOT) {
        fclose(inFile);
    }




    if (rank == ROOT) {
        fclose(outFile);
    }

    MPI_Finalize();

    return 0;
}