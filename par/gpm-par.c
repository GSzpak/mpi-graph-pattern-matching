#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>
#include <assert.h>
#include <ctype.h>
#include <unistd.h> // TODO: delete

#include "graph.h"
#include "common.h"

#define LOCAL_READ_BUF_SIZE 64
#define MAX_NUM_OF_NODES 10000000
#define MAX_PATTERN_NODES 10
#define MAX_PATTERN_SIZE 1 + MAX_PATTERN_NODES * (2 + 2 * MAX_PATTERN_NODES)
#define SIZE_AVAILABLE 200
#define ROOT 0


static const int NUM_NODES_TAG = 49;
static const int NODE_EDGES_TAG = 50;


int isLineEmpty(const char *line)
{
    /* check if the string consists only of spaces. */
    while (*line != '\0') {
        if (isspace(*line) == 0) {
            return 0;
        }
        line++;
    }
    return 1;
}

// TODO: move procForNode outside
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

// Called only in root
void countDegrees(FILE *inFile, Graph *graph)
{
    char line[LOCAL_READ_BUF_SIZE];
    int actNode, actOutDeg, actNeighbour, i;
    while ((fgets(line, sizeof(line), inFile) != NULL) && 
        (isLineEmpty(line) == 0)) {
        sscanf(line, "%d %d", &actNode, &actOutDeg);
        graph->nodes[actNode].num = actNode;
        for (i = 0; i < actOutDeg; ++i) {
            fgets(line, sizeof(line), inFile);
            sscanf(line, "%d\n", &actNeighbour);
            graph->nodes[actNeighbour].inDegree += 1;
            graph->nodes[actNode].outDegree += 1;
        }
    }
}

void prepareGraphInRoot(FILE *inFile, Graph *graph)
{
    fscanf(inFile, "%d\n", &graph->numOfNodes);
    MPI_Bcast(&graph->numOfNodes, 1, MPI_INT, ROOT, MPI_COMM_WORLD);
    prepareGraph(graph);
    countDegrees(inFile, graph);
}

void preparePatternInRoot(FILE *inFile, Graph *pattern)
{
    int i;
    Node *actNode;

    fscanf(inFile, "%d\n", &pattern->numOfNodes);
    prepareGraph(pattern);
    countDegrees(inFile, pattern);
    for (i = 1; i <= pattern->numOfNodes; ++i) {
        pattern->nodesInGraph[i] = 1;
        actNode = &pattern->nodes[i];
        actNode->num = i;
        actNode->outEdges = (int *) malloc(sizeof(int) * actNode->outDegree);
        memset(actNode->outEdges, 0, sizeof(int) * actNode->outDegree);
        actNode->inEdges = (int *) malloc(sizeof(int) * actNode->inDegree);
        memset(actNode->inEdges, 0, sizeof(int) * actNode->inDegree);
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
    return sizeof(Node) + (node->inDegree + node->outDegree) * sizeof(int);
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

    qsort(graph->nodes, graph->numOfNodes + 1, 
        sizeof(Node), nodeComparator);

    i = graph->numOfNodes;
    actProc = ROOT + 1;
    // boolean flag inidicating if we are going forward or backward while
    // choosing the next process
    forward = 1;
    while (i > 0) {
        actNode = &graph->nodes[i];
        actNodeNum = actNode->num;
        actSize = getNodeSize(actNode);
        if (actSize < remainingSpace[actProc]) {
            graph->procForNode[actNodeNum] = actProc;
            (*numOfNodesForProc)[actProc]++;
            remainingSpace[actProc] -= actSize;
            i--;
        }
        if (actProc == ROOT + 1 && !forward) {
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
        MPI_Request *actRequest = actProc < ROOT ? requests + actProc :
            requests + actProc - 1;
        if (actProc != ROOT) {
            MPI_Isend(numOfNodesForProc + actProc, 1, MPI_INT, actProc,
                NUM_NODES_TAG, MPI_COMM_WORLD, actRequest);
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
    int actProc, actNode, actOutDeg, i, temp;
    // Buffer used to send node's outgoing edges to a process
    // At first index will be sent number of node, 
    // then the outgoing edges
    //TODO: change for malloc
    int tempBuf[graph->numOfNodes + 1];
    char line[LOCAL_READ_BUF_SIZE];

    fscanf(inFile, "%d\n", &temp);
    assert(temp == graph->numOfNodes);

    // Assumes, that input is encoded correctly
    while ((fgets(line, sizeof(line), inFile) != NULL) && 
        (isLineEmpty(line) == 0)) {
        sscanf(line, "%d %d\n", &actNode, &actOutDeg);
        actProc = graph->procForNode[actNode];
        tempBuf[0] = actNode;
        for (i = 0; i < actOutDeg; ++i) {
            fgets(line, sizeof(line), inFile);
            sscanf(line, "%d", tempBuf + i + 1);
        }
        MPI_Send(tempBuf, actOutDeg + 1, MPI_INT, actProc,
            NODE_EDGES_TAG, MPI_COMM_WORLD);
    }
    printf("Finish\n");
}

// Called in workers
void receiveGraph(Graph *graph)
{
    int numOfNodes, receivedCount, actNodeNum, actOutDeg, i, j;
    Node *actNode;
    MPI_Status status;

    MPI_Recv(&numOfNodes, 1, MPI_INT, ROOT, NUM_NODES_TAG,
        MPI_COMM_WORLD, &status);
    printf("num of nodes %d\n", numOfNodes);
    fflush(stdout);

    // Buffer to receive node with outgoing edges
    int *tempBuf = (int *) malloc(sizeof(int) * (MAX_NUM_OF_NODES + 1));
    memset(tempBuf, 0, sizeof(int) * (MAX_NUM_OF_NODES + 1));
    
    for (i = 0; i < numOfNodes; ++i) {
        MPI_Recv(tempBuf, MAX_NUM_OF_NODES + 1, MPI_INT, ROOT,
            NODE_EDGES_TAG, MPI_COMM_WORLD, &status);
        MPI_Get_count(&status, MPI_INT, &receivedCount);
        actOutDeg = receivedCount - 1;
        actNodeNum = tempBuf[0];
        graph->nodesInGraph[actNodeNum] = 1;
        actNode = &graph->nodes[actNodeNum];
        actNode->num = actNodeNum;
        actNode->outDegree = actOutDeg;
        actNode->outEdges = (int *) malloc(sizeof(int) * actOutDeg);
        memset(actNode->outEdges, 0, sizeof(int) * actOutDeg);
        for (j = 1; j <= actOutDeg; ++j) {
            actNode->outEdges[j - 1] = tempBuf[j];
        }
    }
    free(tempBuf);
    
}

void readPattern(FILE *inFile, Graph *pattern)
{
    int i, numOfNodes, actNodeNum, actOutDeg, actNeighbour, inIndex;
    Node *actNode;
    int lastInIndex[pattern->numOfNodes + 1];
    memset(lastInIndex, 0, sizeof(int) * (pattern->numOfNodes + 1));
    char line[LOCAL_READ_BUF_SIZE];

    fscanf(inFile, "%d\n", &numOfNodes);
    assert(numOfNodes == pattern->numOfNodes);
    
    while ((fgets(line, sizeof(line), inFile) != NULL) && 
        (isLineEmpty(line) == 0)) {
        sscanf(line, "%d %d", &actNodeNum, &actOutDeg);
        actNode = &pattern->nodes[actNodeNum];
        for (i = 0; i < actOutDeg; ++i) {
            fgets(line, sizeof(line), inFile);
            sscanf(line, "%d", &actNeighbour);
            actNode->outEdges[i] = actNeighbour;
            inIndex = lastInIndex[actNeighbour];
            pattern->nodes[actNeighbour].inEdges[inIndex] = actNodeNum;
            lastInIndex[actNeighbour]++;
        }
    }
}

/*
 * Pattern will be encoded in a following way:
 * number of nodes, then for every node:
 * outDeg, inDeg, outEdges, inEdges.
 * Nodes are sorted by their number.
 */
void broadcastPattern(Graph *pattern)
{
    int actIndex, node, i;
    Node *actNode;
    
    int buf[MAX_PATTERN_SIZE];
    buf[0] = pattern->numOfNodes;
    actIndex = 1;
    for (node = 1; node <= pattern->numOfNodes; ++node) {
        actNode = &pattern->nodes[node];
        buf[actIndex] = actNode->outDegree;
        actIndex++;
        buf[actIndex] = actNode->inDegree;
        actIndex++;
        for (i = 0; i < actNode->outDegree; ++i) {
            buf[actIndex] = actNode->outEdges[i];
            actIndex++;
        }
        for (i = 0; i < actNode->inDegree; ++i) {
            buf[actIndex] = actNode->inEdges[i];
            actIndex++;
        }
    }
    MPI_Bcast(buf, MAX_PATTERN_SIZE, MPI_INT, ROOT, MPI_COMM_WORLD);
}

void receivePattern(Graph *pattern)
{
    int node, i, actIndex;
    Node *actNode;
    int buf[MAX_PATTERN_SIZE];
    
    MPI_Bcast(buf, MAX_PATTERN_SIZE, MPI_INT, ROOT, MPI_COMM_WORLD);
    pattern->numOfNodes = buf[0];
    prepareGraph(pattern);
    actIndex = 1;
    for (node = 1; node <= pattern->numOfNodes; ++node) {
        pattern->nodesInGraph[node] = 1;
        actNode = &pattern->nodes[node];
        actNode->num = node;
        actNode->outDegree = buf[actIndex];
        actIndex++;
        actNode->inDegree = buf[actIndex];
        actIndex++;
        actNode->outEdges = (int *) malloc(sizeof(int) * actNode->outDegree);
        actNode->inEdges = (int *) malloc(sizeof(int) * actNode->inDegree);
        for (i = 0; i < actNode->outDegree; ++i) {
            actNode->outEdges[i] = buf[actIndex];
            actIndex++;
        }
        for (i = 0; i < actNode->inDegree; ++i) {
            actNode->inEdges[i] = buf[actIndex];
            actIndex++;
        }
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
        prepareGraphInRoot(inFile, &graph);
        preparePatternInRoot(inFile, &pattern);
        assignNodeToProc(&graph, numOfProcs, &numOfNodesForProc);
        rewind(inFile);
        printf("distributing\n");
        distributeGraph(inFile, &graph, numOfNodesForProc, numOfProcs);
        printf("reading pattern\n");
        readPattern(inFile, &pattern);
        printf("broadcasting pattern\n");
        broadcastPattern(&pattern);
        printf("pattern broadcasted\n");
        fclose(inFile);
    } else {
        MPI_Bcast(&graph.numOfNodes, 1, MPI_INT, ROOT, MPI_COMM_WORLD);
        prepareGraph(&graph);
        printf("%d graph prepared\n", rank);
        receiveGraph(&graph);
        printf("%d graph received\n", rank);
        receivePattern(&pattern);
        if (rank == 3) {
            printGraphDebug(&pattern);
        }
    }

    // patternMatch()

    if (rank == ROOT) {
        fclose(outFile);
        free(numOfNodesForProc);
        printf("end\n");
    }

    MPI_Finalize();

    return 0;
}