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
// Macro used in graph distribution
// Check distributeGraph function for more information
#define MAX_NODE_ENCODING 2 * MAX_NUM_OF_NODES + 3
#define MAX_PATTERN_NODES 10
#define MAX_PATTERN_SIZE 1 + MAX_PATTERN_NODES * (2 + 2 * MAX_PATTERN_NODES)
// Each process has 128MB for its nodes
#define SIZE_AVAILABLE 128000000
#define ROOT 0


static const int NUM_NODES_TAG = MAX_NUM_OF_NODES + 1;
static const int NODE_EDGES_TAG = MAX_NUM_OF_NODES + 2;


int isLineEmpty(const char *line)
{
    // Check if the string consists only of spaces
    while (*line != '\0') {
        if (isspace(*line) == 0) {
            return 0;
        }
        line++;
    }
    return 1;
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

/*
 * Assigns nodes to processes evenly by (inDegree + outDegree)
 */
void assignNodeToProc(Graph *graph, int numOfProcs, 
    int **numOfNodesForProc)
{
    int i, actNodeNum, actProc, forward, actSize;
    Node *actNode;
    int remainingSpace[numOfProcs];
    *numOfNodesForProc = (int *) malloc(sizeof(int) * numOfProcs);
    memset(*numOfNodesForProc, 0, sizeof(int) * numOfProcs);
    // Copy of array of nodes - will be used for sorting by inDeg + outDeg
    Node *nodesCopy = (Node *) malloc(sizeof(Node) * (graph->numOfNodes + 1));
    memcpy(nodesCopy, graph->nodes, sizeof(Node) * (graph->numOfNodes + 1));

    for (i = 0; i < numOfProcs; ++i) {
        remainingSpace[i] = SIZE_AVAILABLE;
    }

    qsort(nodesCopy, graph->numOfNodes + 1, sizeof(Node), nodeComparator);

    i = graph->numOfNodes;
    actProc = ROOT + 1;
    // boolean flag inidicating if we are going forward or backward while
    // choosing the next process
    forward = 1;
    while (i > 0) {
        actNode = &nodesCopy[i];
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
    free(nodesCopy);
}

/*
 * Informs every process, how many nodes it will receive
 */
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
// FIXME: reversing graph
void distributeGraph(FILE *inFile, Graph *graph, int *numOfNodesForProc,
    int numOfProcs)
{
    prepareForDistribution(numOfNodesForProc, numOfProcs);
    int actProc, actNode, actOutDeg, actNeighbour, i, temp;
    char line[LOCAL_READ_BUF_SIZE];
    // Buffer used to send node's outgoing edges to a process
    // Node will be encoded as follows: number of node, outDegree, inDegree,
    // then for each edge pair (destNode, processForDestNode)
    int *tempBuf = (int *) malloc(sizeof(int) * MAX_NODE_ENCODING);
    memset(tempBuf, 0, sizeof(int) * MAX_NODE_ENCODING);

    fscanf(inFile, "%d\n", &temp);
    assert(temp == graph->numOfNodes);

    // Assumes, that input is encoded correctly
    while ((fgets(line, sizeof(line), inFile) != NULL) && 
        (isLineEmpty(line) == 0)) {
        sscanf(line, "%d %d\n", &actNode, &actOutDeg);
        actProc = graph->procForNode[actNode];
        tempBuf[0] = actNode;
        tempBuf[1] = actOutDeg;
        tempBuf[2] = graph->nodes[actNode].inDegree;
        for (i = 0; i < actOutDeg; ++i) {
            fgets(line, sizeof(line), inFile);
            sscanf(line, "%d", &actNeighbour);
            tempBuf[2 * i + 3] = actNeighbour;
            tempBuf[2 * i + 4] = graph->procForNode[actNeighbour];
        }
        MPI_Send(tempBuf, 2 * actOutDeg + 3, MPI_INT, actProc,
            NODE_EDGES_TAG, MPI_COMM_WORLD);
    }
    free(tempBuf);
    printf("Finish\n");
}

/*
 * Count number of ingoing and outgoing messages while exchanging
 * information about ingoing edges
 */
void countNumberOfMessages(int rank, Graph *graph, int *numOfSent,
    int *numOfReceived, int *lastInIndex)
{
    int actNeighbour, procForNeighbour, actLastInIndex, i, j;
    Node *actNode;

    for (i = 1; i <= graph->numOfNodes; ++i) {
        if (graph->nodesInGraph[i] == 1) {
            actNode = &graph->nodes[i];
            (*numOfReceived) += actNode->inDegree;
            for (j = 0; j < actNode->outDegree; ++j) {
                actNeighbour = actNode->outEdges[j];
                procForNeighbour = graph->procForNode[actNeighbour];
                if (rank == procForNeighbour) {
                    // Decrease number of received messages for every 
                    // edge held internally by process
                    (*numOfReceived)--;
                    actLastInIndex = lastInIndex[actNeighbour];
                    graph->nodes[actNeighbour].inEdges[actLastInIndex] = i;
                    lastInIndex[actNeighbour]++;
                } else {
                    (*numOfSent)++;
                }
            }
        }
    }

}

/*
 * After receiving their nodes, workers exchange information about
 * their ingoing edges.
 * Every process p will send number of messages equal to the number of
 * edges (v1 -> v2), where v1 is in p and v2 is not in p.
 * Every process will receive number of messages equal to the number of
 * edges (v1 -> v2), where v2 is in p and v1 is not in p.
 */
void exchangeIngoingEdges(int rank, Graph *graph)
{
    int numOfReceived, numOfSent, actNeighbour, procForNeighbour;
    int actRequest, actNumOfReceived, i, j;
    Node *actNode;

    // First free index of inEdges array for every node
    int *lastInIndex = (int *) malloc(sizeof(int) * (MAX_NUM_OF_NODES + 1));
    memset(lastInIndex, 0, sizeof(int) * (MAX_NUM_OF_NODES + 1));

    numOfReceived = 0;
    numOfSent = 0;
    countNumberOfMessages(rank, graph, &numOfSent, &numOfReceived,
        lastInIndex);

    MPI_Request requests[numOfReceived + numOfSent];
    MPI_Status statuses[numOfReceived + numOfSent];

    actRequest = 0;
    // Send and receive edges
    for (i = 1; i <= graph->numOfNodes; ++i) {
        if (graph->nodesInGraph[i] == 1) {
            actNode = &graph->nodes[i];
            // Send outgoing edges
            for (j = 0; j < actNode->outDegree; ++j) {
                actNeighbour = actNode->outEdges[j];
                procForNeighbour = graph->procForNode[actNeighbour];
                if (rank != procForNeighbour) {
                    // Use destination node as tag
                    MPI_Isend(&i, 1, MPI_INT, procForNeighbour,
                        actNeighbour, MPI_COMM_WORLD, requests + actRequest);
                    actRequest++;
                }
            }
            // Receive ingoing edges
            actNumOfReceived = actNode->inDegree - lastInIndex[i];
            for (j = 0; j < actNumOfReceived; ++j) {
                // As above, ingoing edges for node i are tagged with i
                MPI_Irecv(actNode->inEdges + lastInIndex[i], 1, MPI_INT,
                    MPI_ANY_SOURCE, i, MPI_COMM_WORLD, requests + actRequest);
                actRequest++;
                lastInIndex[i]++;
            }
        }
    }

    assert(actRequest == numOfReceived + numOfSent);
    free(lastInIndex);
    MPI_Waitall(numOfReceived + numOfSent, requests, statuses);
}

// Called in workers
// TODO: one comment style
void receiveGraph(int rank, Graph *graph)
{
    int numOfNodes, actNodeNum, actOutDeg, actInDeg, actNeighbour, i, j;
    Node *actNode;
    MPI_Status status;

    MPI_Recv(&numOfNodes, 1, MPI_INT, ROOT, NUM_NODES_TAG,
        MPI_COMM_WORLD, &status);
    printf("num of nodes %d\n", numOfNodes);
    fflush(stdout);

    // Buffer to receive node information from root
    // Check distributeGraph for node encoding
    int *tempBuf = (int *) malloc(sizeof(int) * MAX_NODE_ENCODING);
    memset(tempBuf, 0, sizeof(int) * MAX_NODE_ENCODING);

    for (i = 0; i < numOfNodes; ++i) {
        MPI_Recv(tempBuf, MAX_NODE_ENCODING, MPI_INT, ROOT,
            NODE_EDGES_TAG, MPI_COMM_WORLD, &status);
        actNodeNum = tempBuf[0];
        actOutDeg = tempBuf[1];
        actInDeg = tempBuf[2];
        graph->nodesInGraph[actNodeNum] = 1;
        graph->procForNode[actNodeNum] = rank;
        actNode = &graph->nodes[actNodeNum];
        actNode->num = actNodeNum;
        actNode->outDegree = actOutDeg;
        actNode->inDegree = actInDeg;
        actNode->outEdges = (int *) malloc(sizeof(int) * actOutDeg);
        actNode->inEdges = (int *) malloc(sizeof(int) * actInDeg);
        for (j = 0; j < actOutDeg; ++j) {
            actNeighbour = tempBuf[2 * j + 3];
            actNode->outEdges[j] = actNeighbour;
            graph->procForNode[actNeighbour] = tempBuf[2 * j + 4];
        }
    }
    free(tempBuf);
    exchangeIngoingEdges(rank, graph);
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
        receiveGraph(rank, &graph);
        sleep(rank);        
        printf("%d graph received\n", rank);
        printf("\n");
        printGraphDebug(&graph);
        receivePattern(&pattern);
    }

    // patternMatch()

    if (rank == ROOT) {
        fclose(outFile);
        free(numOfNodesForProc);
        printf("end\n");
    }

    freeGraph(&graph);
    freeGraph(&pattern);

    MPI_Finalize();

    return 0;
}