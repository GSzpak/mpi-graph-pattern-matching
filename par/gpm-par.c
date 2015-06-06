#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>
#include <assert.h>
#include <unistd.h> // TODO: delete

#include "graph.h"
#include "utils.h"


#define LOCAL_READ_BUF_SIZE 64
#define MAX_NUM_OF_NODES 10000000
// Macro used in graph distribution
// Check distributeGraph function for more information
#define MAX_NODE_ENCODING 2 * MAX_NUM_OF_NODES + 3
#define MAX_PATTERN_NODES 10
#define MAX_PATTERN_SIZE 1 + MAX_PATTERN_NODES * (2 + 2 * MAX_PATTERN_NODES)
// Each process has 200MB for its nodes and edges
#define SIZE_AVAILABLE 200000000
#define ROOT 0


static const int NUM_NODES_TAG = MAX_NUM_OF_NODES + 1;
static const int NODE_OUT_EDGES_TAG = MAX_NUM_OF_NODES + 2;
static const int NODE_IN_EDGES_TAG = MAX_NUM_OF_NODES + 3;


/*
 * Counts in/out degrees for each node. Called only in root.
 */
void countDegrees(FILE *inFile, Graph *graph)
{
    char line[LOCAL_READ_BUF_SIZE];
    int actNodeNum, actOutDeg, actNeighbourNum, i;
    Node *actNode;
    Node *actNeighbour;

    while ((fgets(line, sizeof(line), inFile) != NULL) && 
        (isLineEmpty(line) == 0)) {
        sscanf(line, "%d %d", &actNodeNum, &actOutDeg);
        // In root node `num` is at index `num` in nodes array
        actNode = getNode(graph, actNodeNum);
        for (i = 0; i < actOutDeg; ++i) {
            fgets(line, sizeof(line), inFile);
            sscanf(line, "%d\n", &actNeighbourNum);
            actNeighbour = getNode(graph, actNeighbourNum);
            actNeighbour->inDegree += 1;
            actNode->outDegree += 1;
        }
    }
}

void prepareGraphInRoot(FILE *inFile, Graph *graph)
{
    int numOfNodes, node;
    fscanf(inFile, "%d\n", &numOfNodes);
    MPI_Bcast(&numOfNodes, 1, MPI_INT, ROOT, MPI_COMM_WORLD);
    prepareGraph(graph, numOfNodes, numOfNodes);
    for (node = 1; node <= graph->numOfNodes; ++node) {
        graph->nodeIndex[node] = node - 1;
        graph->nodes[node - 1].num = node;
    }
    countDegrees(inFile, graph);
}

void preparePatternInRoot(FILE *inFile, Graph *pattern)
{
    int i, numOfNodes;
    Node *actNode;

    fscanf(inFile, "%d\n", &numOfNodes);
    prepareGraph(pattern, numOfNodes, numOfNodes);
    for (i = 1; i <= pattern->numOfNodes; ++i) {
        pattern->nodeIndex[i] = i - 1;
        pattern->nodes[i - 1].num = i;
    }
    countDegrees(inFile, pattern);
    for (i = 1; i <= pattern->numOfNodes; ++i) {
        pattern->nodeIndex[i] = i - 1;
        actNode = getNode(pattern, i);
        actNode->num = i;
        actNode->outEdges = (int *) malloc(sizeof(int) * actNode->outDegree);
        memset(actNode->outEdges, 0, sizeof(int) * actNode->outDegree);
        actNode->inEdges = (int *) malloc(sizeof(int) * actNode->inDegree);
        memset(actNode->inEdges, 0, sizeof(int) * actNode->inDegree);
    }
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
    // Copy of array of nodes - will be used for sorting by inDeg + outDeg
    Node *nodesCopy = (Node *) malloc(sizeof(Node) * graph->numOfNodes);
    
    memset(*numOfNodesForProc, 0, sizeof(int) * numOfProcs);
    memcpy(nodesCopy, graph->nodes, sizeof(Node) * graph->numOfNodes);
    for (i = 0; i < numOfProcs; ++i) {
        remainingSpace[i] = SIZE_AVAILABLE;
    }
    qsort(nodesCopy, graph->numOfNodes, sizeof(Node), nodeComparator);

    i = graph->numOfNodes - 1;
    actProc = ROOT + 1;
    // Boolean flag inidicating if we are going forward or backward when
    // choosing the next process
    forward = 1;
    
    while (i > -1) {
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

/*
 * Sending out edges to workers. Called in root
 */
void distributeGraph(FILE *inFile, Graph *graph, int *numOfNodesForProc,
    int numOfProcs)
{
    prepareForDistribution(numOfNodesForProc, numOfProcs);

    int actProc, actNodeNum, actOutDeg, actNeighbour, i, temp;
    Node *actNode;
    char line[LOCAL_READ_BUF_SIZE];
    // Buffer used to send node's outgoing edges to a process
    // Node will be encoded as follows: number of node, outDegree, inDegree,
    // then for each edge pair (destNode, processForDestNode)
    int *tempBuf = (int *) malloc(sizeof(int) * MAX_NODE_ENCODING);
    memset(tempBuf, 0, sizeof(int) * MAX_NODE_ENCODING);

    fscanf(inFile, "%d\n", &temp);
    assert(temp == graph->numOfNodes);

    // Assumes, that input is encoded correctly
    // Distributes nodes with outDeg > 0
    while ((fgets(line, sizeof(line), inFile) != NULL) && 
        (isLineEmpty(line) == 0)) {
        sscanf(line, "%d %d", &actNodeNum, &actOutDeg);
        actProc = graph->procForNode[actNodeNum];
        actNode = getNode(graph, actNodeNum);
        tempBuf[0] = actNodeNum;
        tempBuf[1] = actOutDeg;
        tempBuf[2] = actNode->inDegree;
        for (i = 0; i < actOutDeg; ++i) {
            fgets(line, sizeof(line), inFile);
            sscanf(line, "%d", &actNeighbour);
            tempBuf[2 * i + 3] = actNeighbour;
            tempBuf[2 * i + 4] = graph->procForNode[actNeighbour];
        }
        MPI_Send(tempBuf, 2 * actOutDeg + 3, MPI_INT, actProc,
            NODE_OUT_EDGES_TAG, MPI_COMM_WORLD);
    }

    // Distributes nodes with outDeg == 0
    for (i = 0; i < graph->numOfNodes; ++i) {
        if (graph->nodes[i].outDegree == 0) {
            actNode = &graph->nodes[i];
            tempBuf[0] = actNode->num;
            tempBuf[1] = actNode->outDegree;
            tempBuf[2] = actNode->inDegree;
            actProc = graph->procForNode[actNode->num];
            MPI_Send(tempBuf, 3, MPI_INT, actProc,
                NODE_OUT_EDGES_TAG, MPI_COMM_WORLD);
        }
    }

    free(tempBuf);
    printf("Finish\n");
}

void prepareGraphInWorker(Graph *graph)
{
    int numOfNodes, myPartNumOfNodes;
    MPI_Status status;

    MPI_Bcast(&numOfNodes, 1, MPI_INT, ROOT, MPI_COMM_WORLD);
    MPI_Recv(&myPartNumOfNodes, 1, MPI_INT, ROOT, NUM_NODES_TAG,
        MPI_COMM_WORLD, &status);
    prepareGraph(graph, numOfNodes, myPartNumOfNodes);
    
    printf("num of nodes %d\n", numOfNodes);
    fflush(stdout);
}

/*
 * Called in workers. Every worker receives: node in/out degree and its
 * outgoing edges
 */
void receiveOutEdges(int rank, int numOfProcs, Graph *graph)
{
    int actNodeNum, actOutDeg, actInDeg, actNeighbour, i, j;
    Node *actNode;
    MPI_Status status;

    // Buffer to receive node information from root
    // Check distributeGraph for node encoding
    int *tempBuf = (int *) malloc(sizeof(int) * MAX_NODE_ENCODING);
    memset(tempBuf, 0, sizeof(int) * MAX_NODE_ENCODING);

    for (i = 0; i < graph->myPartNumOfNodes; ++i) {
        MPI_Recv(tempBuf, MAX_NODE_ENCODING, MPI_INT, ROOT,
            NODE_OUT_EDGES_TAG, MPI_COMM_WORLD, &status);
        actNodeNum = tempBuf[0];
        actOutDeg = tempBuf[1];
        actInDeg = tempBuf[2];
        graph->nodeIndex[actNodeNum] = i;
        graph->procForNode[actNodeNum] = rank;
        actNode = &graph->nodes[i];
        actNode->num = actNodeNum;
        actNode->outDegree = actOutDeg;
        actNode->inDegree = actInDeg;
        if (actOutDeg > 0) {
            actNode->outEdges = (int *) malloc(sizeof(int) * actOutDeg);
        }
        if (actInDeg > 0) {
            actNode->inEdges = (int *) malloc(sizeof(int) * actInDeg);
        }
        for (j = 0; j < actOutDeg; ++j) {
            actNeighbour = tempBuf[2 * j + 3];
            actNode->outEdges[j] = actNeighbour;
            graph->procForNode[actNeighbour] = tempBuf[2 * j + 4];
        }
    }
    free(tempBuf);
}

/*
 * Count number of ingoing and outgoing messages while exchanging
 * information about ingoing edges
 */
void countNumberOfInOutSent(int rank, Graph *graph, int *sentToOtherProcs,
    int numOfProcs, int *numOfSent, int *numOfReceived, int *lastInIndex)
{
    int neighbourNum, procForNeighbour, actLastInIndex, i, j;
    Node *actNode;
    Node *neighbour;

    for (i = 0; i < graph->myPartNumOfNodes; ++i) {
        actNode = &graph->nodes[i];
        (*numOfReceived) += actNode->inDegree;
        for (j = 0; j < actNode->outDegree; ++j) {
            neighbourNum = actNode->outEdges[j];
            procForNeighbour = graph->procForNode[neighbourNum];
            if (rank == procForNeighbour) {
                // Decrease number of received messages for every 
                // edge held internally by process
                (*numOfReceived)--;
                neighbour = getNode(graph, neighbourNum);
                actLastInIndex = lastInIndex[neighbourNum];
                neighbour->inEdges[actLastInIndex] = actNode->num;
                lastInIndex[neighbourNum]++;
            } else {
                sentToOtherProcs[procForNeighbour]++;
            }
        }
    }
    // Count number of messages sent to other processes
    for (i = 0; i < numOfProcs; ++i) {
        if (sentToOtherProcs[i] > 0) {
            (*numOfSent)++;
        }
    }
}

/*
 * Send edges, which end in other process, to this process
 * Used in exchangeIngoingEdges function
 */
void sendEdges(Graph * graph, int rank, int numOfProcs, int numOfSent,
    int *sentToOtherProcs, MPI_Request *requests)
{
    int i, j, actRequest, actNeighbour, procForNeighbour, actLastBufIndex;
    int *actBuffer;
    Node *actNode;
    // Buffers used to send edges to other processes
    // Every edge will be sent as a pair (from, to)
    int *otherProcsBufs[numOfProcs];
    // Last indices in process buffers
    int lastIndex[numOfProcs];

    memset(lastIndex, 0, sizeof(lastIndex));
    for (i = 0; i < numOfProcs; ++i) {
        if (sentToOtherProcs[i] > 0) {
            otherProcsBufs[i] = (int *) malloc(sizeof(int) * 2 * 
                sentToOtherProcs[i]);
            memset(otherProcsBufs[i], 0, sizeof(int) * 2 * sentToOtherProcs[i]);
        } else {
            otherProcsBufs[i] = NULL;
        }
    }

    for (i = 0; i < graph->myPartNumOfNodes; ++i) {
        actNode = &graph->nodes[i];
        for (j = 0; j < actNode->outDegree; ++j) {
            actNeighbour = actNode->outEdges[j];
            procForNeighbour = graph->procForNode[actNeighbour];
            if (rank != procForNeighbour) {
                actLastBufIndex = lastIndex[procForNeighbour];
                actBuffer = otherProcsBufs[procForNeighbour];
                actBuffer[actLastBufIndex] = actNode->num;
                actBuffer[actLastBufIndex + 1] = actNeighbour;
                lastIndex[procForNeighbour] += 2;
            }
        }
    }

    actRequest = 0;
    for (i = 0; i < numOfProcs; ++i) {
        if (sentToOtherProcs[i] > 0) {
            MPI_Isend(otherProcsBufs[i], 2 * sentToOtherProcs[i], MPI_INT, i,
                NODE_IN_EDGES_TAG, MPI_COMM_WORLD, requests + actRequest);
            actRequest++;
        } 
    }
        
    assert(actRequest == numOfSent);

    for (i = 0; i < numOfProcs; ++i) {
        if (otherProcsBufs[i] != NULL) {
            free(otherProcsBufs[i]);
        }
    }
}

/*
 * Receive ingoing edges from other processes
 * Used in exchangeIngoingEdges function
 */
void receiveEdges(Graph *graph, int numOfReceived, int *lastInIndex)
{
    int i, remaining, actCount, actReceived, actSource, actDest, actSourceProc;
    MPI_Status status;
    Node *actNode;
    // FIXME: assumes, that ingoing edges will fit into 128mb
    int *edgesBuf = (int *) malloc(sizeof(int) * SIZE_AVAILABLE);
    memset(edgesBuf, 0, sizeof(int) * SIZE_AVAILABLE);
    
    remaining = numOfReceived;
    while (remaining > 0) {
        // FIXME: SIZE_AVAILABLE
        MPI_Recv(edgesBuf, SIZE_AVAILABLE, MPI_INT, MPI_ANY_SOURCE,
            NODE_IN_EDGES_TAG, MPI_COMM_WORLD, &status);
        MPI_Get_count(&status, MPI_INT, &actCount);
        actSourceProc = status.MPI_SOURCE;
        // Every edge is sent as pair (from, to)
        assert(actCount % 2 == 0);
        actReceived = actCount / 2;
        for (i = 0; i < actReceived; ++i) {
            actSource = edgesBuf[2 * i];
            actDest = edgesBuf[2 * i + 1];
            actNode = getNode(graph, actDest);
            actNode->inEdges[lastInIndex[actDest]] = actSource;
            lastInIndex[actDest]++;
            graph->procForNode[actSource] = actSourceProc;
        }
        remaining -= actReceived;
    }
    assert (remaining == 0);
    free(edgesBuf);
}

/*
 * After receiving their nodes, workers exchange information about
 * their ingoing edges.
 * Every process p will send number of messages equal to the number of
 * edges (v1 -> v2), where v1 is in p and v2 is not in p.
 * Every process will receive number of messages equal to the number of
 * edges (v1 -> v2), where v2 is in p and v1 is not in p.
 */
void exchangeInEdges(int rank, int numOfProcs, Graph *graph)
{
    int numOfReceived, numOfSent;
    // Counts number of edges sent to each other process
    int sentToOtherProcs[numOfProcs];
    // First free index of inEdges array for every node
    int *lastInIndex = (int *) malloc(sizeof(int) * (graph->numOfNodes + 1));
    
    numOfSent = 0;    
    numOfReceived = 0;
    memset(sentToOtherProcs, 0, sizeof(sentToOtherProcs));
    memset(lastInIndex, 0, sizeof(int) * (graph->numOfNodes + 1));

    countNumberOfInOutSent(rank, graph, sentToOtherProcs, numOfProcs,
        &numOfSent, &numOfReceived, lastInIndex);


    MPI_Request requests[numOfSent];
    MPI_Status statuses[numOfSent];

    sendEdges(graph, rank, numOfProcs, numOfSent, sentToOtherProcs, 
        requests);
    receiveEdges(graph, numOfReceived, lastInIndex);
    
    free(lastInIndex);
    
    MPI_Waitall(numOfSent, requests, statuses);
}

void readPattern(FILE *inFile, Graph *pattern)
{
    int i, numOfNodes, actNodeNum, actOutDeg, actNeighbourNum, inIndex;
    Node *actNode;
    Node *actNeighbour;
    char line[LOCAL_READ_BUF_SIZE];
    int lastInIndex[pattern->numOfNodes + 1];
    memset(lastInIndex, 0, sizeof(int) * (pattern->numOfNodes + 1));

    fscanf(inFile, "%d\n", &numOfNodes);
    assert(numOfNodes == pattern->numOfNodes);

    while ((fgets(line, sizeof(line), inFile) != NULL) && 
        (isLineEmpty(line) == 0)) {
        sscanf(line, "%d %d", &actNodeNum, &actOutDeg);
        actNode = getNode(pattern, actNodeNum);
        for (i = 0; i < actOutDeg; ++i) {
            fgets(line, sizeof(line), inFile);
            sscanf(line, "%d", &actNeighbourNum);
            actNeighbour = getNode(pattern, actNeighbourNum);
            actNode->outEdges[i] = actNeighbourNum;
            inIndex = lastInIndex[actNeighbourNum];
            actNeighbour->inEdges[inIndex] = actNodeNum;
            lastInIndex[actNeighbourNum]++;
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
    
    memset(buf, 0, sizeof(buf));
    buf[0] = pattern->numOfNodes;
    
    actIndex = 1;
    for (node = 1; node <= pattern->numOfNodes; ++node) {
        actNode = getNode(pattern, node);
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

/*
 * Called in workers. For pattern encoding check broadcastPattern function.
 */
void receivePattern(Graph *pattern)
{
    int numOfNodes, node, i, actIndex;
    Node *actNode;
    int buf[MAX_PATTERN_SIZE];
    
    MPI_Bcast(buf, MAX_PATTERN_SIZE, MPI_INT, ROOT, MPI_COMM_WORLD);
    numOfNodes = buf[0];
    prepareGraph(pattern, numOfNodes, numOfNodes);
    actIndex = 1;
    for (node = 1; node <= pattern->numOfNodes; ++node) {
        pattern->nodeIndex[node] = node - 1;
        actNode = getNode(pattern, node);
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





void findPatternDfsOrdering(Graph *pattern, int **patternDfsOrder,
    int **patternDfsParents)
{
    *patternDfsOrder = (int *) malloc(sizeof(int) * pattern->numOfNodes);
    *patternDfsParents = (int *) malloc(sizeof(int) * (pattern->numOfNodes + 1));
    memset(*patternDfsOrder, 0, sizeof(int) * pattern->numOfNodes);
    memset(*patternDfsParents, 0, sizeof(int) * (pattern->numOfNodes + 1));
    undirectedDfs(1, pattern, *patternDfsOrder, *patternDfsParents);
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
    // Array informing, how many nodes will be sent to each process
    // Used only in root
    int *numOfNodesForProc = NULL;
    int *patternDfsOrder = NULL;
    int *patternDfsParents = NULL;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &numOfProcs);

    if (rank == ROOT) {
        inFile = fopen(inFileName, "r");
        if (inFile == NULL) {
            //MPI_Finalize(); TODO: is necessary?
            error("Can't open input file %s\n", inFileName);
        }
        outFile = fopen(outFileName, "w");
        if (outFile == NULL) {
            //MPI_Finalize(); TODO: is necessary?
            error("Can't open output file.\n", outFileName);
        }
    }

    if (rank == ROOT) {
        printf("preparing\n");
        prepareGraphInRoot(inFile, &graph);
        preparePatternInRoot(inFile, &pattern);
        printf("assigning\n");
        assignNodeToProc(&graph, numOfProcs, &numOfNodesForProc);
        rewind(inFile);
        printf("distributing\n");
        distributeGraph(inFile, &graph, numOfNodesForProc, numOfProcs);
        printf("reading pattern\n");
        readPattern(inFile, &pattern);
        printf("broadcasting pattern\n");
        broadcastPattern(&pattern);
        printf("pattern broadcasted\n");
        printGraphDebug(&graph);
        fclose(inFile);
    } else {
        prepareGraphInWorker(&graph);
        printf("%d graph prepared\n", rank);
        receiveOutEdges(rank, numOfProcs, &graph);
        printf("%d out edges received\n", rank);
        exchangeInEdges(rank, numOfProcs, &graph);
        printf("%d graph received\n", rank);
        printf("\n");
        printGraphDebug(&graph);
        receivePattern(&pattern);
        printf("%d pattern received\n", rank);
        printf("\n");
        findPatternDfsOrdering(&pattern, &patternDfsOrder, &patternDfsParents);
        printArray(patternDfsOrder, pattern.numOfNodes);
        printArray(patternDfsParents, pattern.numOfNodes + 1);
    }

    // patternMatch()

    if (rank == ROOT) {
        fclose(outFile);
        free(numOfNodesForProc);
    } else {
        free(patternDfsOrder);
        free(patternDfsParents);
    }

    printf("end\n");
    freeGraph(&graph);
    freeGraph(&pattern);

    MPI_Finalize();

    return 0;
}