#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>
#include <assert.h>
#include <unistd.h> // TODO: delete

#include "graph.h"
#include "utils.h"
#include "match.h"


#define LOCAL_READ_BUF_SIZE 64
#define MAX_NUM_OF_NODES 10000000
// Maximum node encoding size
// Node is encoded as follows: num, outDegree, inDegree, outEdges, inEdges
#define MAX_NODE_ENCODING 2 * MAX_NUM_OF_NODES + 3
#define MAX_PATTERN_NODES 10
// Maximum pattern encoding size
// Check distributePattern function for more information
#define MAX_PATTERN_SIZE 1 + MAX_PATTERN_NODES * (2 + 2 * MAX_PATTERN_NODES)
// Each process has 200MB for its nodes and edges
#define SIZE_AVAILABLE 209715200
#define MATCH_BUFFER_SIZE 10000
#define ROOT 0
// MPI tags
#define NUM_NODES_TAG 51
#define NODE_OUT_EDGES_TAG 52
#define NODE_IN_EDGES_TAG 53
#define MATCHES_TAG 54
#define FINISHED_TAG 55
#define NODE_REQ_TAG 56
#define NODE_RESP_TAG 57
#define TERMINATE_TAG 58


MPI_Datatype mpiMatchType;
int rankG; /////////////////////////// FIXME:

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
    // then outgoing edges
    int *tempBuf;

    tempBuf = (int *) malloc(sizeof(int) * (MAX_NUM_OF_NODES + 3));
    memset(tempBuf, 0, sizeof(int) * (MAX_NUM_OF_NODES + 3));

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
            tempBuf[i + 3] = actNeighbour;
        }
        MPI_Send(tempBuf, actOutDeg + 3, MPI_INT, actProc,
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

    // Broadcast procForNode array
    MPI_Bcast(graph->procForNode, graph->numOfNodes + 1, MPI_INT, ROOT, 
        MPI_COMM_WORLD);
}

void prepareGraphInWorker(Graph *graph)
{
    int numOfNodes, myPartNumOfNodes;
    MPI_Status status;

    MPI_Bcast(&numOfNodes, 1, MPI_INT, ROOT, MPI_COMM_WORLD);
    MPI_Recv(&myPartNumOfNodes, 1, MPI_INT, ROOT, NUM_NODES_TAG,
        MPI_COMM_WORLD, &status);
    prepareGraph(graph, numOfNodes, myPartNumOfNodes);

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
            actNeighbour = tempBuf[j + 3];
            actNode->outEdges[j] = actNeighbour;
        }
        qsort(actNode->outEdges, actNode->outDegree, sizeof(int), intComparator);
    }
    
    free(tempBuf);

    // Broadcast procForNode array
    MPI_Bcast(graph->procForNode, graph->numOfNodes + 1, MPI_INT, ROOT,
        MPI_COMM_WORLD);
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
 * Receive ingoing edges from other processes
 * Used in exchangeIngoingEdges function
 */
void receiveInEdges(int rank, Graph *graph, int numOfReceived, int *lastInIndex)
{
    int i, remaining, actCount, actReceived, actSource, actDest, actSourceProc;
    int maxInEdgesSize;
    MPI_Status status;
    Node *actNode;
    int *edgesBuf;

    maxInEdgesSize = sizeof(int) * 2 * MAX_NUM_OF_NODES;
    edgesBuf = (int *) malloc(maxInEdgesSize);
    memset(edgesBuf, 0, maxInEdgesSize);

    remaining = numOfReceived;
    while (remaining > 0) {
        MPI_Recv(edgesBuf, maxInEdgesSize, MPI_INT, MPI_ANY_SOURCE,
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
    // Process sorts in edges in every node
    // Out edges are sorted by default
    for (i = 0; i < graph->myPartNumOfNodes; ++i) {
        actNode = &graph->nodes[i];
        qsort(actNode->inEdges, actNode->inDegree, sizeof(int), intComparator);
    }
}

/*
 * Send edges, which end in other process, to this process
 * Used in exchangeIngoingEdges function
 */
void sendAndReceiveEdges(Graph * graph, int rank, int numOfProcs, int numOfSent,
    int numOfReceived, int *sentToOtherProcs, MPI_Request *requests,
    int *lastInIndex)
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
    receiveInEdges(rank, graph, numOfReceived, lastInIndex);
    MPI_Waitall(numOfSent, requests, MPI_STATUSES_IGNORE);

    for (i = 0; i < numOfProcs; ++i) {
        if (otherProcsBufs[i] != NULL) {
            free(otherProcsBufs[i]);
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

    sendAndReceiveEdges(graph, rank, numOfProcs, numOfSent, numOfReceived,
        sentToOtherProcs, requests, lastInIndex);

    free(lastInIndex);
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
    int actIndex, node;
    int buf[MAX_PATTERN_SIZE];

    memset(buf, 0, sizeof(buf));
    buf[0] = pattern->numOfNodes;

    actIndex = 1;
    for (node = 1; node <= pattern->numOfNodes; ++node) {
        copyNodeToBuffer(pattern, node, buf, &actIndex);
    }
    MPI_Bcast(buf, MAX_PATTERN_SIZE, MPI_INT, ROOT, MPI_COMM_WORLD);
}

/*
 * Called in workers. For pattern encoding check broadcastPattern function.
 */
void receivePattern(Graph *pattern)
{
    int numOfNodes, node, actIndex;
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
        readReceivedNode(actNode, node, buf, &actIndex);
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

int nodeMatches(Node *graphNode, Node *patternNode, Match* match)
{
    // Return false, if current graph node is already matched
    if (matchContains(match, graphNode->num)) {
        return 0;
    }

    int i, actPatternDestNum, actPatternSourceNum;
    int actGraphDestNum, actGraphSourceNum;

    // Check out edges - every edge from the pattern must be in the graph
    for (i = 0; i < patternNode->outDegree; ++i) {
        actPatternDestNum = patternNode->outEdges[i];
        actGraphDestNum = patternNumToGraphNum(match, actPatternDestNum);
        if (actGraphDestNum > -1 &&
            !containsOutEdge(graphNode, actGraphDestNum)) {
            return 0;
        }
    }

    // Check in edges
    for (i = 0; i < patternNode->inDegree; ++i) {
        actPatternSourceNum = patternNode->inEdges[i];
        actGraphSourceNum = patternNumToGraphNum(match, actPatternSourceNum);
        if (actGraphSourceNum > -1 &&
            !containsInEdge(graphNode, actGraphSourceNum)) {
            return 0;
        }
    }

    return 1;
}

void handleNodeRequest(MPI_Status *status, Graph *graph, int *buffer)
{
    int requestedNode, bufIndex;

    assert(status->MPI_TAG == NODE_REQ_TAG);
    MPI_Recv(&requestedNode, 1, MPI_INT, status->MPI_SOURCE,
        NODE_REQ_TAG, MPI_COMM_WORLD, status);
    bufIndex = 0;
   //printf("request in %d from %d for %d\n", rankG, status->MPI_SOURCE, requestedNode);
    copyNodeToBuffer(graph, requestedNode, buffer, &bufIndex);
    MPI_Send(buffer, bufIndex, MPI_INT, status->MPI_SOURCE,
        NODE_RESP_TAG, MPI_COMM_WORLD);
}

void askForNode(Graph *graph, int nodeNum, Node *nextNode)
{
    int procForNode, responseReceived, bufIndex;
    int *tempBuf;
    MPI_Request request;
    MPI_Status status;

    procForNode = graph->procForNode[nodeNum];
    tempBuf = (int *) malloc(sizeof(int) * MAX_NODE_ENCODING);

    MPI_Isend(&nodeNum, 1, MPI_INT, procForNode, NODE_REQ_TAG,
        MPI_COMM_WORLD, &request);
   //printf("%d asked for %d\n", rankG, nodeNum);    
    bufIndex = 0;    
    responseReceived = 0;
    while (!responseReceived) {
        MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
        if (status.MPI_TAG == NODE_RESP_TAG) {
            MPI_Recv(tempBuf, MAX_NODE_ENCODING, MPI_INT,
                status.MPI_SOURCE, NODE_RESP_TAG, MPI_COMM_WORLD, &status);
            readReceivedNode(nextNode, nodeNum, tempBuf, &bufIndex);
            responseReceived = 1;
            bufIndex = 0;
        } else {
            handleNodeRequest(&status, graph, tempBuf);            
        }
    }

    free(tempBuf);
}

void handlePendingRequests(Graph *graph)
{
    int isAvailable;
    int *tempBuf;
    MPI_Status status;

    tempBuf = (int *) malloc(sizeof(int) * MAX_NODE_ENCODING);
   //printf("%d handling\n", rankG);
    MPI_Iprobe(MPI_ANY_SOURCE, NODE_REQ_TAG, MPI_COMM_WORLD,
        &isAvailable, &status);
    while (isAvailable) {
        handleNodeRequest(&status, graph, tempBuf);
        MPI_Iprobe(MPI_ANY_SOURCE, NODE_REQ_TAG, MPI_COMM_WORLD,
            &isAvailable, &status);
    }
    free(tempBuf);
   //printf("%d handling finished\n", rankG);
}

Node *getNextParent(Graph *graph, Match *match, int *patternParents,
    int nextPatternNodeNum, Node receivedMatchedNodes[MAX_MATCH_SIZE])
{
    //printf("parent of %d \n", nextPatternNodeNum);
    int patternParentNum, parentInGraphNum, i;
    Node *result;

    result = NULL;
    patternParentNum = patternParents[nextPatternNodeNum];
    parentInGraphNum = patternNumToGraphNum(match, patternParentNum);
    
    if (isInGraph(graph, parentInGraphNum)) {
        return getNode(graph, parentInGraphNum);    
    } else {
        for (i = 0; i < MAX_MATCH_SIZE; ++i) {
            if (receivedMatchedNodes[i].num == parentInGraphNum) {
                result = &receivedMatchedNodes[i];
            }
        }
    }
    return result;
}

void addFinishedMatch(Match *match, Match *finishedMatches, int *index)
{
    memcpy(finishedMatches + (*index)++, match, sizeof(Match));
    if (*index == MATCH_BUFFER_SIZE) {
        MPI_Send(finishedMatches, *index, mpiMatchType, ROOT,
            MATCHES_TAG, MPI_COMM_WORLD);
        *index = 0;
        memset(finishedMatches, 0, sizeof(Match) * MATCH_BUFFER_SIZE);
    }
}

void exploreMatch(Graph* graph, Graph* pattern, Match *match,
    int *nodesMatchingOrder, int *patternParents,
    Node receivedMatchedNodes[MAX_MATCH_SIZE], int *receivedMatchedNodesInd,
    Match *finishedMatches, int *finishedMatchesInd)
{
    Node *parentInGraph;
    Node *nextPatternNode;
    Node *nextGraphNode;
    int *edgesToCheck;
    int nextPatternNodeNum, nextGraphNodeNum, numOfEdges, visitedViaInEdge, i;
    int isNextInGraph;

   //printf ("rank %d match ", rankG);
    //printMatch(match, stdout);
    
    handlePendingRequests(graph);

    if (match->matchedNodes == pattern->numOfNodes) {
        //printf ("%d match found ", rank);
        //printMatch(match, stdout);
        addFinishedMatch(match, finishedMatches, finishedMatchesInd);
        return;
    }

    nextPatternNodeNum = nodesMatchingOrder[match->matchedNodes];
    if (nextPatternNodeNum < 0) {
        nextPatternNodeNum = -nextPatternNodeNum;
        visitedViaInEdge = 1;
    } else {
        visitedViaInEdge = 0;
    }
    nextPatternNode = getNode(pattern, nextPatternNodeNum);
    parentInGraph = getNextParent(graph, match, patternParents,
        nextPatternNodeNum, receivedMatchedNodes);

   //printf ("rank %d next pnode %d parent %d\n", rankG, nextPatternNodeNum, parentInGraph->num);
    // For neighbors of parent we try to match the new node depending whether
    // current node was visited by ingoing or outgoing edge in undirected dfs
    if (visitedViaInEdge) {
        edgesToCheck = parentInGraph->inEdges;
        numOfEdges = parentInGraph->inDegree;
    } else {
        edgesToCheck = parentInGraph->outEdges;
        numOfEdges = parentInGraph->outDegree;
    }

    for (i = 0; i < numOfEdges; ++i) {
        nextGraphNodeNum = edgesToCheck[i];
       //printf("rank %d parent %d nextGraphNodeNum %d\n", rankG, parentInGraph->num, nextGraphNodeNum);
        if (isInGraph(graph, nextGraphNodeNum)) {
            // Process continues matching, if next node is in its part of graph
            nextGraphNode = getNode(graph, nextGraphNodeNum);
            isNextInGraph = 1;
        } else {
            // Otherwise, asks for next node
            askForNode(graph, nextGraphNodeNum,
                receivedMatchedNodes + *receivedMatchedNodesInd);
            //printf("rank %d received %d\n", rankG, nextGraphNodeNum);
            nextGraphNode = &receivedMatchedNodes[(*receivedMatchedNodesInd)++];
            isNextInGraph = 0;
        }
        if (nodeMatches(nextGraphNode, nextPatternNode, match)) {
            addNode(match, nextPatternNodeNum, nextGraphNodeNum);
            exploreMatch(graph, pattern, match, nodesMatchingOrder,
                patternParents, receivedMatchedNodes, receivedMatchedNodesInd,
                finishedMatches, finishedMatchesInd);
            removeNode(match, nextPatternNodeNum);
        }
        if (!isNextInGraph) {
            freeNode(&receivedMatchedNodes[(*receivedMatchedNodesInd)--]);
        }
    }
}

void findMatches(int rank, Graph *graph, Graph *pattern,
    int *nodesMatchingOrder, int *patternParents, MPI_Datatype mpiMatchType)
{
    int *tempBuf;
    Match m;
    MPI_Status status;
    Node receivedMatchedNodes[MAX_MATCH_SIZE];
    Match *finishedMatches;
    int i, terminate, receivedMatchedNodesInd, finishedMatchesInd;

    receivedMatchedNodesInd = 0;
    finishedMatchesInd = 0;
    memset(receivedMatchedNodes, 0, sizeof(receivedMatchedNodes));
    finishedMatches = malloc(sizeof(Match) * MATCH_BUFFER_SIZE);
    memset(finishedMatches, 0, sizeof(Match) * MATCH_BUFFER_SIZE);

    for (i = 0; i < graph->myPartNumOfNodes; ++i) {
        prepareMatch(&m);
        m.matchedNodes = 1;
        m.matches[1] = graph->nodes[i].num;
        exploreMatch(graph, pattern, &m, nodesMatchingOrder,
            patternParents, receivedMatchedNodes, &receivedMatchedNodesInd,
            finishedMatches, &finishedMatchesInd);
        receivedMatchedNodesInd = 0;
        memset(receivedMatchedNodes, 0, sizeof(receivedMatchedNodes));
    }

    if (finishedMatchesInd != 0) {
        // flush remaining finished matches
        MPI_Send(finishedMatches, finishedMatchesInd, mpiMatchType, ROOT,
            MATCHES_TAG, MPI_COMM_WORLD);
    }
//    free(finishedMatches);

    //printf("rank %d all matches produced\n", rank);
    MPI_Ssend(NULL, 0, MPI_BYTE, ROOT, FINISHED_TAG, MPI_COMM_WORLD);

    tempBuf = (int *) malloc(sizeof(int) * MAX_NODE_ENCODING);
    terminate = 0;
    while (!terminate) {
        MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
        if (status.MPI_TAG == TERMINATE_TAG) {
            terminate = 1;
        } else {
            handleNodeRequest(&status, graph, tempBuf);
        }
    }
    free(tempBuf);
}

void receiveMatches(FILE *outFile, int numOfProcs, MPI_Datatype mpiMatchType,
    Graph *pattern)
{
    Match *receivedMatches;
    MPI_Status status;
    MPI_Request request;
    int producingProcs, matchingFinished, actProc, numOfReceived, i;

    receivedMatches = malloc(sizeof(Match) * MATCH_BUFFER_SIZE);
    producingProcs = numOfProcs - 1;
    matchingFinished = 0;
    
    while(!matchingFinished) {
        MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
        switch(status.MPI_TAG) {
            case MATCHES_TAG:
                MPI_Get_count(&status, mpiMatchType, &numOfReceived);
                MPI_Recv(receivedMatches, numOfReceived, mpiMatchType,
                    status.MPI_SOURCE, MATCHES_TAG, MPI_COMM_WORLD, &status);
                for (i = 0; i < numOfReceived; ++i) {
                    printMatch(&receivedMatches[i], outFile);
                }
                //printMatch(&receivedMatch, stdout);
                break;
            case FINISHED_TAG:
                MPI_Recv(NULL, 0, MPI_BYTE, MPI_ANY_SOURCE,
                    FINISHED_TAG, MPI_COMM_WORLD, &status);
                producingProcs--;
                break;
            default:
                // This should never happen
                break;
        }
        if (producingProcs == 0) {
            matchingFinished = 1;
        }
    }

    free(receivedMatches);
    
    // Inform all processes, that matching is finished
    for (actProc = 0; actProc < numOfProcs; ++actProc) {
        if (actProc != ROOT) {
            MPI_Isend(NULL, 0, MPI_BYTE, actProc, TERMINATE_TAG,
                MPI_COMM_WORLD, &request);
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
    // Array informing, how many nodes will be sent to each process
    // Used only in root
    int *numOfNodesForProc = NULL;
    int *patternDfsOrder = NULL;
    int *patternDfsParents = NULL;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &numOfProcs);

    rankG = rank;

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
        // Creates MPI_Match datatype for convenient sending matches
    }
    createMPIMatchDatatype(&mpiMatchType);

    if (rank == ROOT) {
        //printf("preparing\n");
        prepareGraphInRoot(inFile, &graph);
        preparePatternInRoot(inFile, &pattern);
        //printf("assigning\n");
        assignNodeToProc(&graph, numOfProcs, &numOfNodesForProc);
        rewind(inFile);
        //printf("distributing\n");
        distributeGraph(inFile, &graph, numOfNodesForProc, numOfProcs);
        free(numOfNodesForProc);
        //printf("reading pattern\n");
        readPattern(inFile, &pattern);
        //printf("broadcasting pattern\n");
        broadcastPattern(&pattern);
        //printf("pattern broadcasted\n");
        //printGraphDebug(&graph);
        fclose(inFile);
        receiveMatches(outFile, numOfProcs, mpiMatchType, &pattern);
        fclose(outFile);
    } else {
        prepareGraphInWorker(&graph);
        //printf("%d graph prepared\n", rank);
        receiveOutEdges(rank, numOfProcs, &graph);
        /*
        sleep(rank);
       //printf("%d out edges received\n", rank);
        printGraphDebug(&graph);
        */
        exchangeInEdges(rank, numOfProcs, &graph);
        /*
        sleep(rank);
       //printf("%d graph received\n", rank);
        printGraphDebug(&graph);
        sleep(4);
        */
        receivePattern(&pattern);
        //printf("%d pattern received\n", rank);
        //printf("\n");
        findPatternDfsOrdering(&pattern, &patternDfsOrder, &patternDfsParents);
        findMatches(rank, &graph, &pattern, patternDfsOrder, patternDfsParents,
            mpiMatchType);
        free(patternDfsOrder);
        free(patternDfsParents);
    }

    //printf("%d ends\n", rank);
    freeGraph(&graph);
    freeGraph(&pattern);

    MPI_Finalize();

    return 0;
}