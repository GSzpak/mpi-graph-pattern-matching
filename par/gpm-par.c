#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>
#include <assert.h>

#include "graph.h"
#include "utils.h"
#include "match.h"
#include "distribution.h"


// Maximum node encoding size
// Node is encoded as follows: num, outDegree, inDegree, outEdges, inEdges
#define MAX_NODE_ENCODING 2 * MAX_NUM_OF_NODES + 3
#define MATCH_BUFFER_SIZE 10000
#define MATCHES_TAG 54
#define FINISHED_TAG 55
#define NODE_REQ_TAG 56
#define REC_IN_EDGES_TAG 57
#define REC_OUT_EDGES_TAG 58
#define TERMINATE_TAG 59


// MPI datatype for Match struct
MPI_Datatype mpiMatchType;
// Buffers to handle received in/out edges of current node
int receivedInEdgesBuffer[MAX_NUM_OF_NODES];
int receivedOutEdgesBuffer[MAX_NUM_OF_NODES];
// Currently matched nodes
Node matchedNodes[MAX_MATCH_SIZE + 1];


// Functions declarations
void findPatternDfsOrdering(Graph *pattern, int **patternDfsOrder,
    int **patternDfsParents);
int nodeMatches(Node *graphNode, Node *patternNode, Match* match);
void exploreMatch(Graph* graph, Graph* pattern, Match *match,
    int *nodesMatchingOrder, int *patternParents,
    Match *finishedMatches, int *finishedMatchesInd);
void handleNodeRequest(MPI_Status *status, Graph *graph);
void handlePendingRequests(Graph *graph);
void askForNode(Graph *graph, int nodeNum, Node *nextNode);
void addFinishedMatch(Match *match, Match *finishedMatches, int *index);
void tryMatchNextGraphNode(Graph* graph, Graph* pattern, Match *match,
    int *nodesMatchingOrder, int *patternParents,
    Match *finishedMatches, int *finishedMatchesInd,
    Node *nextPatternNode, int nextGraphNodeNum);
void findMatches(Graph *graph, Graph *pattern, int *nodesMatchingOrder,
    int *patternParents);
void receiveMatches(FILE *outFile, int numOfProcs, Graph *pattern);


void findPatternDfsOrdering(Graph *pattern, int **patternDfsOrder,
    int **patternDfsParents)
{
    *patternDfsOrder = (int *) safeMalloc(sizeof(int) * pattern->numOfNodes);
    *patternDfsParents = (int *) safeMalloc(sizeof(int) * (pattern->numOfNodes + 1));
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

void handleNodeRequest(MPI_Status *status, Graph *graph)
{
    int requestedNodeNum;
    MPI_Request inEdgesRequest, outEdgesRequest;
    Node *node;

    assert(status->MPI_TAG == NODE_REQ_TAG);
    MPI_Recv(&requestedNodeNum, 1, MPI_INT, status->MPI_SOURCE,
        NODE_REQ_TAG, MPI_COMM_WORLD, status);
    node = getNode(graph, requestedNodeNum);
    MPI_Isend(node->inEdges, node->inDegree, MPI_INT, status->MPI_SOURCE,
        REC_IN_EDGES_TAG, MPI_COMM_WORLD, &inEdgesRequest);
    MPI_Isend(node->outEdges, node->outDegree, MPI_INT, status->MPI_SOURCE,
        REC_OUT_EDGES_TAG, MPI_COMM_WORLD, &outEdgesRequest);
    MPI_Request_free(&inEdgesRequest);
    MPI_Request_free(&outEdgesRequest);
}

void handlePendingRequests(Graph *graph)
{
    int isAvailable;
    MPI_Status status;

    MPI_Iprobe(MPI_ANY_SOURCE, NODE_REQ_TAG, MPI_COMM_WORLD,
        &isAvailable, &status);
    while (isAvailable) {
        handleNodeRequest(&status, graph);
        MPI_Iprobe(MPI_ANY_SOURCE, NODE_REQ_TAG, MPI_COMM_WORLD,
            &isAvailable, &status);
    }
}

void askForNode(Graph *graph, int nodeNum, Node *nextNode)
{
    int procForNode, responseReceived, requestAvailable, inReceived, outReceived;
    MPI_Request requests[3];
    MPI_Status nodeReceivingStatuses[3];
    MPI_Status nodeSendingStatus;

    procForNode = graph->procForNode[nodeNum];
    MPI_Isend(&nodeNum, 1, MPI_INT, procForNode, NODE_REQ_TAG,
        MPI_COMM_WORLD, &requests[0]);
    MPI_Irecv(receivedInEdgesBuffer, MAX_NODE_ENCODING, MPI_INT,
        procForNode, REC_IN_EDGES_TAG, MPI_COMM_WORLD, &requests[1]);
    MPI_Irecv(receivedOutEdgesBuffer, MAX_NODE_ENCODING, MPI_INT,
        procForNode, REC_OUT_EDGES_TAG, MPI_COMM_WORLD, &requests[2]);

    MPI_Testall(3, requests, &responseReceived, nodeReceivingStatuses);
    while (!responseReceived) {
        MPI_Iprobe(MPI_ANY_SOURCE, NODE_REQ_TAG, MPI_COMM_WORLD,
            &requestAvailable, &nodeSendingStatus);
        if (requestAvailable) {
            handleNodeRequest(&nodeSendingStatus, graph);
        }
        MPI_Testall(3, requests, &responseReceived, nodeReceivingStatuses);
    }

    MPI_Get_count(&nodeReceivingStatuses[1], MPI_INT, &inReceived);
    MPI_Get_count(&nodeReceivingStatuses[2], MPI_INT, &outReceived);
    reproduceNode(nextNode, nodeNum, receivedInEdgesBuffer, inReceived,
        receivedOutEdgesBuffer, outReceived);
}

void addFinishedMatch(Match *match, Match *finishedMatches, int *index)
{
    memcpy(finishedMatches + (*index)++, match, sizeof(Match));
    if (*index == MATCH_BUFFER_SIZE) {
        MPI_Ssend(finishedMatches, *index, mpiMatchType, ROOT,
            MATCHES_TAG, MPI_COMM_WORLD);
        *index = 0;
        memset(finishedMatches, 0, sizeof(Match) * MATCH_BUFFER_SIZE);
    }
}

void tryMatchNextGraphNode(Graph *graph, Graph *pattern, Match *match,
    int *nodesMatchingOrder, int *patternParents,
    Match *finishedMatches, int *finishedMatchesInd,
    Node *nextPatternNode, int nextGraphNodeNum)
{
    Node *nextGraphNode;

    nextGraphNode = &matchedNodes[nextPatternNode->num];

    if (isInGraph(graph, nextGraphNodeNum)) {
        // Process continues matching, if next node is in its part of graph
        copyNode(nextGraphNode, getNode(graph, nextGraphNodeNum));
    } else {
        // Otherwise, asks for next node
        askForNode(graph, nextGraphNodeNum, nextGraphNode);
    }

    if (nodeMatches(nextGraphNode, nextPatternNode, match)) {
        addNode(match, nextPatternNode->num, nextGraphNodeNum);
        exploreMatch(graph, pattern, match, nodesMatchingOrder,
            patternParents, finishedMatches, finishedMatchesInd);
        removeNode(match, nextPatternNode->num);
    }

    freeNode(nextGraphNode);
}

void exploreMatch(Graph* graph, Graph* pattern, Match *match,
    int *nodesMatchingOrder, int *patternParents,
    Match *finishedMatches, int *finishedMatchesInd)
{
    Node *parentInGraph;
    Node *nextPatternNode;
    int *edgesToCheck;
    int nextPatternNodeNum, nextGraphNodeNum, patternParentNum, visitedViaInEdge;
    int numOfEdges, i;

    handlePendingRequests(graph);

    if (match->matchedNodes == pattern->numOfNodes) {
        addFinishedMatch(match, finishedMatches, finishedMatchesInd);
        return;
    }

    nextPatternNodeNum = nodesMatchingOrder[match->matchedNodes];
    visitedViaInEdge = nextPatternNodeNum < 0 ? 1 : 0;
    nextPatternNodeNum = abs(nextPatternNodeNum);
    nextPatternNode = getNode(pattern, nextPatternNodeNum);
    patternParentNum = patternParents[nextPatternNodeNum];
    parentInGraph = &matchedNodes[patternParentNum];

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
        tryMatchNextGraphNode(graph, pattern, match, nodesMatchingOrder,
            patternParents, finishedMatches, finishedMatchesInd,
            nextPatternNode, nextGraphNodeNum);
    }
}

void findMatches(Graph *graph, Graph *pattern, int *nodesMatchingOrder,
    int *patternParents)
{
    Match m;
    MPI_Status status;
    Match *finishedMatches;
    int i, terminate, finishedMatchesInd;

    finishedMatchesInd = 0;
    finishedMatches = safeMalloc(sizeof(Match) * MATCH_BUFFER_SIZE);
    memset(finishedMatches, 0, sizeof(Match) * MATCH_BUFFER_SIZE);

    for (i = 0; i < graph->myPartNumOfNodes; ++i) {
        prepareMatch(&m);
        m.matchedNodes = 1;
        m.matches[1] = graph->nodes[i].num;
        copyNode(&matchedNodes[1], &graph->nodes[i]);
        exploreMatch(graph, pattern, &m, nodesMatchingOrder,
            patternParents, finishedMatches, &finishedMatchesInd);
        freeNode(&matchedNodes[1]);
    }

    if (finishedMatchesInd != 0) {
        // Flush remaining finished matches
        MPI_Ssend(finishedMatches, finishedMatchesInd, mpiMatchType, ROOT,
            MATCHES_TAG, MPI_COMM_WORLD);
    }
    free(finishedMatches);

    MPI_Ssend(NULL, 0, MPI_BYTE, ROOT, FINISHED_TAG, MPI_COMM_WORLD);

    terminate = 0;
    while (!terminate) {
        MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
        if (status.MPI_TAG == TERMINATE_TAG) {
            terminate = 1;
        } else {
            handleNodeRequest(&status, graph);
        }
    }
}

void receiveMatches(FILE *outFile, int numOfProcs, Graph *pattern)
{
    Match *receivedMatches;
    MPI_Status status;
    MPI_Request endRequests[numOfProcs - 1];
    int producingProcs, matchingFinished, actProc, numOfReceived, i;

    receivedMatches = safeMalloc(sizeof(Match) * MATCH_BUFFER_SIZE);
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
        MPI_Request *actRequest = actProc < ROOT ? endRequests + actProc :
            endRequests + actProc - 1;
        if (actProc != ROOT) {
            MPI_Isend(NULL, 0, MPI_BYTE, actProc, TERMINATE_TAG,
                MPI_COMM_WORLD, actRequest);
        }
    }
    MPI_Waitall(numOfProcs - 1, endRequests, MPI_STATUSES_IGNORE);
}

int main(int argc, char **argv)
{
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
    double startTime, distributionTime, endTime;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &numOfProcs);

    if (argc != 3) {
        error("Wrong number of arguments.\n");
    }

    if (rank == ROOT) {
        inFile = fopen(inFileName, "r");
        if (inFile == NULL) {
            error("Can't open input file %s\n", inFileName);
        }
        outFile = fopen(outFileName, "w");
        if (outFile == NULL) {
            error("Can't open output file.\n", outFileName);
        }
    }

    // Creates MPI_Match datatype for convenient sending matches
    createMPIMatchDatatype(&mpiMatchType);

    if (rank == ROOT) {
        startTime = MPI_Wtime();
        prepareGraphInRoot(inFile, &graph);
        preparePatternInRoot(inFile, &pattern);
        assignNodeToProc(&graph, numOfProcs, &numOfNodesForProc);
        rewind(inFile);
        distributeGraph(inFile, &graph, numOfNodesForProc, numOfProcs);
        free(numOfNodesForProc);
        readPattern(inFile, &pattern);
        broadcastPattern(&pattern);
        distributionTime = MPI_Wtime();
        printf("Distribution time[s]: %.2f\n", distributionTime - startTime);
        fclose(inFile);
        receiveMatches(outFile, numOfProcs, &pattern);
        endTime = MPI_Wtime();
        printf("Computations time[s]: %.2f\n", endTime - distributionTime);
        fclose(outFile);
    } else {
        prepareGraphInWorker(&graph);
        receiveOutEdges(rank, numOfProcs, &graph);
        exchangeInEdges(rank, numOfProcs, &graph);
        receivePattern(&pattern);
        findPatternDfsOrdering(&pattern, &patternDfsOrder, &patternDfsParents);
        findMatches(&graph, &pattern, patternDfsOrder, patternDfsParents);
        free(patternDfsOrder);
        free(patternDfsParents);
    }

    freeGraph(&graph);
    freeGraph(&pattern);

    MPI_Finalize();

    return 0;
}
