#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>
#include <assert.h>
#include <unistd.h> // TODO: delete

#include "graph.h"
#include "utils.h"
#include "match.h"
#include "distribution.h"


// Maximum node encoding size
// Node is encoded as follows: num, outDegree, inDegree, outEdges, inEdges
#define MAX_NODE_ENCODING 2 * MAX_NUM_OF_NODES + 3
// Each process has 200MB for its nodes and edges
#define MATCH_BUFFER_SIZE 100000
#define MATCHES_TAG 54
#define FINISHED_TAG 55
#define NODE_REQ_TAG 56
#define NODE_RESP_TAG 57
#define TERMINATE_TAG 58


// MPI datatype for Match struct
MPI_Datatype mpiMatchType;
// buffer to handle sending / receiving nodes
int *nodesSendingReceivingBuffer;


// functions declarations
void findPatternDfsOrdering(Graph *pattern, int **patternDfsOrder,
    int **patternDfsParents);
int nodeMatches(Node *graphNode, Node *patternNode, Match* match);
void exploreMatch(Graph* graph, Graph* pattern, Match *match,
    int *nodesMatchingOrder, int *patternParents,
    Node receivedMatchedNodes[MAX_MATCH_SIZE], int *receivedMatchedNodesInd,
    Match *finishedMatches, int *finishedMatchesInd);
void handleNodeRequest(MPI_Status *status, Graph *graph);
void handlePendingRequests(Graph *graph);
void askForNode(Graph *graph, int nodeNum, Node *nextNode);
Node *getNextParent(Graph *graph, Match *match, int *patternParents,
    int nextPatternNodeNum, Node receivedMatchedNodes[MAX_MATCH_SIZE]);
void addFinishedMatch(Match *match, Match *finishedMatches, int *index);
void tryMatchNextGraphNode(Graph* graph, Graph* pattern, Match *match,
    int *nodesMatchingOrder, int *patternParents,
    Node receivedMatchedNodes[MAX_MATCH_SIZE], int *receivedMatchedNodesInd,
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
    int requestedNode, bufIndex;

    assert(status->MPI_TAG == NODE_REQ_TAG);
    MPI_Recv(&requestedNode, 1, MPI_INT, status->MPI_SOURCE,
        NODE_REQ_TAG, MPI_COMM_WORLD, status);
    bufIndex = 0;
   //printf("request in %d from %d for %d\n", rankG, status->MPI_SOURCE, requestedNode);
    copyNodeToBuffer(graph, requestedNode, nodesSendingReceivingBuffer, &bufIndex);
    MPI_Send(nodesSendingReceivingBuffer, bufIndex, MPI_INT, status->MPI_SOURCE,
        NODE_RESP_TAG, MPI_COMM_WORLD);
}

void handlePendingRequests(Graph *graph)
{
    int isAvailable;
    MPI_Status status;

   //printf("%d handling\n", rankG);
    MPI_Iprobe(MPI_ANY_SOURCE, NODE_REQ_TAG, MPI_COMM_WORLD,
        &isAvailable, &status);
    while (isAvailable) {
        handleNodeRequest(&status, graph);
        MPI_Iprobe(MPI_ANY_SOURCE, NODE_REQ_TAG, MPI_COMM_WORLD,
            &isAvailable, &status);
    }
   //printf("%d handling finished\n", rankG);
}

void askForNode(Graph *graph, int nodeNum, Node *nextNode)
{
    int procForNode, responseReceived, requestAvailable, bufIndex;
    MPI_Request requests[2];
    MPI_Status status;

    procForNode = graph->procForNode[nodeNum];
    MPI_Isend(&nodeNum, 1, MPI_INT, procForNode, NODE_REQ_TAG,
        MPI_COMM_WORLD, &requests[0]);
    MPI_Irecv(nodesSendingReceivingBuffer, MAX_NODE_ENCODING, MPI_INT,
        procForNode, NODE_RESP_TAG, MPI_COMM_WORLD, &requests[1]);

    MPI_Testall(2, requests, &responseReceived, MPI_STATUSES_IGNORE);
    while (!responseReceived) {
        MPI_Iprobe(MPI_ANY_SOURCE, NODE_REQ_TAG, MPI_COMM_WORLD,
            &requestAvailable, &status);
        if (requestAvailable) {
            handleNodeRequest(&status, graph);
        }
        MPI_Testall(2, requests, &responseReceived, MPI_STATUSES_IGNORE);
    }

    bufIndex = 0;
    readReceivedNode(nextNode, nodeNum, nodesSendingReceivingBuffer, &bufIndex);
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
        MPI_Ssend(finishedMatches, *index, mpiMatchType, ROOT,
            MATCHES_TAG, MPI_COMM_WORLD);
        *index = 0;
        memset(finishedMatches, 0, sizeof(Match) * MATCH_BUFFER_SIZE);
    }
}

void tryMatchNextGraphNode(Graph* graph, Graph* pattern, Match *match,
    int *nodesMatchingOrder, int *patternParents,
    Node receivedMatchedNodes[MAX_MATCH_SIZE], int *receivedMatchedNodesInd,
    Match *finishedMatches, int *finishedMatchesInd,
    Node *nextPatternNode, int nextGraphNodeNum)
{
    int isNextInGraph;
    Node *nextGraphNode;

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
        addNode(match, nextPatternNode->num, nextGraphNodeNum);
        exploreMatch(graph, pattern, match, nodesMatchingOrder,
            patternParents, receivedMatchedNodes, receivedMatchedNodesInd,
            finishedMatches, finishedMatchesInd);
        removeNode(match, nextPatternNode->num);
    }
    if (!isNextInGraph) {
        freeNode(&receivedMatchedNodes[--(*receivedMatchedNodesInd)]);
    }
}

void exploreMatch(Graph* graph, Graph* pattern, Match *match,
    int *nodesMatchingOrder, int *patternParents,
    Node receivedMatchedNodes[MAX_MATCH_SIZE], int *receivedMatchedNodesInd,
    Match *finishedMatches, int *finishedMatchesInd)
{
    Node *parentInGraph;
    Node *nextPatternNode;
    int *edgesToCheck;
    int nextPatternNodeNum, nextGraphNodeNum, numOfEdges, visitedViaInEdge, i;

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
        tryMatchNextGraphNode(graph, pattern, match, nodesMatchingOrder,
            patternParents, receivedMatchedNodes, receivedMatchedNodesInd,
            finishedMatches, finishedMatchesInd, nextPatternNode,
            nextGraphNodeNum);
    }
}

void findMatches(Graph *graph, Graph *pattern, int *nodesMatchingOrder,
    int *patternParents)
{
    Match m;
    MPI_Status status;
    Node receivedMatchedNodes[MAX_MATCH_SIZE];
    Match *finishedMatches;
    int i, terminate, receivedMatchedNodesInd, finishedMatchesInd;

    receivedMatchedNodesInd = 0;
    finishedMatchesInd = 0;
    memset(receivedMatchedNodes, 0, sizeof(receivedMatchedNodes));
    finishedMatches = safeMalloc(sizeof(Match) * MATCH_BUFFER_SIZE);
    memset(finishedMatches, 0, sizeof(Match) * MATCH_BUFFER_SIZE);

    for (i = 0; i < graph->myPartNumOfNodes; ++i) {
        prepareMatch(&m);
        m.matchedNodes = 1;
        m.matches[1] = graph->nodes[i].num;
        exploreMatch(graph, pattern, &m, nodesMatchingOrder,
            patternParents, receivedMatchedNodes, &receivedMatchedNodesInd,
            finishedMatches, &finishedMatchesInd);
        assert(receivedMatchedNodesInd == 0);
        // FIXME:
        //memset(receivedMatchedNodes, 0, sizeof(receivedMatchedNodes));
        //printf("%d %d\n", rankG, i);
    }

    if (finishedMatchesInd != 0) {
        // flush remaining finished matches
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
                    //printMatch(&receivedMatches[i], stdout);
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
    double startTime, endTime;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &numOfProcs);

    if (argc != 3) {
        error("Wrong number of arguments.\n");
    }

    if (rank == ROOT) {
        inFile = fopen(inFileName, "r");
        if (inFile == NULL) {
            //MPI_Finalize(); FIXME: is necessary?
            error("Can't open input file %s\n", inFileName);
        }
        outFile = fopen(outFileName, "w");
        if (outFile == NULL) {
            //MPI_Finalize(); TODO: is necessary?
            error("Can't open output file.\n", outFileName);
        }
    }

    // Creates MPI_Match datatype for convenient sending matches
    createMPIMatchDatatype(&mpiMatchType);

    if (rank == ROOT) {
        startTime = MPI_Wtime();
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
        receiveMatches(outFile, numOfProcs, &pattern);
        endTime = MPI_Wtime();
        fprintf(stdout, "time[s]: %.2f\n", endTime - startTime);
        fclose(outFile);
    } else {
        nodesSendingReceivingBuffer =
            (int *) safeMalloc(sizeof(int) * MAX_NODE_ENCODING);
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
        printf("%d graph received\n", rank);
        printGraphDebug(&graph);
        sleep(4);
        //*/
        receivePattern(&pattern);
        //printf("%d pattern received\n", rank);
        //printf("\n");
        findPatternDfsOrdering(&pattern, &patternDfsOrder, &patternDfsParents);
        findMatches(&graph, &pattern, patternDfsOrder, patternDfsParents);
        free(patternDfsOrder);
        free(patternDfsParents);
        free(nodesSendingReceivingBuffer);
    }

    //printf("%d ends\n", rank);
    freeGraph(&graph);
    freeGraph(&pattern);

    MPI_Finalize();

    return 0;
}
