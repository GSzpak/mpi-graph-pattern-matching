//
// Created by Szymon Matejczyk on 21.05.15.
//

#ifndef UTILS_H
#define UTILS_H

#ifdef DEBUG
#define DEBUG_TEST 1
#else
#define DEBUG_TEST 0
#endif

#define max(a,b) \
   ({ __typeof__ (a) _a = (a); \
       __typeof__ (b) _b = (b); \
     _a > _b ? _a : _b; })

#define debug_print(...) \
            do { if (DEBUG_TEST) fprintf(stderr, ##__VA_ARGS__); } while (0)

void error(const char* errorMsgFormat, ...);
int isLineEmpty(const char *line);
void printArray(int* arr, int len);
int intComparator(const void *elem1, const void *elem2);
// Returns 1 if x is an arr and 0 otherwise
int binsearch(int x, int *arr, int len);


#endif //UTILS_H
