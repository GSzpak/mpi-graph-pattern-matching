#ifndef UTILS_H
#define UTILS_H


void error(const char* errorMsgFormat, ...);
int isLineEmpty(const char *line);
void printArrayDebug(int* arr, int len);
int intComparator(const void *elem1, const void *elem2);
// Returns 1 if x is an arr and 0 otherwise
int binsearch(int x, int *arr, int len);
void *safeMalloc(size_t size);


#endif //UTILS_H
