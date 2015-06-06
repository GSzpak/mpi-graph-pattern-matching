//
// Created by Szymon Matejczyk on 21.05.15.
//
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>

#include "utils.h"

void error(const char* errorMsgFormat, ...)
{
    va_list argptr;
    va_start(argptr, errorMsgFormat);
    vfprintf(stderr, errorMsgFormat, argptr);
    fflush(stderr);
    va_end(argptr);
    exit(-1);
}

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

void printArray(int* arr, int len)
{
    int i;
    printf("[%d", arr[0]);
    for (i = 1; i < len; ++i) {
        printf("\t%d", arr[i]);
    }
    printf("]\n");
}

int intComparator(const void *elem1, const void *elem2)
{
    int *x = (int *) elem1;
    int *y = (int *) elem2;
    if (*x > *y) {
        return 1;
    } else if (*x == *y) {
        return 0;
    } else {
        return -1;
    }
}

int binsearch(int x, int *arr, int len)
{
    int left, right, mid;
    left = 0;
    right = len - 1;
    while (left <= right) {
        mid = (left + right) / 2;
        if (arr[mid] == x) {
            return 1;
        } else if (arr[mid] < x) {
            left = mid + 1;            
        } else {
            right = mid - 1;
        }
    }
    return 0;
}