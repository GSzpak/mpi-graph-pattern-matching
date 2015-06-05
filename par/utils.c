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