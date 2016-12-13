#ifndef UTILS_H
#define UTILS_H
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define DEBUG 1
#define DEBUG_SMALL 1

#define MAX_LINE 10000
#define MAX_CHAR 255
#define MAX_PATH_LENGTH 1024
#define DEFAULT_CHAR_BUFFER_SIZE 524288
#define DEFAULT_LONG_BUFFER_SIZE 65536

#define MAX_BUFFERS_PERFILE 20
#define DEFAULT_TRIPLE_BUFFER_SIZE 33554432
//(1024*1024*1024/32)*24 = 805306368 - that is 800 MB for each buffer at most
//about 33 MB of input fits into this buffer, and having 20 of these allows each chunk size go up to 660 MB - never the case really
//if only 1024 files are allowed per directory, as on some linux machines, we can have 1024/20 = about 50 input files if you write all temps into the same directory
//this will amount for the total input size of 50*660 MB = 33 GB and all this in memory < 2 GB
//for merge - at most 1024 input runs

#define SUCCESS 0
#define FAILURE 1
#define EMPTY 2

#define ABS(n) ((n)<0)? -(n) : (n)


void OpenBinaryFileRead (FILE ** fp, char * file_name);
void OpenBinaryFileWrite (FILE ** fp, char * file_name);
void OpenBinaryFileAppend(FILE ** fp, char * file_name);


void Fwrite (const void *buffer, size_t elem_size, size_t num_elements, FILE *fp );
void * Calloc (int num_bytes);

#endif


