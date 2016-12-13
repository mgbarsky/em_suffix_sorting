#ifndef MERGERANKS_H
#define MERGERANKS_H

#include "utils.h"
#include "algorithm.h"

extern long base_current_rank; // rank we update as we go over counts
extern long current_rank; //original rank preserved for output back

typedef struct HeapElement {
	long currentRank;
    long nextRank;
    long count;
    int file_number;  //gives a position in an array of file numbers - which store file id plus interval id
} HeapElement;

typedef struct output_element {
	GlobalRecord output;
	int file_number; //gives a position in an array of file numbers - which store file id plus interval id
} OutputElement;
#endif